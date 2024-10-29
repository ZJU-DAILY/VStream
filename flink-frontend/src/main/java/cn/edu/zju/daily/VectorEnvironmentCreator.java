package cn.edu.zju.daily;

import cn.edu.zju.daily.util.Parameters;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collection;
import org.apache.flink.contrib.streaming.vstate.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.vstate.RocksDBOptionsFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.*;

public class VectorEnvironmentCreator implements Serializable {

    Parameters params;
    String dbStoragePath;
    private static final long ESTIMATED_VERSION_RECORD_BYTES = 41L;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 67108864L;

    public VectorEnvironmentCreator(Parameters params) {
        this.params = params;
        this.dbStoragePath =
                params.getRocksDBStoragePrefix()
                        + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");
    }

    public RocksDBOptionsFactory getRocksDBOptionsFactory() {
        return new RocksDBOptionsFactory() {
            @Override
            public DBOptions createDBOptions(
                    DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                currentOptions.setDbLogDir(dbStoragePath); // log in rocksdb data dir
                currentOptions.setUseDirectIoForFlushAndCompaction(true);
                currentOptions.setInfoLogLevel(params.getRocksDBInfoLogLevel());
                currentOptions.setMaxBackgroundJobs(params.getRocksDBMaxBackgroundJobs());
                currentOptions.setAvoidFlushDuringRecovery(true);
                currentOptions.setAvoidFlushDuringShutdown(true);
                currentOptions.setFlushVerifyMemtableCount(false);
                return currentOptions;
            }

            @Override
            public ColumnFamilyOptions createColumnOptions(
                    ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                return currentOptions;
            }

            @Override
            public VectorColumnFamilyOptions createVectorColumnOptions(
                    VectorColumnFamilyOptions currentOptions,
                    Collection<AutoCloseable> handlesToClose) {
                currentOptions.setMaxElements(params.getRocksDBMaxElementsPerHnswTable() + 1);
                currentOptions.setSpace(SpaceType.valueOf(params.getMetricType()));
                currentOptions.setM(params.getHnswM());
                currentOptions.setEfConstruction(params.getHnswEfConstruction());
                currentOptions.setDim(params.getVectorDim());
                currentOptions.setWriteBufferSize(params.getRocksDBSSTableSize());
                currentOptions.setMemTableConfig(
                        new HnswMemTableConfig(params.getVectorDim())
                                .setSpace(SpaceType.valueOf(params.getMetricType()))
                                .setM(params.getHnswM())
                                .setMaxElements(params.getRocksDBMaxElementsPerHnswTable() + 1));
                currentOptions.setTableFormatConfig(
                        new HnswTableOptions()
                                .setDim(params.getVectorDim())
                                .setSpace(SpaceType.valueOf(params.getMetricType()))
                                .setM(params.getHnswM())
                                .setBlockSize(params.getRocksDBBlockSize())
                                .setBlockCache(new LRUCache(params.getRocksDBBlockCacheSize()))
                                .setBlockRestartInterval(params.getRocksDBBlockRestartInterval()));
                currentOptions.setTerminationWeight(params.getRocksDBTerminationWeight());
                currentOptions.setTerminationThreshold(params.getRocksDBTerminationThreshold());
                currentOptions.setTerminationLowerBound(params.getRocksDBTerminationLowerBound());
                int flushThreshold = params.getRocksDBFlushThreshold();
                int maxWriteBufferNumber = params.getRocksDBMaxWriteBufferNumber();
                currentOptions.setFlushThreshold(params.getRocksDBFlushThreshold());
                currentOptions.setMaxWriteBufferNumber(
                        Math.min(flushThreshold + 3, maxWriteBufferNumber));
                currentOptions.setLevel0SlowdownWritesTrigger(Integer.MAX_VALUE);
                currentOptions.setLevel0StopWritesTrigger(Integer.MAX_VALUE);
                currentOptions.setLevel0FileNumCompactionTrigger(Integer.MAX_VALUE);
                return currentOptions;
            }

            @Override
            public ColumnFamilyOptions createVectorVersionColumnOptions(
                    ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                int oneFifthInsertsPerTable =
                        (int)
                                Math.ceil(
                                        (double) params.getRocksDBMaxElementsPerHnswTable()
                                                / (1 - params.getDeleteRatio())
                                                / 5);
                long retainSize =
                        ESTIMATED_VERSION_RECORD_BYTES
                                * oneFifthInsertsPerTable
                                * params.getRocksDBFlushThreshold();
                currentOptions.setWriteBufferSize(Math.max(retainSize, DEFAULT_WRITE_BUFFER_SIZE));
                currentOptions.setFlushThreshold(6);
                currentOptions.setMaxWriteBufferNumber(9);
                return currentOptions;
            }

            @Override
            public VectorSearchOptions createVectorSearchOptions(
                    VectorSearchOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                currentOptions.setK(params.getK());
                currentOptions.setAsyncIO(true);
                currentOptions.setSearchSST(!params.isRocksDBSkipSST());
                currentOptions.setEvict(false);
                currentOptions.setTerminationFactor(params.getRocksDBTerminationFactor());
                return currentOptions;
            }
        };
    }

    private void setBackend(StreamExecutionEnvironment env) {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(false);
        backend.setRocksDBOptions(getRocksDBOptionsFactory());
        backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
        backend.setDbStoragePath(dbStoragePath);
        env.setStateBackend(backend);
    }

    public void prepareVectorEnvironment(StreamExecutionEnvironment env) {
        setBackend(env);
    }
}
