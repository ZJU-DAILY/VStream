package cn.edu.zju.daily;

import cn.edu.zju.daily.util.Parameters;
import org.apache.flink.contrib.streaming.vstate.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.vstate.RocksDBOptionsFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collection;

public class VectorEnvironmentCreator implements Serializable {

    Parameters params;
    String dbStoragePath;

    public VectorEnvironmentCreator(Parameters params) {
        this.params = params;
        this.dbStoragePath = "/home/auroflow/code/vector-search/rocksdb-stream/tmp/rocksdb-backend-"
            + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");
    }

    public RocksDBOptionsFactory getRocksDBOptionsFactory() {
        return new RocksDBOptionsFactory() {
            @Override
            public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                currentOptions.setDbLogDir(dbStoragePath);  // log in rocksdb data dir
                currentOptions.setUseDirectIoForFlushAndCompaction(true);
                currentOptions.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                currentOptions.setMaxBackgroundJobs(params.getRocksDBMaxBackgroundJobs());
                return currentOptions;
            }

            @Override
            public ColumnFamilyOptions createColumnOptions(
                    ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                return currentOptions;
            }

            @Override
            public VectorColumnFamilyOptions createVectorColumnOptions(
                    VectorColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
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
                currentOptions.setTableFormatConfig(new HnswTableOptions()
                        .setDim(params.getVectorDim())
                        .setSpace(SpaceType.valueOf(params.getMetricType()))
                        .setM(params.getHnswM())
                        .setBlockSize(params.getRocksDBBlockSize())
                        .setBlockCache(new LRUCache(params.getRocksDBBlockCacheSize()))
                        .setBlockRestartInterval(params.getRocksDBBlockRestartInterval())
                );
                currentOptions.setTerminationWeight(params.getRocksDBTerminationWeight());
                currentOptions.setTerminationThreshold(params.getRocksDBTerminationThreshold());
                currentOptions.setTerminationLowerBound(params.getRocksDBTerminationLowerBound());
                currentOptions.setFlushThreshold(params.getRocksDBFlushThreshold());
                currentOptions.setMaxWriteBufferNumber(params.getRocksDBMaxWriteBufferNumber());
                return currentOptions;
            }

            @Override
            public VectorSearchOptions createVectorSearchOptions(
                    VectorSearchOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                currentOptions.setK(params.getK());
                currentOptions.setAsyncIO(true);
                currentOptions.setSearchSST(true);   // skip sstables
                currentOptions.setEvict(true);
                currentOptions.setTerminationFactor(params.getRocksDBTerminationFactor());
                return currentOptions;
            }
        };
    }

    private void setBackend(StreamExecutionEnvironment env) {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setRocksDBOptions(getRocksDBOptionsFactory());

        backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);

        backend.setDbStoragePath(dbStoragePath);
        env.setStateBackend(backend);
    }

    public void prepareVectorEnvironment(StreamExecutionEnvironment env) {
        setBackend(env);
    }
}
