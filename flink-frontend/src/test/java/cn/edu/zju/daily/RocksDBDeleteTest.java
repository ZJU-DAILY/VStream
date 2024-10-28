package cn.edu.zju.daily;

import cn.edu.zju.daily.data.DataSerializer;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormat;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.contrib.streaming.vstate.PredefinedOptions;
import org.apache.flink.contrib.streaming.vstate.RocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.vstate.RocksDBResourceContainer;
import org.apache.flink.core.fs.local.LocalDataInputStream;
import org.junit.jupiter.api.Test;
import org.rocksdb.*;

@Slf4j
public class RocksDBDeleteTest {

    private static final long MAX_TTL = 1000;
    private static final String DATA_PATH =
            "/mnt/sda1/work/vector-search/dataset/sift/sift_base.fvecs";
    private static final String QUERY_PATH =
            "/mnt/sda1/work/vector-search/dataset/sift/sift_query.fvecs";
    private static final int DIM = 128;
    private static final double DELETE_RATIO = 0.1;

    static int m = 16;
    static int dim = 128;
    static int efSearch = 16;
    static int efConstruction = 128;
    static int k = 10;
    static long maxElements = 1000L;
    static long ssTableSize = 4096L * (1 << 21); // 4 G
    static long blockSize = 4 << 10; // 4 KB
    static long blockCacheSize = 26214400L; // 25 MB

    static float terminationWeight = 0.001f;
    static float terminationFactor = 0.8f;
    static float terminationThreshold = 0f;
    static float terminationLowerBound = 0.0f;
    static int sortInterval = 10000;
    static int flushThreshold = 12;
    static int maxWriteBufferNumber = 15;
    static int blockRestartInterval = 4;

    RocksDB db;
    DBOptions dbOptions;
    VectorColumnFamilyOptions vectorCFOptions;
    WriteOptions writeOptions;
    VectorColumnFamilyHandle vectorCFHandle;
    FlushOptions flushOptions = new FlushOptions();
    RocksDBResourceContainer container = null;

    private void openDB(String dir, boolean create) throws RocksDBException {
        container =
                new RocksDBResourceContainer(
                        PredefinedOptions.DEFAULT,
                        new RocksDBOptionsFactory() {
                            @Override
                            public DBOptions createDBOptions(
                                    DBOptions currentOptions,
                                    Collection<AutoCloseable> handlesToClose) {
                                currentOptions.setUseDirectIoForFlushAndCompaction(true);
                                currentOptions.setAvoidUnnecessaryBlockingIO(true);
                                currentOptions.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                                currentOptions.setFlushVerifyMemtableCount(false);
                                return currentOptions;
                            }

                            @Override
                            public ColumnFamilyOptions createColumnOptions(
                                    ColumnFamilyOptions currentOptions,
                                    Collection<AutoCloseable> handlesToClose) {
                                return currentOptions;
                            }

                            @Override
                            public VectorColumnFamilyOptions createVectorColumnOptions(
                                    VectorColumnFamilyOptions currentOptions,
                                    Collection<AutoCloseable> handlesToClose) {
                                currentOptions.setMaxElements(maxElements + 1);
                                currentOptions.setSpace(SpaceType.L2);
                                currentOptions.setM(m);
                                currentOptions.setEfConstruction(efConstruction);
                                currentOptions.setDim(dim);
                                currentOptions.setWriteBufferSize(ssTableSize);
                                currentOptions.setMemTableConfig(
                                        new HnswMemTableConfig(dim)
                                                .setSpace(SpaceType.L2)
                                                .setM(m)
                                                .setMaxElements(maxElements + 1));
                                currentOptions.setTableFormatConfig(
                                        new HnswTableOptions()
                                                .setDim(dim)
                                                .setSpace(SpaceType.L2)
                                                .setM(m)
                                                .setBlockRestartInterval(blockRestartInterval)
                                                .setBlockSize(blockSize)
                                                .setBlockCache(new LRUCache(blockCacheSize)));
                                currentOptions.setFlushThreshold(flushThreshold);
                                currentOptions.setMaxWriteBufferNumber(maxWriteBufferNumber);
                                currentOptions.setTerminationWeight(terminationWeight);
                                currentOptions.setTerminationThreshold(terminationThreshold);
                                currentOptions.setTerminationLowerBound(terminationLowerBound);
                                return currentOptions;
                            }

                            @Override
                            public VectorSearchOptions createVectorSearchOptions(
                                    VectorSearchOptions currentOptions,
                                    Collection<AutoCloseable> handlesToClose) {
                                currentOptions.setK(k);
                                currentOptions.setAsyncIO(true);
                                currentOptions.setTerminationFactor(terminationFactor);
                                currentOptions.setSearchSST(true);
                                currentOptions.setEvict(false);
                                return currentOptions;
                            }
                        });

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        RocksDB.DEFAULT_COLUMN_FAMILY, container.getColumnOptions()));

        dbOptions = container.getDbOptions();
        vectorCFOptions = container.getVectorColumnOptions();
        writeOptions = container.getWriteOptions();
        VectorCFDescriptor vectorCFDescriptor =
                new VectorCFDescriptor("test".getBytes(), vectorCFOptions);

        System.out.println("RocksDB dir: " + dir);
        // String dir = "./tmp/rocksdb- ???";
        new File(dir).mkdirs();
        db = RocksDB.open(dbOptions, dir, columnFamilyDescriptors, new ArrayList<>());
        vectorCFHandle = db.createVectorColumnFamily(vectorCFDescriptor);
    }

    private void closeDB() {
        db.close();
        dbOptions.close();
        vectorCFOptions.close();
        writeOptions.close();
        vectorCFHandle.close();
        flushOptions.close();
    }

    private int flushCount = 0;

    private void flush() throws RocksDBException {
        db.flush(flushOptions, vectorCFHandle);
        System.out.println("Flushed #" + flushCount + ".");
        flushCount++;
    }

    private void copyDirectory(String sourceDirectoryLocation, String destinationDirectoryLocation)
            throws IOException {
        Files.walk(Paths.get(sourceDirectoryLocation))
                .forEach(
                        source -> {
                            Path destination =
                                    Paths.get(
                                            destinationDirectoryLocation,
                                            source.toString()
                                                    .substring(sourceDirectoryLocation.length()));
                            try {
                                Files.copy(source, destination);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
    }

    static byte[] id = new byte[Long.BYTES];
    static byte[] vec = new byte[dim * Float.BYTES + Long.BYTES];
    static byte[] queryVec = new byte[dim * Float.BYTES];

    private void insert(FloatVector vector) {
        // System.out.println("Inserting #" + vector.getId());
        DataSerializer.serializeFloatVectorWithTimestamp(vector, id, vec);
        try {
            db.put(vectorCFHandle, writeOptions, id, vec);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(VectorDeletion marker) {
        // System.out.println("Deleting #" + marker.getId());
        DataSerializer.serializeLong(marker.getId(), id);
        try {
            db.delete(vectorCFHandle, writeOptions, id);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private SearchResult search(FloatVector query) {
        DataSerializer.serializeFloatVector(query, queryVec);
        byte[] resultBytes;
        VectorSearchOptions vectorSearchOptions = container.getVectorSearchOptions();

        if (query.getId() % sortInterval == sortInterval - 1) {

            vectorSearchOptions.setTriggerSort(true);
            vectorSearchOptions.setTs(Math.max(0, query.getEventTime() - query.getTTL()));
            vectorSearchOptions.setSearchSST(true);
            try {
                long start = System.currentTimeMillis();
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
                long duration = System.currentTimeMillis() - start;
                System.out.println("Query " + query.getId() + " searched in " + duration + " ms.");
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            vectorSearchOptions.setTriggerSort(false);
        } else {
            vectorSearchOptions.setTs(Math.max(0, query.getEventTime() - query.getTTL()));
            vectorSearchOptions.setSearchSST(true);
            try {
                long start = System.currentTimeMillis();
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
                long duration = System.currentTimeMillis() - start;
                System.out.println("Query " + query.getId() + " searched in " + duration + " ms.");
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
        vectorSearchOptions.close();

        return DataSerializer.deserializeSearchResult(
                resultBytes, 0, query.getId(), 1, query.getEventTime());
    }

    @Test
    void test() throws IOException, RocksDBException {

        String dir =
                "./tmp/rocksdb-standalone-"
                        + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");

        openDB(dir, true);

        Configuration conf = new Configuration();
        conf.set(
                StreamFormat.FETCH_IO_SIZE,
                new MemorySize(10 * (Integer.BYTES + DIM * Float.BYTES)));

        FloatVectorBinaryInputFormat dataFormat =
                new FloatVectorBinaryInputFormat(
                        "data",
                        MAX_TTL,
                        FloatVectorBinaryInputFormat.FileType.F_VEC,
                        0,
                        1000000,
                        DIM,
                        1,
                        DELETE_RATIO,
                        null);
        FloatVectorBinaryInputFormat.Reader dataReader =
                dataFormat.createReader(conf, new LocalDataInputStream(new File(DATA_PATH)));

        FloatVectorBinaryInputFormat queryFormat =
                new FloatVectorBinaryInputFormat(
                        "query",
                        MAX_TTL,
                        FloatVectorBinaryInputFormat.FileType.F_VEC,
                        0,
                        10000,
                        DIM,
                        1,
                        0,
                        null);
        FloatVectorBinaryInputFormat.Reader queryReader =
                queryFormat.createReader(conf, new LocalDataInputStream(new File(QUERY_PATH)));

        long ts = 0;
        while (true) {
            boolean eof = false;

            // query
            VectorData query = queryReader.read();
            if (query == null) {
                break;
            }
            query.setEventTime(ts);
            SearchResult result = search(query.asVector());
            // System.out.println(result);

            // insert + delete
            for (int i = 0; i < 100; i++) {
                VectorData vector = dataReader.read();
                if (vector == null) {
                    eof = true;
                    break;
                }
                if (vector.isDeletion()) {
                    delete(vector.asDeletion());
                } else {
                    vector.setEventTime(ts++);
                    insert(vector.asVector());
                }
            }

            if (eof) {
                break;
            }
        }

        closeDB();
    }
}
