package cn.edu.zju.daily;

import cn.edu.zju.daily.data.DataSerializer;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.contrib.streaming.vstate.*;
import org.junit.jupiter.api.Test;
import org.rocksdb.*;

public class RocksDBLocalTest {

    static boolean doInsert = true;
    static long timestamp = System.currentTimeMillis();
    static String backupDir = "./tmp/rocksdb-0";
    static long maxTTL = 250000;

    static int m = 16;
    static int dim = 128;
    static int efSearch = 16;
    static int efConstruction = 128;
    static int k = 10;
    static long maxElements = 25000L;
    static long ssTableSize = 4096L * (1 << 21); // 4 G
    static long blockSize = 4 << 10; // 4 KB
    static long blockCacheSize = 26214400L; // 25 MB

    static float terminationWeight = 0.001f;
    static float terminationFactor = 0.8f;
    static float terminationThreshold = 0f;
    static float terminationLowerBound = 0.0f;
    static int sortInterval = 100;
    static int flushThreshold = 2;
    static int maxWriteBufferNumber = 7;
    static int blockRestartInterval = 4;

    static RocksDB db;
    static DBOptions dbOptions;
    static VectorColumnFamilyOptions vectorCFOptions;
    static ColumnFamilyOptions vectorVersionCFOptions;
    static WriteOptions writeOptions;
    static VectorColumnFamilyHandle vectorCFHandle;
    static FlushOptions flushOptions = new FlushOptions();
    static RocksDBResourceContainer container = null;

    static void openDB(String dir, boolean create) throws RocksDBException {
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
                            public ColumnFamilyOptions createVectorVersionColumnOptions(
                                    ColumnFamilyOptions currentOptions,
                                    Collection<AutoCloseable> handlesToClose) {
                                currentOptions.setWriteBufferSize(12345678);
                                currentOptions.setMaxWriteBufferNumber(3);
                                return RocksDBOptionsFactory.super.createVectorVersionColumnOptions(
                                        currentOptions, handlesToClose);
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
        vectorVersionCFOptions = container.getVectorVersionColumnOptions();

        writeOptions = container.getWriteOptions();
        VectorCFDescriptor vectorCFDescriptor =
                new VectorCFDescriptor("test".getBytes(), vectorCFOptions, vectorVersionCFOptions);

        System.out.println("RocksDB dir: " + dir);
        // String dir = "./tmp/rocksdb- ???";
        new File(dir).mkdirs();
        db = RocksDB.open(dbOptions, dir, columnFamilyDescriptors, new ArrayList<>());
        vectorCFHandle =
                db.createVectorColumnFamily(
                        vectorCFDescriptor, vectorCFDescriptor.getVersionOptions());
    }

    static void closeDB() {
        db.close();
        dbOptions.close();
        vectorCFOptions.close();
        vectorVersionCFOptions.close();
        writeOptions.close();
        vectorCFHandle.close();
        flushOptions.close();
    }

    static int flushCount = 0;

    static void flush() throws RocksDBException {
        db.flush(flushOptions, vectorCFHandle);
        System.out.println("Flushed #" + flushCount + ".");
        flushCount++;
    }

    static void copyDirectory(String sourceDirectoryLocation, String destinationDirectoryLocation)
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

    static void insert(FloatVector vector) {
        DataSerializer.serializeFloatVectorWithTimestamp(vector, id, vec);
        try {
            db.put(vectorCFHandle, writeOptions, id, vec);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    static SearchResult search(FloatVector query) {
        DataSerializer.serializeFloatVector(query, queryVec);
        byte[] resultBytes;
        VectorSearchOptions vectorSearchOptions = container.getVectorSearchOptions();

        long start = System.currentTimeMillis();
        if (query.getId() % sortInterval == sortInterval - 1) {

            vectorSearchOptions.setTriggerSort(true);
            vectorSearchOptions.setTs(Math.max(0, query.getEventTime() - query.getTTL()));
            vectorSearchOptions.setSearchSST(false);
            try {
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            vectorSearchOptions.setTriggerSort(false);
        } else {
            vectorSearchOptions.setTs(Math.max(0, query.getEventTime() - query.getTTL()));
            vectorSearchOptions.setSearchSST(false);
            try {
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
        vectorSearchOptions.close();

        return DataSerializer.deserializeSearchResult(
                resultBytes, 0, query.getId(), 1, query.getEventTime());
    }

    private static void createCheckpoint(String dir) {
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(dir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void test() throws Exception {

        String dir =
                "./tmp/rocksdb-standalone-"
                        + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");

        openDB(dir, true);

        FloatVectorIterator vectors =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs", 1, 10000);
        FloatVectorIterator queries =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs");
        PrintWriter writer = new PrintWriter("./results.txt", "UTF-8");
        int index = 0;
        for (FloatVector vector : vectors) {
            vector.setEventTime(index);
            insert(vector);
            index++;
            if (index % 100 == 0) {
                FloatVector query = queries.next();
                query.setEventTime(index);
                query.setTTL(maxTTL);
                long start = System.currentTimeMillis();
                SearchResult result = search(query);
                System.out.println(
                        "Query "
                                + query.getId()
                                + " done in "
                                + (System.currentTimeMillis() - start)
                                + " ms: "
                                + result);
                writer.println(result);
            }
            if (index >= 1_000_000) {
                break;
            }
        }

        flush();
        // createCheckpoint(dir + "/checkpoint");
        closeDB();
        writer.close();
    }
}
