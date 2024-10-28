package cn.edu.zju.daily;

import cn.edu.zju.daily.data.DataSerializer;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.contrib.streaming.vstate.*;
import org.rocksdb.*;

public class RocksDBParallelTest {

    static boolean doInsert = true;
    static long timestamp = System.currentTimeMillis();
    static String backupDir = "./tmp/rocksdb-0";

    static int m = 16;
    static int dim = 128;
    static int efConstruction = 128;
    static int k = 10;
    static long maxElements = 1000L;
    static long ssTableSize = 4096L * (1 << 21); // 4 G
    static long blockSize = 4 << 10; // 4 KB
    static long blockCacheSize = 1073741824L; // 1 GB

    static float terminationWeight = 0.001f;
    static float terminationFactor = 0.8f;
    static float terminationThreshold = 0f;
    static float terminationLowerBound = 0.5f;
    static int sortInterval = 100;
    static int flushThreshold = 5;
    static int maxWriteBufferNumber = 10;
    static int blockRestartInterval = 4;

    static RocksDB db;
    static DBOptions dbOptions;
    static VectorColumnFamilyOptions vectorCFOptions;
    static ColumnFamilyOptions vectorVersionCFOptions;
    static WriteOptions writeOptions;
    static VectorSearchOptions vectorSearchOptions;
    static VectorColumnFamilyHandle vectorCFHandle;
    static FlushOptions flushOptions = new FlushOptions();

    static void openDB(String dir, boolean create) throws RocksDBException {
        RocksDBResourceContainer container =
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
                                                .setBlockCache(
                                                        new LRUCache(
                                                                blockCacheSize)) // 1 GB block cache
                                        );
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
        vectorSearchOptions = container.getVectorSearchOptions();
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
        writeOptions.close();
        vectorVersionCFOptions.close();
        vectorSearchOptions.close();
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

    private static class InsertRunner implements Runnable {

        @Override
        public void run() {

            byte[] id = new byte[Long.BYTES];
            byte[] vec = new byte[dim * Float.BYTES + Long.BYTES];
            byte[] queryVec = new byte[dim * Float.BYTES + 2 * Long.BYTES];
            String dir =
                    "./tmp/rocksdb-standalone-"
                            + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");

            if (doInsert) {
                long start = System.currentTimeMillis();

                try {
                    openDB(dir, true);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
                FloatVectorIterator vectors = null;
                try {
                    vectors =
                            FloatVectorIterator.fromFile(
                                    "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs",
                                    1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                //            Collections.shuffle(list);
                int index = 0;
                for (FloatVector vector : vectors) {
                    vector.setEventTime(index);
                    DataSerializer.serializeFloatVectorWithTimestamp(vector, id, vec);
                    try {
                        db.put(vectorCFHandle, writeOptions, id, vec);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                    index++;
                    if (index % 1000 == 0) {
                        System.out.println("Inserted " + index + " vectors.");
                    }
                }

                System.out.println(
                        "Insert done in " + (System.currentTimeMillis() - start) + " ms.");

                //            flush();
            } else {
                // copy from backup
                try {
                    copyDirectory(backupDir, dir);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                // check correctness
                try {
                    openDB(dir, false);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("Using backup.");
            }
        }
    }

    private static class SearchRunner implements Runnable {

        @Override
        public void run() {

            byte[] id = new byte[Long.BYTES];
            byte[] vec = new byte[dim * Float.BYTES + Long.BYTES];
            byte[] queryVec = new byte[dim * Float.BYTES + 2 * Long.BYTES];
            String dir =
                    "./tmp/rocksdb-standalone-"
                            + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");

            FloatVectorIterator queries = null;
            try {
                queries =
                        FloatVectorIterator.fromFile(
                                "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs", 10);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            //        GroundTruthResultIterator gts = GroundTruthResultIterator.fromFile(
            //                "/home/auroflow/code/vector-search/data/sift/sift_groundtruth.ivecs",
            // k);

            //        List<Float> accuracies = new ArrayList<>();
            PrintWriter writer = null;
            try {
                writer = new PrintWriter("./results.txt", "UTF-8");
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            while (queries.hasNext()) {
                FloatVector query = queries.next();
                query.setEventTime(10000);
                query.setTTL(1000);

                //            SearchResult gt = gts.next();
                DataSerializer.serializeFloatVectorWithTimestampAndTTL(query, queryVec);
                byte[] resultBytes;

                long start = System.currentTimeMillis();
                if (query.getId() % sortInterval == sortInterval - 1) {
                    vectorSearchOptions.setTriggerSort(true);
                    vectorSearchOptions.setTs(query.getEventTime() - query.getTTL());
                    try {
                        resultBytes =
                                db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                    vectorSearchOptions.setTriggerSort(false);
                } else {
                    vectorSearchOptions.setTs(query.getEventTime() - query.getTTL());
                    try {
                        resultBytes =
                                db.vectorSearch(vectorCFHandle, vectorSearchOptions, queryVec);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                }

                SearchResult result =
                        DataSerializer.deserializeSearchResult(
                                resultBytes, 0, query.getId(), 1, query.getEventTime());

                writer.print(result.getQueryId());
                for (int i = 0; i < result.size(); i++) {
                    writer.print(" " + result.id(i));
                }
                writer.println();

                System.out.println(
                        "Query "
                                + query.getId()
                                + " done in "
                                + (System.currentTimeMillis() - start)
                                + " ms: "
                                + result);

                //            accuracies.add(SearchResult.getAccuracy(result, gt));
                //
                //            if (query.getId() % 100 == 99) {
                //                System.out.println((query.getId() + 1) + " queries: " +
                // accuracies);
                //                // average
                //                System.out.println("average: " +
                // accuracies.stream().mapToDouble(Float::doubleValue).average().getAsDouble());
                //            }
            }
            //         closeDB();
            writer.close();
        }
    }

    static void test() throws Exception {

        Thread insert = new Thread(new InsertRunner());
        Thread search = new Thread(new SearchRunner());
        insert.start();
        Thread.sleep(10000);
        search.start();

        insert.join();
        search.join();
    }

    public static void main(String[] args) throws Exception {
        test();
    }
}
