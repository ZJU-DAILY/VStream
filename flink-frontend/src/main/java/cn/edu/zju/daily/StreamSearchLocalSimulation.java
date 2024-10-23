package cn.edu.zju.daily;

import cn.edu.zju.daily.data.DataSerializer;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import org.apache.flink.contrib.streaming.vstate.*;
import org.rocksdb.*;

public class StreamSearchLocalSimulation {

    static boolean doInsert = true;
    static long timestamp = System.currentTimeMillis();

    static int m = 16;
    static int dim = 128;
    static int efConstruction = 128;
    static int k = 10;
    static long maxElements = 200000L;
    static long ssTableSize = 4096L * (1 << 20); // 4 G
    static long blockSize = 128 << 10; // 128 KB
    static long blockCacheSize = 1073741824; // 1 GB

    static float terminationWeight = 0.001f;
    static float terminationFactor = 0.8f;
    static float terminationThreshold = 0f;
    static float terminationLowerBound = 0.5f;
    static int sortInterval = 100;
    static int flushThreshold = 2;
    static int maxWriteBufferNumber = 5;

    static RocksDB db;
    static DBOptions dbOptions;
    static VectorColumnFamilyOptions vectorCFOptions;
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
        writeOptions = container.getWriteOptions();
        VectorCFDescriptor vectorCFDescriptor =
                new VectorCFDescriptor("test".getBytes(), vectorCFOptions);

        System.out.println("RocksDB dir: " + dir);
        // String dir = "./tmp/rocksdb- ???";
        new File(dir).mkdirs();
        db = RocksDB.open(dbOptions, dir, columnFamilyDescriptors, new ArrayList<>());
        vectorCFHandle = db.createVectorColumnFamily(vectorCFDescriptor);
    }

    static void closeDB() {
        db.close();
        dbOptions.close();
        vectorCFOptions.close();
        writeOptions.close();
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

    static void test() throws Exception {

        byte[] id = new byte[Long.BYTES];
        byte[] vec = new byte[dim * Float.BYTES];
        String dir =
                "./tmp/rocksdb-standalone-"
                        + LocalDateTime.now().toString().split("\\.")[0].replace(":", "-");

        long start = System.currentTimeMillis();

        openDB(dir, true);
        FloatVectorIterator vectors =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs", 4);
        FloatVectorIterator queries =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs", 10);

        int index = 0;
        Random random = new Random();
        for (FloatVector vector : vectors) {
            for (int i = 0; i < dim; i++) {
                vector.getValue()[i] += random.nextInt(3) - 1; // randomize
            }
            DataSerializer.serializeFloatVector(vector, id, vec);
            db.put(vectorCFHandle, writeOptions, id, vec);
            index++;
            if (index % 1000 == 0) {
                System.out.println("Inserted " + index + " vectors.");
            }
            if (index % 5000 == 0 && queries.hasNext()) {
                // search one
                FloatVector query = queries.next();
                query.setEventTime(System.currentTimeMillis());
                DataSerializer.serializeFloatVector(query, vec);
                byte[] resultBytes;

                long start1 = System.currentTimeMillis();
                if (query.getId() % sortInterval == sortInterval - 1) {
                    vectorSearchOptions.setTriggerSort(true);
                    resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, vec);
                    vectorSearchOptions.setTriggerSort(false);
                } else {
                    resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, vec);
                }

                SearchResult result =
                        DataSerializer.deserializeSearchResult(
                                resultBytes, 0, query.getId(), 1, query.getEventTime());
                System.out.println(
                        "Query "
                                + query.getId()
                                + " done in "
                                + (System.currentTimeMillis() - start1)
                                + " ms");
            }
        }

        System.out.println("Insert done in " + (System.currentTimeMillis() - start) + " ms.");
        //         closeDB();
    }

    public static void main(String[] args) throws Exception {
        test();
    }
}
