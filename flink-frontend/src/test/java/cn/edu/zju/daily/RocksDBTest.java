package cn.edu.zju.daily;

import cn.edu.zju.daily.data.DataSerializer;
import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import org.apache.flink.contrib.streaming.vstate.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Use RocksDBLocalTest in src folder instead.
 */
public class RocksDBTest {

    int m = 75;
    int dim = 128;
    int efConstruction = 128;
    int k = 10;
    long maxElements = 10001L;

    RocksDB db;
    DBOptions dbOptions;
    VectorColumnFamilyOptions vectorCFOptions;
    WriteOptions writeOptions;
    VectorSearchOptions vectorSearchOptions;
    VectorColumnFamilyHandle vectorCFHandle;
    FlushOptions flushOptions = new FlushOptions();

    @BeforeEach
    void openDB() throws RocksDBException {
        RocksDBResourceContainer container = new RocksDBResourceContainer(PredefinedOptions.DEFAULT,
            new RocksDBOptionsFactory() {
                @Override
                public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
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
                    currentOptions.setMaxElements(maxElements);
                    currentOptions.setSpace(SpaceType.L2);
                    currentOptions.setM(m);
                    currentOptions.setEfConstruction(efConstruction);
                    currentOptions.setDim(dim);
                    currentOptions.setMemTableConfig(
                        new HnswMemTableConfig(dim).setSpace(SpaceType.L2).setM(m).setMaxElements(maxElements));
                    currentOptions.setTableFormatConfig(new HnswTableOptions()
                        .setDim(dim)
                        .setSpace(SpaceType.L2)
                        .setM(m)
                    );
                    currentOptions.setTerminationThreshold(0f);
                    currentOptions.setTerminationWeight(0f);
                    return currentOptions;
                }

                @Override
                public VectorSearchOptions createVectorSearchOptions(
                    VectorSearchOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                    currentOptions.setK(k);
                    currentOptions.setTerminationFactor(0.5f);
                    return currentOptions;
                }
            }
        );


        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, container.getColumnOptions()));

        dbOptions = container.getDbOptions();
        vectorSearchOptions = container.getVectorSearchOptions();
        vectorCFOptions = container.getVectorColumnOptions();
        writeOptions = container.getWriteOptions();
        VectorCFDescriptor vectorCFDescriptor = new VectorCFDescriptor("test".getBytes(), vectorCFOptions);

        String dir = "./tmp/rocksdb-" + System.currentTimeMillis();
        new File(dir).mkdirs();
        db = RocksDB.open(dbOptions, dir, columnFamilyDescriptors, new ArrayList<>());
        vectorCFHandle = db.createVectorColumnFamily(vectorCFDescriptor);
    }

    @AfterEach
    void closeDB() {
        db.close();
        dbOptions.close();
        vectorCFOptions.close();
        writeOptions.close();
        vectorSearchOptions.close();
    }

    void flush() throws RocksDBException {
        db.flush(flushOptions, vectorCFHandle);
        System.out.println("Flushed.");
    }

    @Test
    void test() throws Exception {

        FloatVectorIterator vectors = FloatVectorIterator.fromFile(
            "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");
        byte[] id = new byte[Long.BYTES];
        byte[] vec = new byte[dim * Float.BYTES];
        for (FloatVector vector : vectors) {
            DataSerializer.serializeFloatVector(vector, id, vec);
            db.put(vectorCFHandle, writeOptions, id, vec);
            if (vector.getId() % 10000 == 0 && vector.getId() != 0) {
                flush();
            }
        }

        System.out.println("Insert done.");
        // flush();

        FloatVectorIterator queries = FloatVectorIterator.fromFile(
            "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs");
        GroundTruthResultIterator gts = GroundTruthResultIterator.fromFile(
            "/home/auroflow/code/vector-search/data/sift/sift_groundtruth.ivecs", k);

        List<Float> accuracies = new ArrayList<>();

        while (queries.hasNext()) {
            FloatVector query = queries.next();
            SearchResult gt = gts.next();
            DataSerializer.serializeFloatVector(query, vec);
            byte[] resultBytes;
            if (query.getId() % 10000 == 0 && query.getId() != 0) {
                vectorSearchOptions.setTriggerSort(true);
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, vec);
                vectorSearchOptions.setTriggerSort(false);
            } else {
                resultBytes = db.vectorSearch(vectorCFHandle, vectorSearchOptions, vec);
            }

            System.out.println("Query " + query.getId() + " done.");
            SearchResult result = DataSerializer.deserializeSearchResult(resultBytes, 0, query.getId(), 1, query.getEventTime());
            accuracies.add(SearchResult.getAccuracy(result, gt));
        }
        System.out.println(accuracies);
        // average
        System.out.println("average: " + accuracies.stream().mapToDouble(Float::doubleValue).average().getAsDouble());
    }
}
