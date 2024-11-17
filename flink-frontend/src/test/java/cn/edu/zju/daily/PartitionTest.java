package cn.edu.zju.daily;

import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.partitioner.curve.HilbertCurve;
import cn.edu.zju.daily.partitioner.lsh.LSHashSpaceFillingPartitioner;
import cn.edu.zju.daily.util.Parameters;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/** Test the best NumHashFamilies. */
public class PartitionTest {

    private static final String dataPath = "/home/auroflow/vector-search/data/sift/sift_base.fvecs";
    private static final String queryPath =
            "/home/auroflow/vector-search/data/sift/sift_query.fvecs";
    private static final String gtPath =
            "/home/auroflow/vector-search/data/sift/sift_groundtruth.ivecs";

    static class Buffer {
        private List<FloatVector> vectors = new ArrayList<>();
        private Index<Long, float[], FloatVector, Float> index;
        private int capacity;

        Buffer(Index<Long, float[], FloatVector, Float> index, int capacity) {
            this.index = index;
            this.capacity = capacity;
        }

        public void add(FloatVector vector) throws InterruptedException {
            vectors.add(vector);
            if (vectors.size() == capacity) {
                index.addAll(vectors);
                vectors.clear();
            }
        }

        public void flush() throws InterruptedException {
            if (!vectors.isEmpty()) {
                index.addAll(vectors);
                vectors.clear();
            }
        }
    }

    private List<Index<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();
    private List<LSHashSpaceFillingPartitioner> partitioners = new ArrayList<>();

    private void initPartitioners(Parameters params) throws IOException {
        int dim = params.getVectorDim();
        int lshNumFamilies = params.getLshNumFamilies();
        int numPartitions = params.getParallelism();
        float lshBucketWidth = params.getLshBucketWidth();
        int hilbertBits = params.getLshNumSpaceFillingBits();

        Random random = new Random(38324);
        for (int i = 0; i < numPartitions; i++) {
            LSHashSpaceFillingPartitioner partitioner =
                    new LSHashSpaceFillingPartitioner(
                            dim,
                            lshNumFamilies,
                            lshBucketWidth,
                            hilbertBits,
                            Integer.MAX_VALUE,
                            10000,
                            Integer.MAX_VALUE,
                            numPartitions,
                            new HilbertCurve.Builder(),
                            new Random(random.nextLong()));
            Iterator<FloatVector> vectors = FloatVectorIterator.fromFile(dataPath, 1, 100000);
            partitioner.initializeWith(vectors, 0);
            partitioners.add(partitioner);
        }
    }

    private void insert(Parameters params) throws IOException, InterruptedException {
        int numPartitions = params.getParallelism();
        int dim = params.getVectorDim();
        int m = params.getHnswM();
        int efConstruction = params.getHnswEfConstruction();
        int efSearch = params.getHnswEfSearch();
        int lshNumFamilies = params.getLshNumFamilies();
        int maxElements = 2 * 10_000_000 * lshNumFamilies / numPartitions;

        List<Buffer> buffers = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            Index<Long, float[], FloatVector, Float> index =
                    HnswIndex.newBuilder(
                                    dim, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, maxElements)
                            .withM(m)
                            .withEf(efSearch)
                            .withEfConstruction(efConstruction)
                            .build();
            buffers.add(new Buffer(index, 1000));
            indexes.add(index);
        }

        Iterator<FloatVector> vectors = FloatVectorIterator.fromFile(dataPath, 1);
        int i = 0;
        while (vectors.hasNext()) {
            if (i % 10000 == 0) {
                System.out.println("Inserting " + i + "th vector.");
            }
            i++;
            FloatVector vector = vectors.next();
            Set<Integer> partitions = new HashSet<>();
            for (LSHashSpaceFillingPartitioner partitioner : partitioners) {
                partitions.add(partitioner.getDataPartition(vector));
            }
            for (int partition : partitions) {
                buffers.get(partition).add(vector);
            }
        }
        for (Buffer buffer : buffers) {
            buffer.flush();
        }
        System.out.println("Insert complete.");
        for (Buffer buffer : buffers) {
            System.out.print(buffer.index.size() + " ");
        }
        System.out.println();
    }

    private void search(Parameters params) throws Exception {
        Iterator<FloatVector> queries = FloatVectorIterator.fromFile(queryPath, 1);
        GroundTruthResultIterator gt = GroundTruthResultIterator.fromFile(gtPath, params.getK());
        while (queries.hasNext() && gt.hasNext()) {
            FloatVector query = queries.next();
            SearchResult gtResult = gt.next();
            Set<Integer> partitions = new HashSet<>();
            for (LSHashSpaceFillingPartitioner partitioner : partitioners) {
                List<Integer> partition = partitioner.getQueryPartition(query);
                if (partition.size() != 1) {
                    throw new RuntimeException("Query should be in one partition.");
                }
                partitions.add(partition.get(0));
            }

            PriorityQueue<com.github.jelmerk.knn.SearchResult<FloatVector, Float>> aggResult =
                    new PriorityQueue<>(params.getK(), Comparator.reverseOrder());

            for (int partition : partitions) {
                List<com.github.jelmerk.knn.SearchResult<FloatVector, Float>> nearest =
                        indexes.get(partition).findNearest(query.vector(), params.getK());
                aggResult.addAll(nearest);
                while (aggResult.size() > params.getK()) {
                    aggResult.poll();
                }
            }
            SearchResult result =
                    new SearchResult(
                            query.getId(),
                            aggResult.stream()
                                    .map(r -> r.item().getId())
                                    .collect(Collectors.toList()),
                            aggResult.stream().map(r -> r.distance()).collect(Collectors.toList()));
            float accuracy = SearchResult.getAccuracy(result, gtResult);
            System.out.println("Accuracy: " + accuracy);
        }
    }

    public static void main(String[] args) {
        Parameters params =
                Parameters.load(
                        "/home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml",
                        false);

        PartitionTest test = new PartitionTest();
        try {
            test.initPartitioners(params);
            test.insert(params);
            test.search(params);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
