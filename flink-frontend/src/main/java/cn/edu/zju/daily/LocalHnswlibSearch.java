package cn.edu.zju.daily;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.partitioner.*;
import cn.edu.zju.daily.partitioner.curve.HilbertCurve;
import cn.edu.zju.daily.partitioner.curve.SpaceFillingCurve;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.nio.file.Paths;
import java.util.*;
import lombok.Getter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class LocalHnswlibSearch {

    private static final boolean indexBuilt = false;
    private static final String indexPath = "/home/auroflow/code/vector-search/index/index.idx";

    private static final List<Index<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();

    private static final int dimension = 128;
    private static final int m = 16;
    private static final int ef = 16;
    private static final int efConstruction = 128;
    private static final int maxElements = 100_000_00;
    private static final int k = 50;
    private static final int parallelism = 160;
    private static final int splitFactor = 8;
    private static final int totalElements = 1000000;

    private static final PartitionToKeyMapper mapper = new PartitionToKeyMapper(parallelism);

    @Getter
    private static class LocalCollector implements Collector<PartitionedElement> {

        private final List<Integer> partitions = new ArrayList<>();

        @Override
        public void collect(PartitionedElement element) {
            partitions.add(mapper.getPartition(element.getPartitionId()));
        }

        @Override
        public void close() {}

        public void clear() {
            partitions.clear();
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Running LocalHnswlibSearch");
        LocalCollector collector = new LocalCollector();
        SpaceFillingCurve.Builder builder = new HilbertCurve.Builder();

        // read partition type from args
        String partitionType = args[0];
        RichPartitionFunction partitioner = null;
        if (partitionType.equals("lsh")) {
            System.out.println("Using LSH partitioner");
            partitioner =
                    new LSHWithSpaceFillingPartitionFunction(
                            new Random(38324),
                            dimension,
                            splitFactor,
                            10,
                            5,
                            7,
                            totalElements,
                            totalElements,
                            totalElements,
                            parallelism,
                            builder);
        } else if (partitionType.equals("sfc")) {
            System.out.println("Using SFC partitioner");
            partitioner =
                    new SpaceFillingPartitionFunction(
                            dimension, -128, 128, 4, parallelism, 8, 100000, builder);
        } else {
            System.out.println("Invalid partitioner type");
            return;
        }

        FloatVectorIterator it =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs", 1);

        List<FloatVector> vectors = new ArrayList<>();
        long ts = 0;
        while (it.hasNext()) {
            FloatVector vector = it.next();
            vector.setEventTime(++ts);
            vector.setTTL(totalElements);
            vectors.add(vector);
        }

        System.out.println("Vectors read.");
        partitioner.open(new Configuration());

        if (partitioner instanceof Initializable) {
            ((Initializable) partitioner).initialize(vectors.subList(0, 1000000));
        }
        System.out.println("Partitioners initialized.");

        if (!indexBuilt) {
            for (int i = 0; i < parallelism; i++) {
                indexes.add(
                        HnswIndex.newBuilder(
                                        dimension,
                                        DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE,
                                        maxElements)
                                .withM(m)
                                .withEf(ef)
                                .withEfConstruction(efConstruction)
                                .build());
            }

            for (FloatVector vector : ProgressBar.wrap(vectors, "Building index")) {
                collector.clear();
                partitioner.flatMap1(vector, collector);
                for (int partition : collector.partitions) {
                    indexes.get(partition).add(vector);
                }
            }

            for (int i = 0; i < indexes.size(); i++) {
                indexes.get(i).save(Paths.get(indexPath + "." + i));
            }
        } else {
            for (int i = 0; i < parallelism; i++) {
                indexes.add(HnswIndex.load(Paths.get(indexPath + "." + i)));
            }
        }

        System.out.print("Partition sizes: ");
        for (int i = 0; i < parallelism; i++) {
            System.out.print(indexes.get(i).size() + " ");
        }
        System.out.println();

        FloatVectorIterator queries =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs", 1);

        GroundTruthResultIterator gt =
                GroundTruthResultIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_groundtruth.ivecs", 50);

        while (queries.hasNext() && gt.hasNext()) {
            FloatVector query = queries.next();
            Set<SearchResult<FloatVector, Float>> set = new HashSet<>();
            collector.clear();
            partitioner.flatMap1(query, collector);
            for (int partition : collector.partitions) {
                Index<Long, float[], FloatVector, Float> index = indexes.get(partition);
                List<SearchResult<FloatVector, Float>> nearest =
                        index.findNearest(query.vector(), k);
                set.addAll(nearest);
            }

            List<SearchResult<FloatVector, Float>> results = new ArrayList<>(set);
            results.sort(Comparator.comparing(SearchResult::distance));

            List<Long> ids = new ArrayList<>();
            List<Float> distances = new ArrayList<>();
            for (int j = 0; j < k; j++) {
                ids.add(results.get(j).item().id());
                distances.add(results.get(j).distance());
            }
            cn.edu.zju.daily.data.result.SearchResult result =
                    new cn.edu.zju.daily.data.result.SearchResult(query.id(), ids, distances);
            float acc = cn.edu.zju.daily.data.result.SearchResult.getAccuracy(result, gt.next());
            System.out.println("Query " + query.id() + " accuracy: " + acc);
        }
    }
}
