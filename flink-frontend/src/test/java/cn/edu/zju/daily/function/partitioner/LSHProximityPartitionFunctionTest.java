package cn.edu.zju.daily.function.partitioner;

import static cn.edu.zju.daily.function.partitioner.LSHProximityPartitionFunction.ProximateHashValueGenerator;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.util.Parameters;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.io.IOException;
import java.util.*;
import me.tongfei.progressbar.ProgressBar;
import org.junit.jupiter.api.Test;

public class LSHProximityPartitionFunctionTest {

    @Test
    void testProximity() {

        int len = 10;
        int prox = 1;

        ProximateHashValueGenerator gen = new ProximateHashValueGenerator(new int[len], prox);
        int count = 0;
        while (gen.hasNext()) {
            count++;
            gen.next();
        }

        System.out.println(count);
    }

    @Test
    void testPartition() throws IOException {
        Parameters params =
                Parameters.load(
                        "/home/auroflow/code/vector-search/rocksdb-stream/src/test/resources/test-params.yaml",
                        false);
        FloatVectorIterator it =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs");
        LSHProximityPartitionFunction partitioner =
                (LSHProximityPartitionFunction)
                        PartitionFunction.getPartitionFunction(params, new Random(234567L));
        int count = 0;
        int[] counts = new int[params.getParallelism()];
        while (it.hasNext()) {
            FloatVector v = it.next();
            Set<Integer> nodeIds = partitioner.getNodeIds(v, params.getProximity());
            for (int nodeId : nodeIds) {
                counts[nodeId] += 1;
            }
        }
        System.out.println(Arrays.toString(counts));
    }

    @Test
    void testAccuracy() throws Exception {
        int parallelism = 120;
        int proximity = 1;
        int len = 128;

        Parameters params =
                Parameters.load(
                        "/home/auroflow/code/vector-search/rocksdb-stream/src/test/resources/test-params.yaml",
                        false);
        FloatVectorIterator vectors =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");
        FloatVectorIterator queries =
                FloatVectorIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_query.fvecs");
        LSHProximityPartitionFunction partitioner =
                (LSHProximityPartitionFunction)
                        PartitionFunction.getPartitionFunction(params, new Random(234567L));

        List<HnswIndex<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            indexes.add(
                    HnswIndex.newBuilder(len, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, 1000000)
                            .withM(16)
                            .withEfConstruction(128)
                            .build());
        }

        List<List<FloatVector>> data = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            data.add(new ArrayList<>());
        }

        try (ProgressBar pbar = new ProgressBar("Partitioning", 1000000)) {
            while (vectors.hasNext()) {
                FloatVector v = vectors.next();
                Set<Integer> nodeIds = partitioner.getNodeIds(v, 0);
                for (int nodeId : nodeIds) {
                    data.get(nodeId).add(v);
                }
                pbar.step();
            }
        }

        // print size
        for (int i = 0; i < parallelism; i++) {
            System.out.print(data.get(i).size() + " ");
        }
        System.out.println();

        try (ProgressBar pbar = new ProgressBar("Building", parallelism)) {
            for (int i = 0; i < parallelism; i++) {
                indexes.get(i).addAll(data.get(i));
                pbar.step();
            }
        }

        List<cn.edu.zju.daily.data.result.SearchResult> groundTruths = new ArrayList<>();
        GroundTruthResultIterator gtIt =
                GroundTruthResultIterator.fromFile(
                        "/home/auroflow/code/vector-search/data/sift/sift_groundtruth.ivecs", 10);
        while (gtIt.hasNext()) {
            groundTruths.add(gtIt.next());
        }

        List<Float> accuracy = new ArrayList<>();

        int partitionCount = 0;

        try (ProgressBar pbar = new ProgressBar("Searching", 10000)) {
            while (queries.hasNext()) {
                FloatVector q = queries.next();
                Set<Integer> nodeIds = partitioner.getNodeIds(q, 1);
                partitionCount += nodeIds.size();
                PriorityQueue<SearchResult<FloatVector, Float>> results =
                        new PriorityQueue<>(Comparator.reverseOrder());
                for (int nodeId : nodeIds) {
                    List<SearchResult<FloatVector, Float>> nearest =
                            indexes.get(nodeId).findNearest(q.vector(), 10);
                    results.addAll(nearest);
                }
                while (results.size() > 10) {
                    results.poll();
                }
                List<Long> ids =
                        results.stream()
                                .map(SearchResult::item)
                                .map(FloatVector::getId)
                                .collect(toList());
                int hit = 0;
                for (long id : ids) {
                    if (groundTruths.get((int) q.getId()).getIds().contains(id)) {
                        hit++;
                    }
                }
                accuracy.add((float) hit / 10);
                pbar.step();
            }
        }

        System.out.println((float) partitionCount / 10000);
        System.out.println(accuracy);
        System.out.println(
                accuracy.stream().mapToDouble(Float::doubleValue).average().getAsDouble());
    }
}
