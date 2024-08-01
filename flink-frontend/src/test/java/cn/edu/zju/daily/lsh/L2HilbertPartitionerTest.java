package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

public class L2HilbertPartitionerTest {

    @Test
    void test() throws IOException {

        long TTL = 100000;
        int numPartitions = 160;
        int[] partitions = new int[numPartitions];

        L2HilbertPartitioner partitioner = new L2HilbertPartitioner(128, 8, 1, 7, 1000000, 10000, TTL, numPartitions, new Random());

        FloatVectorIterator iter = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");
        partitioner.initializeWith(iter, 0);

        iter = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");

        for (int i = 0; i < 1000000; i++) {
            FloatVector vector = iter.next();
            vector.setEventTime(i);
            vector.setTTL(TTL);
            int partition = partitioner.getDataPartition(vector);
            partitions[partition]++;
        }

        for (int i = 0; i < numPartitions; i++) {
            System.out.println(partitions[i]);
        }
    }

    @Test
    void testAccuracy() throws Exception {
        int numElements = 10000;
        int numQueries = 100;
        int numPartitions = 120;
        int dim = 128;
        int k = 10;
        List<HnswIndex<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            indexes.add(HnswIndex.newBuilder(dim, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, numElements).build());
        }

        int numHashFamilies = 8;
        List<L2HilbertPartitioner> partitioners = new ArrayList<>();
        for (int i = 0; i < numHashFamilies; i++) {
            partitioners.add(new L2HilbertPartitioner(dim, 8, 1, 7, numElements, numElements, numElements, numPartitions, new Random()));
        }

        FloatVectorIterator vectors = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/siftsmall/siftsmall_base.fvecs");
        FloatVectorIterator queries = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/siftsmall/siftsmall_query.fvecs");
        GroundTruthResultIterator truth = GroundTruthResultIterator.fromFile("/home/auroflow/code/vector-search/data/siftsmall/siftsmall_groundtruth.ivecs", k);

        for (L2HilbertPartitioner partitioner : partitioners) {
            FloatVectorIterator initVectors = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/siftsmall/siftsmall_base.fvecs");
            partitioner.initializeWith(initVectors, 0);
        }

        for (int i = 0; i < numElements; i++) {
            if (i % 10000 == 0) {
                System.out.println("Inserting " + i);
            }
            FloatVector vector = vectors.next();
            vector.setEventTime(i);
            vector.setTTL(numElements);
            Set<Integer> partitions = new HashSet<>();
            for (L2HilbertPartitioner partitioner : partitioners) {
                int partition = partitioner.getDataPartition(vector);
                partitions.add(partition);
            }
            for (int partition : partitions) {
                indexes.get(partition).add(vector);
            }
        }

        List<Float> accuracies = new ArrayList<>();
        List<Integer> partitionCounts = new ArrayList<>();

        for (int i = 0; i < numQueries; i++) {
            if (i % 100 == 0) {
                System.out.println("Querying " + i);
            }
            FloatVector query = queries.next();
            SearchResult next = truth.next();

            query.setEventTime(i);
            query.setTTL(numElements);
            Set<Integer> partitions = new HashSet<>();
            for (L2HilbertPartitioner partitioner : partitioners) {
                List<Integer> partition = partitioner.getQueryPartition(query);
                partitions.addAll(partition);
            }
            partitionCounts.add(partitions.size());
            List<com.github.jelmerk.knn.SearchResult<FloatVector, Float>> results = new ArrayList<>();
            for (int partition : partitions) {
                List<com.github.jelmerk.knn.SearchResult<FloatVector, Float>> nearest = indexes.get(partition).findNearest(query.array(), k);
                results.addAll(nearest);
            }
            results.sort(Comparator.comparingDouble(com.github.jelmerk.knn.SearchResult::distance));
            results = results.subList(0, k);
            int count = 0;
            for (com.github.jelmerk.knn.SearchResult<FloatVector, Float> result : results) {
                if (next.getIds().contains(result.item().id())) {
                    count++;
                }
            }
            accuracies.add((float) count / k);
        }
        System.out.println(accuracies.stream().mapToDouble(Float::doubleValue).average().getAsDouble());
        System.out.println(partitionCounts.stream().mapToDouble(Integer::doubleValue).average().getAsDouble());
    }

    @Test
    void testSearchDuplication() throws Exception {
        int numElements = 1000000;
        int numPartitions = 120;
        int updateInterval = 10000;
        int maxTTL = 10000;
        int dim = 128;
        int k = 10;
        List<HnswIndex<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            indexes.add(HnswIndex.newBuilder(dim, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, numElements).build());
        }

        int numHashFamilies = 1;
        List<L2HilbertPartitioner> partitioners = new ArrayList<>();
        for (int i = 0; i < numHashFamilies; i++) {
            partitioners.add(new L2HilbertPartitioner(dim, 8, 1, 7, updateInterval, numElements, maxTTL, numPartitions, new Random()));
        }

        FloatVectorIterator vectors = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");
        FloatVectorIterator queries = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_query.fvecs");
        GroundTruthResultIterator truth = GroundTruthResultIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_groundtruth.ivecs", k);

        for (L2HilbertPartitioner partitioner : partitioners) {
            FloatVectorIterator initVectors = FloatVectorIterator.fromFile("/home/auroflow/code/vector-search/data/sift/sift_base.fvecs");
            partitioner.initializeWith(initVectors, 0);
        }

        List<Integer> partitionCounts = new ArrayList<>();

        for (int i = 0; i < 1000000; i++) {
            FloatVector vector = vectors.next();
            vector.setEventTime(i);
            vector.setTTL(numElements);
            Set<Integer> partitions = new HashSet<>();
            for (L2HilbertPartitioner partitioner : partitioners) {
                int partition = partitioner.getDataPartition(vector);
                partitions.add(partition);
            }
            for (int partition : partitions) {
                indexes.get(partition).add(vector);
            }

            if (i % 100 == 99) {
                FloatVector query = queries.next();

                query.setEventTime(i);
                query.setTTL(5000);
                Set<Integer> queryPartitions = new HashSet<>();
                for (L2HilbertPartitioner partitioner : partitioners) {
                    List<Integer> partition = partitioner.getQueryPartition(query);
                    queryPartitions.addAll(partition);
                }
                partitionCounts.add(queryPartitions.size());
            }
        }

        System.out.println(partitionCounts.stream().mapToDouble(Integer::doubleValue).average().getAsDouble());
    }
}
