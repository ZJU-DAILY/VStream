package cn.edu.zju.daily;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.PartitionFunction;
import cn.edu.zju.daily.partitioner.PartitionToKeyMapper;
import cn.edu.zju.daily.partitioner.SimplePartitionFunction;
import cn.edu.zju.daily.util.Parameters;
import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MilvusRecallTestJob {
    private static final Parameters params = new Parameters();

    private static final int total = 2000000;
    //    private static final int total = 1000000;
    private static final int batch = 25000;
    // private static final int batch = 1000;
    //    private static final int numSamples = 1000;
    private static final int numSamples = 100;
    private static final int numBatches = 20;
    private static final int parallelism = 160;
    private static final int m = 16;
    private static final int efConstruction = 128;
    private static final int ef = 16;
    private static final int dim = 128;
    private static final PartitionToKeyMapper mapper = new PartitionToKeyMapper(parallelism);
    private static final int k = 50;
    private static final int replication = 10;

    private static final ExecutorService es = Executors.newWorkStealingPool();

    static {
        params.setHdfsAddress("hdfs://10.214.151.23:9000");
        params.setHdfsUser("auroflow");
        params.setSourcePath("/user/auroflow/vector_search/dataset/vectors/twitter7.fvecs");
        params.setWaitingIndexStrategy("none");
        params.setVectorDim(dim);
        params.setInsertSkip(0);
        params.setInsertLimitPerLoop(total);
        params.setInsertReadBulkSize(1000);
        params.setInsertLoops(1);
        params.setDeleteRatio(0);
    }

    private static class LocalCollector implements Collector<PartitionedElement> {

        private final Set<Integer> partitions = new HashSet<>();

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

    private static class SearchTask implements Callable<List<Long>>, Supplier<List<Long>> {

        VectorData query;
        List<VectorData> data;

        SearchTask(VectorData query, List<VectorData> data) {
            this.query = query;
            this.data = data;
        }

        @Override
        public List<Long> call() {
            PriorityQueue<Pair<Long, Double>> top =
                    new PriorityQueue<>(
                            k,
                            Comparator.<Pair<Long, Double>, Double>comparing(Pair::getRight)
                                    .reversed());
            for (int j = 0; j < data.size(); j++) {
                VectorData target = data.get(j);
                DistanceFunction<float[], Float> dist = DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE;
                double distance = dist.distance(query.getValue(), target.getValue());
                top.add(Pair.of(target.getId(), distance));
                if (top.size() > k) {
                    top.poll();
                }
            }
            List<Long> result = new ArrayList<>();
            while (!top.isEmpty()) {
                result.add(top.poll().getLeft());
            }
            Collections.reverse(result);
            return result;
        }

        @Override
        public List<Long> get() {
            return call();
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<VectorData> source =
                new HDFSVectorSourceBuilder(env, params).getSourceStream(false);

        List<VectorData> vectors = source.executeAndCollect(total);
        List<VectorData> queries = new ArrayList<>();

        for (int i = 0; i < vectors.size(); i++) {
            vectors.get(i).setEventTime(i);
        }
        Random random = new Random(66984);
        for (int i = 0; i < numBatches; i++) {
            int startPos = total - (i + 1) * batch;
            for (int j = 0; j < numSamples; j++) {
                queries.add(vectors.get(random.nextInt(batch) + startPos));
            }
        }

        List<HnswIndex<Long, float[], FloatVector, Float>> indexes = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            HnswIndex<Long, float[], FloatVector, Float> index =
                    HnswIndex.newBuilder(dim, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, total)
                            .withM(m)
                            .withEfConstruction(efConstruction)
                            .withEf(ef)
                            .build();
            indexes.add(index);
        }

        //        LSHWithSpaceFillingPartitionFunction partitioner =
        //                new LSHWithSpaceFillingPartitionFunction(
        //                        random,
        //                        dim,
        //                        replication,
        //                        10,
        //                        5,
        //                        7,
        //                        1000000007,
        //                        10000000,
        //                        1000000007,
        //                        parallelism,
        //                        new Builder());
        //        partitioner.open(new Configuration());
        //        partitioner.initialize(vectors);
        SimplePartitionFunction partitioner = new SimplePartitionFunction(parallelism);
        partitioner.open(new Configuration());

        insert(vectors.subList(0, total - numBatches * batch), indexes, partitioner);

        for (int b = 0; b < numBatches; b++) {

            float recall = query(queries, vectors, indexes, partitioner);
            System.out.println("Total recall: " + recall);
            insert(
                    vectors.subList(
                            total - (numBatches - b) * batch, total - (numBatches - b - 1) * batch),
                    indexes,
                    partitioner);
        }

        float recall = query(queries, vectors, indexes, partitioner);
        System.out.println("Total recall: " + recall);
    }

    private static void insert(
            List<VectorData> vectors,
            List<HnswIndex<Long, float[], FloatVector, Float>> indexes,
            PartitionFunction partitioner)
            throws Exception {
        LocalCollector collector = new LocalCollector();
        List<List<FloatVector>> buffers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            buffers.add(new ArrayList<>());
        }
        for (VectorData vector : ProgressBar.wrap(vectors, "Partitioning")) {
            collector.clear();
            partitioner.flatMap1(vector, collector);
            for (int partition : collector.partitions) {
                buffers.get(partition).add(vector.asVector());
            }
        }
        try (ProgressBar pbar = new ProgressBar("Create index", parallelism)) {
            for (int i = 0; i < parallelism; i++) {
                indexes.get(i).addAll(buffers.get(i));
                pbar.step();
            }
        }
    }

    private static float query(
            List<VectorData> queries,
            List<VectorData> data,
            List<HnswIndex<Long, float[], FloatVector, Float>> indexes,
            PartitionFunction partitioner)
            throws Exception {
        LocalCollector collector = new LocalCollector();
        float totalRecall = 0;

        // search
        List<List<Long>> results = new ArrayList<>();
        for (VectorData query : ProgressBar.wrap(queries, "Query vector")) {
            collector.clear();
            partitioner.flatMap2(query, collector);
            //            List<SearchResult<FloatVector, Float>> total = new ArrayList<>(k);
            Set<SearchResult<FloatVector, Float>> total = new HashSet<>(k);
            for (int partition : collector.partitions) {
                total.addAll(indexes.get(partition).findNearest(query.getValue(), k));
            }
            List<SearchResult<FloatVector, Float>> totalList = new ArrayList<>(total);
            totalList.sort(Comparator.comparing(SearchResult::distance));

            List<Long> result =
                    totalList.subList(0, k).stream()
                            .map(SearchResult::item)
                            .map(FloatVector::id)
                            .collect(toList());

            results.add(result);
        }

        // ground truth
        List<CompletableFuture<List<Long>>> futures = new ArrayList<>();
        for (VectorData query : queries) {
            futures.add(CompletableFuture.supplyAsync(new SearchTask(query, data), es));
        }
        List<List<Long>> gt = new ArrayList<>();
        for (CompletableFuture<List<Long>> future : ProgressBar.wrap(futures, "Ground truth")) {
            gt.add(future.get());
        }

        // Compare
        for (int i = 0; i < queries.size(); i++) {
            totalRecall += getRecall(results.get(i), gt.get(i));
        }

        return totalRecall / queries.size();
    }

    private static float getRecall(List<Long> results, List<Long> gt) {
        //        System.out.println(results);
        //        System.out.println(gt);
        int total = 0;
        int hit = 0;
        for (long result : results) {
            total++;
            if (gt.contains(result)) {
                hit++;
            }
        }
        return (float) hit / total;
    }
}
