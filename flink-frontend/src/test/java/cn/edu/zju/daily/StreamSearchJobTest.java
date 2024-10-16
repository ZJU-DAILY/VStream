package cn.edu.zju.daily;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.function.PartialResultProcessFunction;
import cn.edu.zju.daily.function.RocksDBKeyedProcessFunction;
import cn.edu.zju.daily.function.partitioner.LSHPartitionFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class StreamSearchJobTest {

    private static Parameters params;

    @BeforeAll
    public static void setUpAll() {
        params =
                Parameters.load(
                        "/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/test-params.yaml",
                        false);
    }

    @Test
    void testGroundTruth() throws Exception {

        String sourcePath = "/home/auroflow/code/vector-search/data/siftsmall/siftsmall_base.fvecs";
        String queryPath = "/home/auroflow/code/vector-search/data/siftsmall/siftsmall_query.fvecs";
        String groundTruthPath =
                "/home/auroflow/code/vector-search/data/siftsmall/siftsmall_groundtruth.ivecs";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new VectorEnvironmentCreator(params).prepareVectorEnvironment(env);

        FloatVectorIterator vectorIt = FloatVectorIterator.fromFile(sourcePath);
        List<FloatVector> vectors = new ArrayList<>();
        while (vectorIt.hasNext()) {
            vectors.add(vectorIt.next());
        }

        FloatVectorIterator queryIt = FloatVectorIterator.fromFile(queryPath);
        List<FloatVector> queries = new ArrayList<>();
        while (queryIt.hasNext()) {
            queries.add(queryIt.next());
        }

        Random random = new Random(2345678L);
        LSHPartitionFunction partitioner =
                new LSHPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getNumCopies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth());

        List<PartitionedData> data = new ArrayList<>();

        Collector<PartitionedData> collector =
                new Collector<PartitionedData>() {
                    @Override
                    public void collect(PartitionedData record) {
                        data.add(record);
                    }

                    @Override
                    public void close() {}
                };

        for (FloatVector vector : vectors) {
            partitioner.flatMap1(vector, collector);
        }
        for (FloatVector query : queries) {
            partitioner.flatMap2(query, collector);
        }

        List<SearchResult> searchResults =
                env.fromCollection(data, TypeInformation.of(PartitionedData.class))
                        .keyBy(PartitionedData::getPartitionId)
                        .process(new RocksDBKeyedProcessFunction(100))
                        .setParallelism(params.getParallelism())
                        .setMaxParallelism(params.getParallelism())
                        .keyBy(SearchResult::getQueryId)
                        .process(new PartialResultProcessFunction(params.getK()))
                        .filter(SearchResult::isComplete)
                        .executeAndCollect(queries.size());

        searchResults.sort(Comparator.comparingLong(SearchResult::getQueryId));

        List<SearchResult> groundTruths = new ArrayList<>();
        GroundTruthResultIterator gtIt =
                GroundTruthResultIterator.fromFile(groundTruthPath, params.getK());
        while (gtIt.hasNext()) {
            groundTruths.add(gtIt.next());
        }

        List<Float> accuracies = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            int total = 0;
            int hit = 0;
            List<Long> gt = groundTruths.get(i).getIds();
            List<Long> result = searchResults.get(i).getIds();
            total += result.size();
            for (long r : result) {
                if (gt.contains(r)) {
                    hit++;
                }
            }
            accuracies.add((float) hit / total);
        }
        System.out.println(accuracies);
        System.out.println(
                "average: "
                        + accuracies.stream()
                                .mapToDouble(Float::doubleValue)
                                .average()
                                .getAsDouble());
    }

    @Test
    void testRandom() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        VectorEnvironmentCreator creator = new VectorEnvironmentCreator(params);
        creator.prepareVectorEnvironment(env);

        int dim = 2;
        int numPartitions = 8;
        int k = 10;

        List<FloatVector> vectors = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            vectors.add(FloatVector.getRandom(i, dim));
        }

        List<FloatVector> queries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            queries.add(FloatVector.getRandom(i, dim));
        }

        Random random = new Random(2345678L);
        LSHPartitionFunction partitioner =
                new LSHPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getNumCopies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        0.5F);

        env.fromCollection(vectors)
                .connect(env.fromCollection(queries))
                .flatMap(partitioner)
                .keyBy(PartitionedData::getPartitionId)
                .process(new RocksDBKeyedProcessFunction(100))
                .setParallelism(numPartitions)
                .setMaxParallelism(numPartitions)
                .keyBy(SearchResult::getQueryId)
                //            .countWindow(numPartitions)
                .process(new PartialResultProcessFunction(k))
                .filter(SearchResult::isComplete)
                .addSink(new DiscardingSink<>());

        env.executeAsync();
    }
}
