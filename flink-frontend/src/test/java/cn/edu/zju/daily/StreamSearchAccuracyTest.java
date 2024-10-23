package cn.edu.zju.daily;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.GroundTruthResultIterator;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.result.SearchResultEncoder;
import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.pipeline.RocksDBStreamingPipeline;
import cn.edu.zju.daily.util.Parameters;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

public class StreamSearchAccuracyTest {

    private static final Parameters params =
            Parameters.load(
                    "/home/auroflow/code/vector-search/rocksdb-stream/src/test/resources/test-params.yaml",
                    false);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new VectorEnvironmentCreator(params).prepareVectorEnvironment(env);

        HDFSVectorSourceBuilder source = new HDFSVectorSourceBuilder(env, params);
        RocksDBStreamingPipeline pipeline = new RocksDBStreamingPipeline(params);

        SingleOutputStreamOperator<PartitionedElement> data = source.getHybridStream(false);
        SingleOutputStreamOperator<SearchResult> results = pipeline.applyToHybridStream(data);
        String fileSinkPath = params.getFileSinkPath() + "/" + System.currentTimeMillis();
        FileSink<SearchResult> sink =
                FileSink.<SearchResult>forRowFormat(
                                new Path(fileSinkPath), new SearchResultEncoder())
                        .build();
        CloseableIterator<SearchResult> searchResults = results.executeAndCollect();
        // Compare with ground truth
        // searchResults.sort(Comparator.comparingLong(SearchResult::getQueryId));

        List<SearchResult> groundTruths = new ArrayList<>();
        GroundTruthResultIterator gtIt =
                GroundTruthResultIterator.fromFile(params.getGroundTruthPath(), params.getK());
        while (gtIt.hasNext()) {
            groundTruths.add(gtIt.next());
        }

        List<Float> accuracies = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();
        List<Integer> copies = new ArrayList<>();

        for (CloseableIterator<SearchResult> it = searchResults; it.hasNext(); ) {
            SearchResult result = it.next();

            int total = 0;
            int hit = 0;
            int queryId = (int) result.getQueryId();
            List<Long> gt = groundTruths.get(queryId).getIds();
            total += result.size();
            for (long r : result.getIds()) {
                if (gt.contains(r)) {
                    hit++;
                }
            }
            accuracies.add((float) hit / total);
            latencies.add(result.getSearchDuration());
            copies.add(result.getCopies());

            if (accuracies.size() % 100 == 99) {
                System.out.println(
                        (accuracies.size() + 1)
                                + " queries: "
                                + accuracies.stream()
                                        .skip(Math.max(0, accuracies.size() - 99))
                                        .collect(toList()));
                // average
                System.out.println(
                        "average: "
                                + accuracies.stream()
                                        .mapToDouble(Float::doubleValue)
                                        .average()
                                        .getAsDouble()
                                + ", latency: "
                                + latencies.stream()
                                        .mapToLong(Long::longValue)
                                        .average()
                                        .getAsDouble()
                                + ", copies: "
                                + copies.stream()
                                        .mapToInt(Integer::intValue)
                                        .average()
                                        .getAsDouble());
            }
        }
        searchResults.close();
    }
}
