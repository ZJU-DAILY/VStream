package cn.edu.zju.daily;

import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.result.SearchResultEncoder;
import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.pipeline.MilvusSeparatedStreamingPipeline;
import cn.edu.zju.daily.util.Parameters;
import java.time.LocalDateTime;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MilvusSeparatedStreamSearchJob {

    private static final Parameters params =
            Parameters.load(
                    "/home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml",
                    false);

    private static final boolean doSearch = true;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HDFSVectorSourceBuilder source = new HDFSVectorSourceBuilder(env, params);
        MilvusSeparatedStreamingPipeline pipeline = new MilvusSeparatedStreamingPipeline(params);

        SingleOutputStreamOperator<FloatVector> vectors = source.getSourceStream(true);
        SingleOutputStreamOperator<FloatVector> queries = source.getQueryStream(true);
        SingleOutputStreamOperator<SearchResult> results = pipeline.apply(vectors, queries);

        String fileSinkPath =
                params.getFileSinkPath()
                        + "/"
                        + (LocalDateTime.now().toString().split("\\.")[0].replace(":", "-"));
        FileSink<SearchResult> sink =
                FileSink.<SearchResult>forRowFormat(
                                new Path(fileSinkPath), new SearchResultEncoder())
                        .build();
        results.sinkTo(sink);

        env.executeAsync();
    }
}
