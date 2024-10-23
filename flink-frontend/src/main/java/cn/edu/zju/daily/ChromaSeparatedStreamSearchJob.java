package cn.edu.zju.daily;

import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.result.SearchResultEncoder;
import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.pipeline.ChromaSeparatedStreamingPipeline;
import cn.edu.zju.daily.util.Parameters;
import java.time.LocalDateTime;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ChromaSeparatedStreamSearchJob {

    private static final String DEFAULT_PARAM_PATH =
            "/home/auroflow/code/vector-search/VStream/flink-frontend/src/main/resources/params.yaml";

    private static final boolean doSearch = true;

    public static void main(String[] args) throws Exception {

        String paramPath = args.length > 0 ? args[0] : DEFAULT_PARAM_PATH;
        Parameters params = Parameters.load(paramPath, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HDFSVectorSourceBuilder source = new HDFSVectorSourceBuilder(env, params);
        ChromaSeparatedStreamingPipeline pipeline = new ChromaSeparatedStreamingPipeline(params);

        SingleOutputStreamOperator<VectorData> vectors = source.getSourceStream(true);
        SingleOutputStreamOperator<VectorData> queries = source.getQueryStream(true);
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
