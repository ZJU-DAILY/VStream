package cn.edu.zju.daily;

import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.result.SearchResultEncoder;
import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.pipeline.RocksDBStreamingPipeline;
import cn.edu.zju.daily.util.Parameters;
import java.time.LocalDateTime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class RocksDBRecoveryTest {

    private static final String DEFAULT_PARAM_PATH = "./src/test/resources/test-params.yaml";
    private static final String SAVEPOINT_PATH =
            "/home/auroflow/flink/flink-savepoints/savepoint-b949ea-4e330ad62c52";

    @Test
    void test() throws Exception {

        Parameters params = Parameters.load(DEFAULT_PARAM_PATH, false);

        Configuration conf = new Configuration();
        conf.setInteger("parallelism.default", 1);
        conf.setInteger("taskmanager.numberOfTaskSlots", 20);
        conf.setString("execution.savepoint.path", SAVEPOINT_PATH);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //                StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointTimeout(600000);

        new VectorEnvironmentCreator(params).prepareVectorEnvironment(env);

        HDFSVectorSourceBuilder source = new HDFSVectorSourceBuilder(env, params);
        RocksDBStreamingPipeline pipeline = new RocksDBStreamingPipeline(params);

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
        results.print();

        env.execute();
        System.out.println("Job exited, waiting for possible savepoint.");
        Thread.sleep(10000);
    }
}
