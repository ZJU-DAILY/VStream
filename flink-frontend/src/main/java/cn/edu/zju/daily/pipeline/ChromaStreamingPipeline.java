package cn.edu.zju.daily.pipeline;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.function.ChromaDBKeyedProcessFunction;
import cn.edu.zju.daily.function.PartialResultProcessFunction;
import cn.edu.zju.daily.function.partitioner.PartitionFunction;
import cn.edu.zju.daily.util.MilvusUtil;
import cn.edu.zju.daily.util.Parameters;
import java.util.Random;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromaStreamingPipeline {
    private final Parameters params;
    MilvusUtil milvusUtil = new MilvusUtil();

    Logger LOG = LoggerFactory.getLogger(MilvusStreamingPipeline.class);

    public ChromaStreamingPipeline(Parameters params) throws InterruptedException {
        this.params = params;
    }

    private PartitionFunction getPartitioner() {
        Random random = new Random(2345678L);
        return PartitionFunction.getPartitionFunction(params, random);
    }

    /**
     * Apply the streaming pipeline to the throttled vectors and queries.
     *
     * @param vectors
     * @param queries
     * @return
     */
    public SingleOutputStreamOperator<SearchResult> apply(
            SingleOutputStreamOperator<FloatVector> vectors,
            SingleOutputStreamOperator<FloatVector> queries) {

        if (params.getParallelism() < params.getNumCopies()) {
            throw new RuntimeException("parallelism must be >= numCopies");
        }
        PartitionFunction partitioner = getPartitioner();
        return applyToPartitionedData(
                vectors.connect(queries).flatMap(partitioner).name("partition"));
    }

    /** Apply the streaming pipeline to an unpartitioned PartitionedData stream. */
    public SingleOutputStreamOperator<SearchResult> applyToHybridStream(
            SingleOutputStreamOperator<PartitionedData> data) {

        PartitionFunction partitioner = getPartitioner();
        return applyToPartitionedData(data.flatMap(partitioner).name("partition"));
    }

    /**
     * Apply the stream pipelines to a streaming data set containing first vectors, then queries.
     */
    public SingleOutputStreamOperator<SearchResult> applyToPartitionedData(
            SingleOutputStreamOperator<PartitionedData> data) {

        KeyedProcessFunction<Integer, PartitionedData, SearchResult> processFunction =
                new ChromaDBKeyedProcessFunction(params);

        return data.keyBy(PartitionedData::getPartitionId)
                .process(processFunction)
                .setParallelism(params.getParallelism())
                .setMaxParallelism(params.getParallelism())
                .name("insert & search")
                .keyBy(SearchResult::getQueryId)
                //            .countWindow(numPartitions)
                .process(new PartialResultProcessFunction(params.getK()))
                .setParallelism(params.getReduceParallelism())
                .name("reduce");
    }
}
