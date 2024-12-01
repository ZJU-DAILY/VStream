package cn.edu.zju.daily.pipeline;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.function.PartialResultProcessFunction;
import cn.edu.zju.daily.function.chroma.ChromaDBProcessFunction;
import cn.edu.zju.daily.function.milvus.MilvusUtil;
import cn.edu.zju.daily.partitioner.PartitionFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.Random;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class ChromaStreamingPipeline {
    private final Parameters params;
    MilvusUtil milvusUtil = new MilvusUtil();

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
            SingleOutputStreamOperator<VectorData> vectors,
            SingleOutputStreamOperator<VectorData> queries) {

        if (params.getPartitioner().startsWith("lsh")
                && params.getParallelism() < params.getLshNumFamilies()) {
            throw new RuntimeException("parallelism must be >= lshNumFamilies");
        }
        PartitionFunction partitioner = getPartitioner();
        return applyToPartitionedData(
                vectors.connect(queries).flatMap(partitioner).name("partition"));
    }

    /** Apply the streaming pipeline to an unpartitioned PartitionedData stream. */
    public SingleOutputStreamOperator<SearchResult> applyToHybridStream(
            SingleOutputStreamOperator<PartitionedElement> data) {

        PartitionFunction partitioner = getPartitioner();
        return applyToPartitionedData(data.flatMap(partitioner).name("partition"));
    }

    /**
     * Apply the stream pipelines to a streaming data set containing first vectors, then queries.
     */
    public SingleOutputStreamOperator<SearchResult> applyToPartitionedData(
            SingleOutputStreamOperator<PartitionedElement> data) {

        KeyedProcessFunction<Integer, PartitionedElement, SearchResult> processFunction =
                new ChromaDBProcessFunction(params);

        return data.keyBy(PartitionedElement::getPartitionId)
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
