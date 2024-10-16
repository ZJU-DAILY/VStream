package cn.edu.zju.daily.pipeline;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.function.*;
import cn.edu.zju.daily.function.partitioner.PartitionFunction;
import cn.edu.zju.daily.util.MilvusUtil;
import cn.edu.zju.daily.util.Parameters;
import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromaSeparatedStreamingPipeline {

    private final Parameters params;
    MilvusUtil milvusUtil = new MilvusUtil();

    Logger LOG = LoggerFactory.getLogger(ChromaSeparatedStreamingPipeline.class);

    public ChromaSeparatedStreamingPipeline(Parameters params) throws InterruptedException {
        this.params = params;
    }

    private PartitionFunction getPartitioner() {
        Random random = new Random(2345678L);
        return PartitionFunction.getPartitionFunction(params, random);
    }

    private FlatMapFunction<FloatVector, PartitionedData> getUnaryPartitioner(boolean isQuery) {
        Random random = new Random(2345678L);
        return PartitionFunction.getUnaryPartitionFunction(params, random, isQuery);
    }

    public SingleOutputStreamOperator<SearchResult> apply(
            DataStream<FloatVector> data, DataStream<FloatVector> query) {

        // This implementation is partly wrong, because it uses different partitioners for data and
        // query, which are
        // "trained" with different data, resulting in different partitioning schemes. However, this
        // prevents
        SingleOutputStreamOperator<PartitionedData> partitionedData =
                data.flatMap(getUnaryPartitioner(false))
                        .name("data partition")
                        .setParallelism(1)
                        .setMaxParallelism(1);

        applyToDataStream(partitionedData);

        SingleOutputStreamOperator<PartitionedData> partitionedQuery =
                query.flatMap(getUnaryPartitioner(true))
                        .name("query partition")
                        .setParallelism(1)
                        .setMaxParallelism(1);

        return applyToQueryStream(partitionedQuery);
    }

    @Deprecated
    public SingleOutputStreamOperator<SearchResult> applyWithJoinedPartitioner(
            DataStream<FloatVector> data, DataStream<FloatVector> query) {
        PartitionFunction partitioner = getPartitioner();
        OutputTag<PartitionedData> partitionedQueryTag =
                new OutputTag<PartitionedData>("partitioned-query") {};

        SingleOutputStreamOperator<PartitionedData> partitionedData =
                data.connect(query)
                        .flatMap(partitioner)
                        .process(new PartitionedDataSplitFunction(partitionedQueryTag))
                        .setParallelism(1)
                        .setMaxParallelism(1)
                        .name("partition");

        SideOutputDataStream<PartitionedData> partitionedQuery =
                partitionedData.getSideOutput(partitionedQueryTag);

        applyToDataStream(partitionedData);
        return applyToQueryStream(partitionedQuery);
    }

    private void applyToDataStream(DataStream<PartitionedData> data) {
        data.keyBy(PartitionedData::getPartitionId)
                .process(new ChromaDBKeyedDataProcessFunction(params))
                .setParallelism(params.getParallelism())
                .setMaxParallelism(params.getParallelism())
                .name("data process")
                .slotSharingGroup("process");
    }

    private SingleOutputStreamOperator<SearchResult> applyToQueryStream(
            DataStream<PartitionedData> data) {
        SingleOutputStreamOperator<SearchResult> rawResults =
                data.keyBy(PartitionedData::getPartitionId)
                        .process(new ChromaDBKeyedQueryProcessFunction(params))
                        .setParallelism(params.getParallelism())
                        .setMaxParallelism(params.getParallelism())
                        .name("query process")
                        .slotSharingGroup("process");
        return rawResults
                .keyBy(SearchResult::getQueryId)
                .process(new PartialResultProcessFunction(params.getK()))
                .setParallelism(params.getReduceParallelism())
                .name("result reduce");
    }
}
