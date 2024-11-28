package cn.edu.zju.daily.pipeline;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.function.PartialResultProcessFunction;
import cn.edu.zju.daily.function.PartitionedDataSplitFunction;
import cn.edu.zju.daily.function.milvus.MilvusDataProcessFunction;
import cn.edu.zju.daily.function.milvus.MilvusQueryProcessFunction;
import cn.edu.zju.daily.function.milvus.MilvusUtil;
import cn.edu.zju.daily.partitioner.PartitionFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

@Slf4j
public class MilvusSeparatedStreamingPipeline {

    private final Parameters params;
    MilvusUtil milvusUtil = new MilvusUtil();

    public MilvusSeparatedStreamingPipeline(Parameters params) throws InterruptedException {
        this.params = params;
        milvusUtil.connect(params.getMilvusHost(), params.getMilvusPort());

        String collectionName = params.getMilvusCollectionName();
        System.out.println("Preparing collection...");

        if (milvusUtil.collectionExists(collectionName)) {
            boolean deleted = milvusUtil.dropCollection(collectionName);
            if (deleted) {
                LOG.warn("ChromaCollection {} already exists, deleted.", collectionName);
            } else {
                throw new RuntimeException("Failed to delete existed collection.");
            }
        }
        milvusUtil.createCollection(
                params.getMilvusCollectionName(),
                params.getVectorDim(),
                params.getMilvusNumShards());
        int numPartitions = params.getParallelism();
        for (int i = 0; i < numPartitions; i++) {
            String partitionName = Integer.toString(i);
            milvusUtil.createPartition(collectionName, partitionName);
        }
        boolean indexBuilt =
                milvusUtil.buildHnswIndex(
                        params.getMilvusCollectionName(),
                        params.getMetricType(),
                        params.getHnswM(),
                        params.getHnswEfConstruction(),
                        params.getHnswEfSearch());
        boolean loaded = milvusUtil.loadCollection(collectionName);

        if (indexBuilt && loaded) {
            while (!milvusUtil.hasIndex(collectionName) || !milvusUtil.isLoaded(collectionName)) {
                System.out.println("Waiting for loading and collection build...");
                Thread.sleep(5000);
            }
            System.out.println("Done.");
        }

        System.out.println("Prepared.");
    }

    public PartitionFunction getPartitioner() {
        Random random = new Random(2345678L);
        return PartitionFunction.getPartitionFunction(params, random);
    }

    private FlatMapFunction<VectorData, PartitionedElement> getUnaryPartitioner(boolean isQuery) {
        Random random = new Random(2345678L);
        return PartitionFunction.getUnaryPartitionFunction(params, random, isQuery);
    }

    public SingleOutputStreamOperator<SearchResult> apply(
            DataStream<VectorData> data, DataStream<VectorData> query) {

        // This implementation is actually problematic, because it uses different partitioners for
        // data and
        // query, which are "trained" with different data, resulting in different partitioning
        // schemes. However, this
        // reduces pressure on partitioners.
        SingleOutputStreamOperator<PartitionedElement> partitionedData =
                partition(data, false, "data partition");
        applyToDataStream(partitionedData);

        SingleOutputStreamOperator<PartitionedElement> partitionedQuery =
                partition(query, true, "query partition");
        return applyToQueryStream(partitionedQuery);
    }

    @Deprecated
    public SingleOutputStreamOperator<SearchResult> applyWithJoinedPartitioner(
            DataStream<VectorData> data, DataStream<VectorData> query) {
        PartitionFunction partitioner = getPartitioner();
        OutputTag<PartitionedElement> partitionedQueryTag =
                new OutputTag<PartitionedElement>("partitioned-query") {};

        SingleOutputStreamOperator<PartitionedElement> partitionedData =
                data.connect(query)
                        .flatMap(partitioner)
                        .process(new PartitionedDataSplitFunction(partitionedQueryTag))
                        .setParallelism(1)
                        .setMaxParallelism(1)
                        .name("partition");

        SideOutputDataStream<PartitionedElement> partitionedQuery =
                partitionedData.getSideOutput(partitionedQueryTag);

        applyToDataStream(partitionedData);
        return applyToQueryStream(partitionedQuery);
    }

    public SingleOutputStreamOperator<PartitionedElement> partition(
            DataStream<VectorData> data, boolean isQuery, String name) {
        return data.flatMap(getUnaryPartitioner(isQuery))
                .name(name)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    public SingleOutputStreamOperator<Object> applyToDataStream(
            DataStream<PartitionedElement> data) {
        return data.keyBy(PartitionedElement::getPartitionId)
                .process(new MilvusDataProcessFunction(params))
                .setParallelism(params.getParallelism())
                .setMaxParallelism(params.getParallelism())
                .name("data process")
                .slotSharingGroup("process");
    }

    public SingleOutputStreamOperator<SearchResult> applyToQueryStream(
            DataStream<PartitionedElement> data) {
        SingleOutputStreamOperator<SearchResult> rawResults =
                data.keyBy(PartitionedElement::getPartitionId)
                        .process(new MilvusQueryProcessFunction(params))
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
