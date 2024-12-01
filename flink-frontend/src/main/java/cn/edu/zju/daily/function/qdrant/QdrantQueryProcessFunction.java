package cn.edu.zju.daily.function.qdrant;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.util.Parameters;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class QdrantQueryProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult> {

    private final Parameters params;
    private QdrantUtil util;
    private String collectionName;
    private int shard;
    private List<PartitionedElement> queryData;

    public QdrantQueryProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.util = new QdrantUtil(params.getQdrantHost(), params.getQdrantPort());
        this.collectionName = params.getQdrantCollectionName();
        this.shard = getRuntimeContext().getIndexOfThisSubtask();
        this.queryData = new ArrayList<>();
    }

    @Override
    public void processElement(
            PartitionedElement value,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context ctx,
            Collector<SearchResult> out)
            throws Exception {
        if (value.getDataType() == PartitionedElement.DataType.QUERY) {
            queryData.add(value);
            if (queryData.size() >= params.getQdrantQueryBatchSize()) {
                List<SearchResult> results = search(queryData);
                for (SearchResult result : results) {
                    out.collect(result);
                }
                queryData.clear();
            }
        }
    }

    private List<SearchResult> search(List<PartitionedElement> queryData) {
        long start = System.currentTimeMillis();
        List<SearchResult> results =
                util.search(
                        collectionName, queryData, shard, params.getK(), params.getHnswEfSearch());
        LOG.info(
                "Partition {}: {} queries (from #{}) returned in {} ms",
                getRuntimeContext().getIndexOfThisSubtask(),
                queryData.size(),
                queryData.get(0).getData().getId(),
                System.currentTimeMillis() - start);
        return results;
    }
}
