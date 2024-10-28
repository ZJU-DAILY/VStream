package cn.edu.zju.daily.function;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.MilvusUtil;
import cn.edu.zju.daily.util.Parameters;
import io.milvus.response.SearchResultsWrapper;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Milvus search function. */
@Slf4j
public class MilvusKeyedQueryProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult> {

    private final Parameters params;
    ListState<PartitionedElement> buffer;
    ValueState<Integer> count;
    private MilvusUtil milvusUtil = null;

    public MilvusKeyedQueryProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<PartitionedElement> listStateDescriptor =
                new ListStateDescriptor<>("buffer", PartitionedElement.class);
        buffer = getRuntimeContext().getListState(listStateDescriptor);
        ValueStateDescriptor<Integer> countState =
                new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(countState);
        milvusUtil = new MilvusUtil();
        milvusUtil.connect(params.getMilvusHost(), params.getMilvusPort());
    }

    @Override
    public void processElement(
            PartitionedElement value,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context ctx,
            Collector<SearchResult> out)
            throws Exception {

        // initialize state
        if (count.value() == null) {
            count.update(0);
        }

        int bufferCapacity = params.getMilvusQueryBufferCapacity();
        int nodeId = getRuntimeContext().getIndexOfThisSubtask();

        // if buffer is full, insert to Milvus
        if (count.value() + 1 == bufferCapacity) {
            List<PartitionedElement> vectors = new ArrayList<>();
            for (PartitionedElement vector : buffer.get()) {
                vectors.add(vector);
            }
            vectors.add(value);
            List<SearchResult> results = search(vectors, nodeId);
            if (results != null) {
                for (SearchResult result : results) {
                    out.collect(result);
                }
            }
            buffer.clear();
            count.update(0);
        } else {
            buffer.add(value);
            count.update(count.value() + 1);
        }
    }

    private List<SearchResult> search(List<PartitionedElement> data, int partitionId) {
        int k = params.getK();
        int efSearch = params.getHnswEfSearch();
        String collectionName = params.getMilvusCollectionName();
        String metricType = params.getMetricType();
        String partitionName = Integer.toString(partitionId);
        List<FloatVector> vectors =
                data.stream().map(el -> el.getData().asVector()).collect(toList());

        long start = System.currentTimeMillis();
        SearchResultsWrapper resultsWrapper =
                milvusUtil.search(
                        vectors, k, efSearch, collectionName, partitionName, metricType, 0);
        if (resultsWrapper != null) {
            List<SearchResult> results = new ArrayList<>();

            for (int i = 0; i < vectors.size(); i++) {
                List<SearchResultsWrapper.IDScore> pairs = resultsWrapper.getIDScore(i);
                List<Long> ids = new ArrayList<>();
                List<Float> scores = new ArrayList<>();

                // getIDScore is ascending
                for (SearchResultsWrapper.IDScore pair : pairs) {
                    ids.add(pair.getLongID());
                    scores.add(pair.getScore());
                }
                results.add(
                        new SearchResult(
                                partitionId,
                                vectors.get(i).getId(),
                                ids,
                                scores,
                                1,
                                data.get(i).getNumPartitionsSent(),
                                vectors.get(i).getEventTime()));
            }

            if (vectors.size() == 1) {
                LOG.info(
                        "Partition {}: Query #{} (one of {} partitions) returned in {} ms.",
                        partitionId,
                        vectors.get(0).id(),
                        data.get(0).getNumPartitionsSent(),
                        System.currentTimeMillis() - start);
            } else {
                LOG.info(
                        "Partition {}: {} queries returned in {} ms.",
                        partitionId,
                        vectors.size(),
                        System.currentTimeMillis() - start);
            }
            return results;
        } else {
            return null;
        }
    }
}
