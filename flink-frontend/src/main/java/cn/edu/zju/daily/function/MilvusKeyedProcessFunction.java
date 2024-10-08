package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.MilvusUtil;
import cn.edu.zju.daily.util.Parameters;
import io.milvus.response.SearchResultsWrapper;
import java.util.*;
import java.util.concurrent.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusKeyedProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedData, SearchResult> {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusKeyedProcessFunction.class);

    private ListState<FloatVector> state;
    private ValueState<Integer> count;
    private final int insertBatchSize;
    private MilvusUtil milvusUtil;
    private final Parameters params;
    private ExecutorService insertExecutor;

    public MilvusKeyedProcessFunction(Parameters params) {
        this.params = params;
        this.insertBatchSize = params.getMilvusInsertBufferCapacity();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<FloatVector> descriptor1 =
                new ListStateDescriptor<>("pending", FloatVector.class);
        state = getRuntimeContext().getListState(descriptor1);
        ValueStateDescriptor<Integer> descriptor2 =
                new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(descriptor2);
        insertExecutor = Executors.newSingleThreadExecutor();

        // initialize milvus connection
        milvusUtil = new MilvusUtil();
        milvusUtil.connect(params.getMilvusHost(), params.getMilvusPort());

        if (!milvusUtil.collectionExists(params.getMilvusCollectionName())) {
            LOG.error("Collection {} does not exist!", params.getMilvusCollectionName());
            throw new IllegalArgumentException("Collection does not exist.");
        }
        if (!milvusUtil.hasIndex(params.getMilvusCollectionName())) {
            LOG.error("No index on collection {}!", params.getMilvusCollectionName());
            throw new IllegalArgumentException("No index.");
        }
        if (!milvusUtil.isLoaded(params.getMilvusCollectionName())) {
            LOG.error("Collection {} not loaded!", params.getMilvusCollectionName());
            throw new IllegalArgumentException("Collection not loaded.");
        }
    }

    @Override
    public void processElement(
            PartitionedData data,
            KeyedProcessFunction<Integer, PartitionedData, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        int currentKey = context.getCurrentKey();
        if (currentKey != data.getPartitionId()) {
            throw new RuntimeException(
                    "Key mismatch: " + currentKey + " != " + data.getPartitionId());
        }

        if (data.getDataType() == PartitionedData.DataType.QUERY) {
            // use another thread for searching?
            SearchResult result =
                    search(data.getVector(), data.getPartitionId(), data.getNumPartitionsSent());
            if (result != null) {
                collector.collect(result);
            }
        } else if (data.getDataType() == PartitionedData.DataType.INSERT_OR_DELETE) {
            insertOrDelete(data.getVector(), currentKey);
        }
    }

    private class InsertAndDeleteRunnable implements Runnable {

        final List<FloatVector> vectors;
        final int partitionId;

        InsertAndDeleteRunnable(List<FloatVector> vectors, int partitionId) {
            this.vectors = vectors;
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            Set<Long> idsToDelete = new HashSet<>();
            List<FloatVector> vectorsToAdd = new ArrayList<>();
            for (int i = vectors.size() - 1; i >= 0; i--) {
                FloatVector v = vectors.get(i);
                if (v.isDeletion()) {
                    idsToDelete.add(v.getId());
                } else {
                    if (!idsToDelete.contains(v.getId())) {
                        vectorsToAdd.add(v);
                    }
                }
            }

            if (!idsToDelete.isEmpty()) {
                milvusUtil.delete(
                        new ArrayList<>(idsToDelete),
                        params.getMilvusCollectionName(),
                        Integer.toString(partitionId));
            }

            if (!vectorsToAdd.isEmpty()) {
                milvusUtil.insert(
                        vectorsToAdd,
                        params.getMilvusCollectionName(),
                        Integer.toString(partitionId),
                        false);
            }
        }
    }

    private SearchResult search(FloatVector query, int partitionId, int numSearchPartitions) {
        int k = params.getK();
        int efSearch = params.getHnswEfSearch();
        String collectionName = params.getMilvusCollectionName();
        String metricType = params.getMetricType();
        String partitionName = Integer.toString(partitionId);
        SearchResultsWrapper resultsWrapper =
                milvusUtil.search(
                        Collections.singletonList(query),
                        k,
                        efSearch,
                        collectionName,
                        partitionName,
                        metricType,
                        numSearchPartitions);
        if (resultsWrapper != null) {
            List<SearchResultsWrapper.IDScore> pairs = resultsWrapper.getIDScore(0);
            List<Long> ids = new ArrayList<>();
            List<Float> scores = new ArrayList<>();

            // getIDScore is ascending
            for (SearchResultsWrapper.IDScore pair : pairs) {
                ids.add(pair.getLongID());
                scores.add(pair.getScore());
            }
            return new SearchResult(
                    partitionId,
                    query.getId(),
                    ids,
                    scores,
                    1,
                    numSearchPartitions,
                    query.getEventTime());
        } else {
            return null;
        }
    }

    private void insertOrDelete(FloatVector vector, int partitionId) throws Exception {

        if (count.value() == null) {
            // initialize
            count.update(ThreadLocalRandom.current().nextInt(insertBatchSize));
        }

        if (count.value() == insertBatchSize - 1) {
            List<FloatVector> vectors = new ArrayList<>();
            for (FloatVector v : state.get()) {
                vectors.add(v);
            }
            vectors.add(vector);

            InsertAndDeleteRunnable runnable = new InsertAndDeleteRunnable(vectors, partitionId);
            insertExecutor.execute(runnable);
            state.clear();
            count.update(0);
        } else {
            state.add(vector);
            count.update(count.value() + 1);
        }
    }
}
