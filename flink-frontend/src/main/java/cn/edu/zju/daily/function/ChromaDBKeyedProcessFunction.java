package cn.edu.zju.daily.function;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.util.*;
import cn.edu.zju.daily.util.chromadb.ChromaClient;
import cn.edu.zju.daily.util.chromadb.ChromaCollection;
import cn.edu.zju.daily.util.chromadb.ChromaUtil;
import cn.edu.zju.daily.util.chromadb.EmptyChromaEmbeddingFunction;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromaDBKeyedProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ChromaDBKeyedProcessFunction.class);

    private ChromaCollection collection;
    private ListState<VectorData> state;
    private ValueState<Integer> count;
    private final Parameters params;
    private final int insertBatchSize;
    private ExecutorService insertExecutor;

    public ChromaDBKeyedProcessFunction(Parameters params) {
        this.params = params;
        this.insertBatchSize = params.getChromaInsertBatchSize();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ListStateDescriptor<VectorData> descriptor1 =
                new ListStateDescriptor<>("pending", VectorData.class);
        state = getRuntimeContext().getListState(descriptor1);
        ValueStateDescriptor<Integer> descriptor2 =
                new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(descriptor2);
        insertExecutor = Executors.newSingleThreadExecutor();

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        String collectionName = params.getChromaCollectionNamePrefix() + "_" + subtaskIndex;
        String chromaAddress = ChromaUtil.getChromaAddress(params, getRuntimeContext());
        LOG.info("Subtask {}: Using Chroma server at {}", subtaskIndex, chromaAddress);

        ChromaClient client = new ChromaClient(chromaAddress);
        client.setTimeout(600); // 10 minutes

        // Clear the collection if it already exists
        if (params.isChromaClearData()) {
            List<ChromaCollection> collections = client.listCollections();
            for (ChromaCollection c : collections) {
                try {
                    client.deleteCollection(c.getName());
                } catch (Exception e) {
                    LOG.error("Error deleting collection", e);
                }
            }
        }

        // Create a new collection
        Map<String, Object> metadata = ChromaUtil.getHnswParams(params);
        this.collection =
                client.createCollection(
                        collectionName, metadata, true, EmptyChromaEmbeddingFunction.getInstance());
        LOG.info("Subtask {}: Connected to Chroma server at {}", subtaskIndex, chromaAddress);
    }

    @Override
    public void processElement(
            PartitionedElement data,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        if (data.getDataType() == PartitionedElement.DataType.QUERY) {
            // use another thread for searching?
            SearchResult result =
                    search(
                            data.getData().asVector(),
                            data.getPartitionId(),
                            data.getNumPartitionsSent());
            collector.collect(result);
        } else if (data.getDataType() == PartitionedElement.DataType.INSERT_OR_DELETE) {
            insertOrDelete(data.getData());
        }
    }

    private class InsertAndDeleteRunnable implements Runnable {

        final List<VectorData> insertsAndDeletes;

        InsertAndDeleteRunnable(List<VectorData> insertsAndDeletes) {
            this.insertsAndDeletes = insertsAndDeletes;
        }

        @Override
        public void run() {
            Set<Long> idsToDelete = new HashSet<>();
            List<FloatVector> vectorsToAdd = new ArrayList<>();
            for (int i = insertsAndDeletes.size() - 1; i >= 0; i--) {
                VectorData v = insertsAndDeletes.get(i);
                if (v.isDeletion()) {
                    idsToDelete.add(v.getId());
                } else {
                    if (!idsToDelete.contains(v.getId())) {
                        vectorsToAdd.add(v.asVector());
                    }
                }
            }

            if (!idsToDelete.isEmpty()) {
                try {
                    long now = System.currentTimeMillis();
                    collection.delete(
                            idsToDelete.stream().map(Object::toString).collect(toList()),
                            null,
                            null);
                    LOG.info(
                            "Deleted {} vectors in {} ms",
                            idsToDelete.size(),
                            System.currentTimeMillis() - now);
                } catch (Exception e) {
                    LOG.error("Error deleting vectors", e);
                }
            }

            if (!vectorsToAdd.isEmpty()) {
                try {
                    List<float[]> vectors =
                            vectorsToAdd.stream().map(FloatVector::getValue).collect(toList());
                    List<String> ids =
                            vectorsToAdd.stream()
                                    .map(FloatVector::getId)
                                    .map(Object::toString)
                                    .collect(toList());
                    long now = System.currentTimeMillis();
                    collection.add(vectors, null, ids, ids);
                    LOG.info(
                            "Inserted {} vectors in {} ms",
                            vectorsToAdd.size(),
                            System.currentTimeMillis() - now);
                } catch (Exception e) {
                    LOG.error("Error inserting vectors", e);
                }
            }
        }
    }

    private SearchResult search(FloatVector query, int partitionId, int numSearchPartitions)
            throws Exception {

        long now = System.currentTimeMillis();
        ChromaCollection.QueryResponse queryResponse =
                collection.queryEmbeddings(
                        Collections.singletonList(query.getValue()),
                        params.getK(),
                        null,
                        null,
                        null);
        LOG.info("1 query returned in {} ms", System.currentTimeMillis() - now);
        List<Long> ids =
                queryResponse.getIds().get(0).stream().map(Long::parseLong).collect(toList());
        List<Float> scores = queryResponse.getDistances().get(0);
        return new SearchResult(
                partitionId,
                query.getId(),
                ids,
                scores,
                1,
                numSearchPartitions,
                query.getEventTime());
    }

    private void insertOrDelete(VectorData data) throws Exception {
        if (count.value() == null) {
            // initialize
            count.update(ThreadLocalRandom.current().nextInt(insertBatchSize));
        }

        if (count.value() == insertBatchSize - 1) {
            List<VectorData> vectors = new ArrayList<>();
            for (VectorData v : state.get()) {
                vectors.add(v);
            }
            vectors.add(data);

            InsertAndDeleteRunnable runnable = new InsertAndDeleteRunnable(vectors);
            // insertExecutor.execute(runnable);
            runnable.run();
            state.clear();
            count.update(0);
        } else {
            state.add(data);
            count.update(count.value() + 1);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        List<VectorData> vectors = new ArrayList<>();
        for (VectorData v : state.get()) {
            vectors.add(v);
        }
        if (!vectors.isEmpty()) {
            InsertAndDeleteRunnable runnable = new InsertAndDeleteRunnable(vectors);
            insertExecutor.execute(runnable);
            state.clear();
            count.update(0);
        }
    }
}
