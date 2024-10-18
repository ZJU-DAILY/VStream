package cn.edu.zju.daily.function;

import static cn.edu.zju.daily.util.ChromaUtil.chooseAddressToUse;
import static cn.edu.zju.daily.util.ChromaUtil.readAddresses;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.*;
import java.util.*;
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
import tech.amikos.chromadb.handler.ApiException;

/** Chroma insert function. */
public class ChromaDBKeyedDataProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedData, Object>
        implements CheckpointedFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChromaDBKeyedDataProcessFunction.class);

    private CustomChromaClient client;
    private String collectionName;
    private CustomChromaCollection collection;
    private ListState<FloatVector> state;
    private ValueState<Integer> count;
    private final Parameters params;
    private final int insertBatchSize;

    // private ExecutorService insertExecutor;

    public ChromaDBKeyedDataProcessFunction(Parameters params) {
        this.params = params;
        this.insertBatchSize = params.getChromaInsertBatchSize();
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
        // insertExecutor = Executors.newSingleThreadExecutor();

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        collectionName = params.getChromaCollectionName() + "_" + subtaskIndex;
        String addressFile = params.getChromaAddressFile();
        List<String> addresses = readAddresses(addressFile); // host:port_low:port_high

        // Get the hostname of the current task executor
        JobInfo jobInfo =
                new JobInfo(params.getFlinkJobManagerHost(), params.getFlinkJobManagerPort());
        String hostName =
                jobInfo.getHost(getRuntimeContext().getTaskName(), subtaskIndex).toLowerCase();

        // Find the chroma server address on this task executor
        String address = null;
        for (String a : addresses) {
            String host = a.split(":")[0].toLowerCase();
            if (host.equals(hostName)) {
                address = a;
                break;
            }
        }
        if (address == null) {
            throw new RuntimeException("No Chroma server address found for " + hostName);
        }

        String addressToUse = chooseAddressToUse(address, jobInfo, getRuntimeContext());
        LOG.info("Subtask {}: Using Chroma server at {}", subtaskIndex, addressToUse);

        client = new CustomChromaClient(addressToUse);
        client.setTimeout(600); // 10 minutes

        // Clear the collection if it already exists
        if (params.isChromaClearData()) {
            List<CustomChromaCollection> collections = client.listCollections();
            for (CustomChromaCollection c : collections) {
                try {
                    client.deleteCollection(c.getName());
                } catch (Exception e) {
                    LOG.error("Error deleting collection", e);
                }
            }
        }

        this.collection = getOrCreateCollection();
    }

    private CustomChromaCollection getOrCreateCollection() {
        try {
            // Create a new collection
            Map<String, String> metadata = new HashMap<>();
            metadata.put("hnsw:space", params.getMetricType().toLowerCase());
            metadata.put("hnsw:M", Integer.toString(params.getHnswM()));
            metadata.put("hnsw:construction_ef", Integer.toString(params.getHnswEfConstruction()));
            metadata.put("hnsw:search_ef", Integer.toString(params.getHnswEfSearch()));
            metadata.put("hnsw:batch_size", Integer.toString(params.getChromaHnswBatchSize()));
            metadata.put(
                    "hnsw:sync_threshold", Integer.toString(10 * params.getChromaHnswBatchSize()));

            return collection =
                    client.createCollection(
                            collectionName,
                            metadata,
                            true,
                            CustomEmptyChromaEmbeddingFunction.getInstance());
        } catch (ApiException e) {
            LOG.error("Error creating collection", e);
            return null;
        }
    }

    @Override
    public void processElement(
            PartitionedData data,
            KeyedProcessFunction<Integer, PartitionedData, Object>.Context context,
            Collector<Object> collector)
            throws Exception {

        if (data.getDataType() == PartitionedData.DataType.INSERT_OR_DELETE) {
            insertOrDelete(data.getVector());
        } else {
            throw new RuntimeException("Unsupported data type: " + data.getDataType());
        }
    }

    private class InsertAndDeleteRunnable implements Runnable {

        final List<FloatVector> insertsAndDeletes;

        InsertAndDeleteRunnable(List<FloatVector> insertsAndDeletes) {
            this.insertsAndDeletes = insertsAndDeletes;
        }

        @Override
        public void run() {
            Set<Long> idsToDelete = new HashSet<>();
            List<FloatVector> vectorsToAdd = new ArrayList<>();
            for (int i = insertsAndDeletes.size() - 1; i >= 0; i--) {
                FloatVector v = insertsAndDeletes.get(i);
                if (v.isDeletion()) {
                    idsToDelete.add(v.getId());
                } else {
                    if (!idsToDelete.contains(v.getId())) {
                        vectorsToAdd.add(v);
                    }
                }
            }

            if (!idsToDelete.isEmpty()) {
                try {
                    long now = System.currentTimeMillis();
                    deleteFromCollection(
                            idsToDelete.stream().map(Object::toString).collect(toList()));
                    LOG.info(
                            "Partition {}: Deleted {} vectors in {} ms",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            idsToDelete.size(),
                            System.currentTimeMillis() - now);
                } catch (Exception e) {
                    LOG.error("Error deleting vectors", e);
                }
            }

            if (!vectorsToAdd.isEmpty()) {
                try {
                    List<List<Float>> vectors =
                            vectorsToAdd.stream().map(FloatVector::list).collect(toList());
                    List<String> ids =
                            vectorsToAdd.stream()
                                    .map(FloatVector::getId)
                                    .map(Object::toString)
                                    .collect(toList());
                    List<Map<String, String>> metadatas =
                            vectorsToAdd.stream().map(FloatVector::getMetadata).collect(toList());
                    long now = System.currentTimeMillis();
                    addToCollection(vectors, ids, metadatas);
                    LOG.info(
                            "Partition {}: Inserted {} vectors (from #{}) in {} ms",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            vectorsToAdd.size(),
                            vectorsToAdd.get(0).getId(),
                            System.currentTimeMillis() - now);
                } catch (Exception e) {
                    LOG.error("Error inserting vectors to {}", collectionName, e);
                }
            }
        }

        private void addToCollection(
                List<List<Float>> vectors, List<String> ids, List<Map<String, String>> metadatas) {
            if (collection == null) {
                collection = getOrCreateCollection();
            }
            try {
                collection.add(vectors, metadatas, ids, ids);
            } catch (Exception e) {
                LOG.error("Error adding vectors, resetting collection.");
                collection = null;
            }
        }

        private void deleteFromCollection(List<String> ids) {
            if (collection == null) {
                collection = getOrCreateCollection();
            }
            try {
                collection.delete(ids, null, null);
            } catch (Exception e) {
                LOG.error("Error deleting vectors, resetting collection.");
                collection = null;
            }
        }
    }

    private void insertOrDelete(FloatVector vector) throws Exception {
        if (count.value() == null) {
            // initialize
            count.update(ThreadLocalRandom.current().nextInt(insertBatchSize));
        }

        state.add(vector);
        count.update(count.value() + 1);
        if (count.value() >= insertBatchSize) {
            flushState();
            count.update(0);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushState();
    }

    private void flushState() throws Exception {
        List<FloatVector> vectors = new ArrayList<>();
        for (FloatVector v : state.get()) {
            vectors.add(v);
        }
        if (!vectors.isEmpty()) {
            InsertAndDeleteRunnable runnable = new InsertAndDeleteRunnable(vectors);
            // insertExecutor.execute(runnable);
            runnable.run(); // synchronous, for now
        }
        state.clear();
    }
}
