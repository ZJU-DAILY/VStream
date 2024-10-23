package cn.edu.zju.daily.function;

import static cn.edu.zju.daily.util.ChromaUtil.chooseAddressToUse;
import static cn.edu.zju.daily.util.ChromaUtil.readAddresses;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import cn.edu.zju.daily.util.*;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.amikos.chromadb.handler.ApiException;

/** Chroma insert function. */
public class ChromaDBKeyedDataProcessFunction extends VectorKeyedDataProcessFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChromaDBKeyedDataProcessFunction.class);

    private CustomChromaClient client;
    private String collectionName;
    private CustomChromaCollection collection;
    private final Parameters params;

    // private ExecutorService insertExecutor;

    public ChromaDBKeyedDataProcessFunction(Parameters params) {
        super(params.getChromaInsertBatchSize());
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

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
    protected void flushInserts(List<FloatVector> pendingInserts) {
        try {
            List<List<Float>> vectors =
                    pendingInserts.stream().map(FloatVector::list).collect(toList());
            List<String> ids =
                    pendingInserts.stream()
                            .map(FloatVector::getId)
                            .map(Object::toString)
                            .collect(toList());
            List<Map<String, String>> metadatas =
                    pendingInserts.stream().map(FloatVector::getMetadata).collect(toList());
            long now = System.currentTimeMillis();
            addToCollection(vectors, ids, metadatas);
            LOG.info(
                    "Partition {}: Inserted {} vectors (from #{}) in {} ms",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    pendingInserts.size(),
                    pendingInserts.get(0).getId(),
                    System.currentTimeMillis() - now);
        } catch (Exception e) {
            LOG.error("Error inserting vectors to {}", collectionName, e);
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

    @Override
    protected void flushDeletes(Map<Long, VectorDeletion> pendingDeletes) {
        try {
            long now = System.currentTimeMillis();
            deleteFromCollection(
                    pendingDeletes.keySet().stream().map(Object::toString).collect(toList()));
            LOG.info(
                    "Partition {}: Deleted {} vectors in {} ms",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    pendingDeletes.size(),
                    System.currentTimeMillis() - now);
        } catch (Exception e) {
            LOG.error("Error deleting vectors", e);
        }
    }

    private void deleteFromCollection(List<String> ids) {
        if (collection == null) {
            collection = getOrCreateCollection();
        }
        try {
            assert collection != null;
            collection.delete(ids, null, null);
        } catch (Exception e) {
            LOG.error("Error deleting vectors, resetting collection.");
            collection = null;
        }
    }
}
