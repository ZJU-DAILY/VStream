package cn.edu.zju.daily.function;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.CustomChromaClient;
import cn.edu.zju.daily.util.CustomChromaCollection;
import cn.edu.zju.daily.util.CustomEmptyChromaEmbeddingFunction;
import cn.edu.zju.daily.util.Parameters;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromaDBKeyedProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedData, SearchResult> {

    private static final Logger LOG = LoggerFactory.getLogger(ChromaDBKeyedProcessFunction.class);

    private CustomChromaCollection collection;
    private ListState<FloatVector> state;
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

        ListStateDescriptor<FloatVector> descriptor1 =
                new ListStateDescriptor<>("pending", FloatVector.class);
        state = getRuntimeContext().getListState(descriptor1);
        ValueStateDescriptor<Integer> descriptor2 =
                new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(descriptor2);
        insertExecutor = Executors.newSingleThreadExecutor();

        String collectionName = params.getChromaCollectionName();
        String addressFile = params.getChromaAddressFile();
        List<String> addresses = readAddresses(addressFile);

        String address;
        try {
            address = addresses.get(getRuntimeContext().getIndexOfThisSubtask());
            if (address != null && !address.startsWith("http")) {
                address = "http://" + address;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    "No address for subtask " + getRuntimeContext().getIndexOfThisSubtask());
        }
        CustomChromaClient client = new CustomChromaClient(address);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("hnsw:space", params.getMetricType().toLowerCase());
        metadata.put("hnsw:M", Integer.toString(params.getHnswM()));
        metadata.put("hnsw:construction_ef", Integer.toString(params.getHnswEfConstruction()));
        metadata.put("hnsw:search_ef", Integer.toString(params.getHnswEfSearch()));

        this.collection =
                client.createCollection(
                        collectionName, metadata, true, new CustomEmptyChromaEmbeddingFunction());
    }

    @Override
    public void processElement(
            PartitionedData data,
            KeyedProcessFunction<Integer, PartitionedData, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        if (data.getDataType() == PartitionedData.DataType.QUERY) {
            // use another thread for searching?
            SearchResult result =
                    search(data.getVector(), data.getPartitionId(), data.getNumPartitionsSent());
            collector.collect(result);
        } else if (data.getDataType() == PartitionedData.DataType.INSERT_OR_DELETE) {
            insertOrDelete(data.getVector());
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
                    List<List<Float>> vectors =
                            vectorsToAdd.stream().map(FloatVector::list).collect(toList());
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
        CustomChromaCollection.QueryResponse queryResponse =
                collection.queryEmbeddings(
                        Collections.singletonList(query.list()), params.getK(), null, null, null);
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

    private void insertOrDelete(FloatVector vector) throws Exception {
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

            InsertAndDeleteRunnable runnable = new InsertAndDeleteRunnable(vectors);
            insertExecutor.execute(runnable);
            state.clear();
            count.update(0);
        } else {
            state.add(vector);
            count.update(count.value() + 1);
        }
    }

    static List<String> readAddresses(String addressFile) throws IOException {
        return Files.readAllLines(Paths.get(addressFile));
    }
}
