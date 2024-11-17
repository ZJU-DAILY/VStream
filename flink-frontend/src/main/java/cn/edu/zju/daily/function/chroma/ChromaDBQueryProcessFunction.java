package cn.edu.zju.daily.function.chroma;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.function.chroma.util.ChromaClient;
import cn.edu.zju.daily.function.chroma.util.ChromaCollection;
import cn.edu.zju.daily.function.chroma.util.ChromaUtil;
import cn.edu.zju.daily.function.chroma.util.EmptyChromaEmbeddingFunction;
import cn.edu.zju.daily.util.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Chroma search function. */
@Slf4j
public class ChromaDBQueryProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult> {

    private ChromaClient client;
    private ChromaCollection collection;
    private String collectionName;
    private final Parameters params;
    private List<PartitionedElement> queryData;

    public ChromaDBQueryProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        collectionName = params.getChromaCollectionNamePrefix() + "_" + subtaskIndex;
        String chromaAddress = ChromaUtil.getChromaAddress(params, getRuntimeContext());
        LOG.info("Subtask {}: Using Chroma server at {}", subtaskIndex, chromaAddress);

        client = new ChromaClient(chromaAddress);
        client.setTimeout(600); // 10 minutes

        this.collection = null; // lazy initialization
        this.queryData = new ArrayList<>();
    }

    @Override
    public void processElement(
            PartitionedElement data,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        if (data.getDataType() == PartitionedElement.DataType.QUERY) {
            // use another thread for searching?
            queryData.add(data);
            if (queryData.size() >= params.getChromaQueryBatchSize()) {
                List<SearchResult> results = search(queryData);
                for (SearchResult result : results) {
                    collector.collect(result);
                }
                queryData.clear();
            }
        } else {
            throw new RuntimeException("Unexpected data type: " + data.getDataType());
        }
    }

    private List<SearchResult> search(List<PartitionedElement> queryData) {

        if (queryData.isEmpty()) {
            return Collections.emptyList();
        }

        if (collection == null) {
            try {
                collection =
                        client.getCollection(
                                collectionName, EmptyChromaEmbeddingFunction.getInstance());
            } catch (Exception e) {
                LOG.error("ChromaCollection {} does not exist yet.", collectionName);
                return getResultList(queryData, null, null);
            }
        }

        /*
        If metadata weren't this slow, we would have included this where condition:
        Map<String, Object> where =
                Collections.singletonMap(
                        FloatVector.METADATA_TS_FIELD,
                        Collections.singletonMap(
                                "$gte",
                                queryData.get(0).getData().getEventTime()
                                        - queryData.get(0).getData().getTTL()));
        */

        ChromaCollection.QueryResponse queryResponse;
        long now = System.currentTimeMillis();
        try {
            queryResponse =
                    collection.queryEmbeddings(
                            queryData.stream()
                                    .map(d -> d.getData().asVector().getValue())
                                    .collect(toList()),
                            params.getK(),
                            null,
                            null,
                            null);
        } catch (Exception e) {
            LOG.error("ChromaDB query failed, will reinitialize collection.");
            collection = null;
            return getResultList(queryData, null, null);
        }

        LOG.info(
                "Partition {}: {} queries (from #{}) returned in {} ms",
                getRuntimeContext().getIndexOfThisSubtask(),
                queryData.size(),
                queryData.get(0).getData().getId(),
                System.currentTimeMillis() - now);
        return getResultList(queryData, queryResponse.getIds(), queryResponse.getDistances());
    }

    private List<SearchResult> getResultList(
            List<PartitionedElement> queryData,
            List<List<String>> idsList,
            List<List<Float>> scoresList) {
        Objects.requireNonNull(queryData);

        List<SearchResult> results = new ArrayList<>();
        for (int i = 0; i < queryData.size(); i++) {
            List<Long> ids;
            if (idsList != null) {
                ids = idsList.get(i).stream().map(Long::parseLong).collect(toList());
            } else {
                ids = Collections.emptyList();
            }
            List<Float> scores;
            if (scoresList != null) {
                scores = scoresList.get(i);
            } else {
                scores = Collections.emptyList();
            }
            results.add(
                    new SearchResult(
                            queryData.get(i).getPartitionId(),
                            queryData.get(i).getData().getId(),
                            ids,
                            scores,
                            1,
                            queryData.get(i).getNumPartitionsSent(),
                            queryData.get(i).getData().getEventTime()));
        }
        return results;
    }
}
