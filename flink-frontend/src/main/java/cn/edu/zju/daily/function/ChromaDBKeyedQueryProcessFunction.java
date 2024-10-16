package cn.edu.zju.daily.function;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.*;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Chroma search function. */
public class ChromaDBKeyedQueryProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedData, SearchResult> {
    private static final Logger LOG =
            LoggerFactory.getLogger(ChromaDBKeyedQueryProcessFunction.class);

    private CustomChromaClient client;
    private CustomChromaCollection collection;
    private String collectionName;
    private final Parameters params;
    private List<PartitionedData> queryData;

    public ChromaDBKeyedQueryProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        collectionName = params.getChromaCollectionName() + "_" + subtaskIndex;
        String addressFile = params.getChromaAddressFile();
        List<String> addresses = ChromaUtil.readAddresses(addressFile); // host:port_low:port_high

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

        String addressToUse = ChromaUtil.chooseAddressToUse(address, jobInfo, getRuntimeContext());
        LOG.info("Subtask {}: Using Chroma server at {}", subtaskIndex, addressToUse);

        client = new CustomChromaClient(addressToUse);
        client.setTimeout(600); // 10 minutes

        this.collection = null; // initialized on demand
        this.queryData = new ArrayList<>();
    }

    @Override
    public void processElement(
            PartitionedData data,
            KeyedProcessFunction<Integer, PartitionedData, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        if (data.getDataType() == PartitionedData.DataType.QUERY) {
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

    private List<SearchResult> search(List<PartitionedData> queryData) throws Exception {

        if (collection == null) {
            try {
                collection =
                        client.getCollection(
                                collectionName, CustomEmptyChromaEmbeddingFunction.getInstance());
            } catch (Exception e) {
                LOG.error("Collection {} does not exist yet.", collectionName);
                return new ArrayList<>();
            }
        }

        long now = System.currentTimeMillis();
        Map<String, Object> where =
                Collections.singletonMap(
                        FloatVector.METADATA_TS_FIELD,
                        Collections.singletonMap(
                                "$gte",
                                queryData.get(0).getVector().getEventTime()
                                        - queryData.get(0).getVector().getTTL()));
        CustomChromaCollection.QueryResponse queryResponse =
                collection.queryEmbeddings(
                        queryData.stream().map(d -> d.getVector().list()).collect(toList()),
                        params.getK(),
                        where,
                        null,
                        null);
        LOG.info(
                "{} queries returned in {} ms", queryData.size(), System.currentTimeMillis() - now);
        List<List<String>> idsList = queryResponse.getIds();
        List<List<Float>> scoresList = queryResponse.getDistances();
        List<SearchResult> results = new ArrayList<>();
        for (int i = 0; i < idsList.size(); i++) {
            List<Long> ids = idsList.get(i).stream().map(Long::parseLong).collect(toList());
            List<Float> scores = scoresList.get(i);
            results.add(
                    new SearchResult(
                            queryData.get(i).getPartitionId(),
                            queryData.get(i).getVector().getId(),
                            ids,
                            scores,
                            1,
                            queryData.get(i).getNumPartitionsSent(),
                            queryData.get(i).getVector().getEventTime()));
        }
        return results;
    }
}
