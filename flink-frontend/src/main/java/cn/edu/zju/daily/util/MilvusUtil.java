package cn.edu.zju.daily.util;

import cn.edu.zju.daily.data.vector.FloatVector;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.GetCollStatResponseWrapper;
import io.milvus.response.SearchResultsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class MilvusUtil {

    public static final Logger LOG = LoggerFactory.getLogger(MilvusUtil.class);

    private MilvusServiceClient milvusServiceClient;

    public void connect(String host, int port) {
        ConnectParam param = ConnectParam.newBuilder().withHost(host).withPort(port).build();
        milvusServiceClient = new MilvusServiceClient(param);
        LOG.info("Milvus connected at {}:{}", host, port);
    }

    public boolean checkHealth() {
        long start = System.currentTimeMillis();
        R<CheckHealthResponse> response = milvusServiceClient.checkHealth();
        boolean isHealthy = response.getData().getIsHealthy();
        LOG.info("Check health returned {} in {} ms.", isHealthy, System.currentTimeMillis() - start);
        if (response.getStatus() != 0) {
            LOG.info("RPC failed.");
            return false;
        }
        return isHealthy;
    }

    public GetCollStatResponseWrapper getCollectionStatistics(String collectionName) {
        GetCollectionStatisticsParam param = GetCollectionStatisticsParam.newBuilder().withCollectionName(collectionName).build();
        R<GetCollectionStatisticsResponse> response = milvusServiceClient.getCollectionStatistics(param);
        if (response.getStatus() == 0) {
            GetCollStatResponseWrapper wrapper = new GetCollStatResponseWrapper(response.getData());
            LOG.info("Collection " + collectionName + " row count: " + wrapper.getRowCount());
            return wrapper;
        } else {
            LOG.info("RPC failed: " + response.getMessage());
            return null;
        }
    }

    public long getCollectionSize(String collectionName) {
        long start = System.currentTimeMillis();
        GetCollStatResponseWrapper stat = getCollectionStatistics(collectionName);
        LOG.info("getCollectionSize: collection stat returned in " + (System.currentTimeMillis() - start) + " ms.");
        if (stat != null) {
            return stat.getRowCount();
        } else {
            return -1;
        }
    }

    public boolean collectionExists(String collectionName) {
        R<Boolean> response = milvusServiceClient.hasCollection(
                HasCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build());
        if (response.getData()) {
            LOG.info("Collection " + collectionName + " already exists.");
        } else {
            LOG.info("Collection " + collectionName + " not exists.");
        }
        return response.getData();
    }

    public boolean dropCollection(String collectionName) {
        R<RpcStatus> rpcStatusR = milvusServiceClient.dropCollection(
                DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
        if (rpcStatusR.getStatus() == 0) {
            LOG.info("Collection " + collectionName + " has been dropped.");
            return true;
        } else {
            LOG.error("Collection " + collectionName + " drop failed, error code: " + rpcStatusR.getStatus());
            return false;
        }
    }

    public void createCollection(String collectionName, Integer vectorDimension, Integer shardsNum) {
        FieldType vectorId = FieldType.newBuilder()
                .withName("id")
                .withDescription("auto primary id")  // in fact not auto
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();
        FieldType ageField = FieldType.newBuilder()
                .withName("age")
                .withDescription("age")
                .withDataType(DataType.Int64)
                .build();
        FieldType vectorValue = FieldType.newBuilder()
                .withName("embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(vectorDimension)
                .build();
        CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDescription(collectionName)
                .withShardsNum(shardsNum)
                .addFieldType(vectorId)
                .addFieldType(ageField)
                .addFieldType(vectorValue)
                .build();
        R<RpcStatus> response = milvusServiceClient.createCollection(createCollectionParam);

        if (response.getStatus() == 0) {
            LOG.info("Collection " + collectionName + " has been created.");
        } else {
            LOG.error("Collection " + collectionName + " create failed, error code: " + response.getStatus());
        }
    }

    public void createPartition(String collectionName, String partitionName) {
        CreatePartitionParam createPartitionParam = CreatePartitionParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .build();
        R<RpcStatus> response = milvusServiceClient.createPartition(createPartitionParam);

        if (response.getStatus() == 0) {
            LOG.info("Partition {} of collection {} is created.", partitionName, collectionName);
        } else {
            LOG.error("Partition {} of collection {} creation failed, error: {}.", partitionName, collectionName, response.getStatus());
        }
    }

    public List<String> listCollections() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder().build();
        ShowCollectionsResponse response = milvusServiceClient.showCollections(param).getData();
        int count = response.getCollectionNamesCount();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            names.add(response.getCollectionNames(i));
        }
        return names;
    }

    public DescCollResponseWrapper describeCollection(String collectionName) {
        R<DescribeCollectionResponse> respDescribeCollection = milvusServiceClient.describeCollection(
                // Return the name and schema of the collection.
                DescribeCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()
        );
        return new DescCollResponseWrapper(respDescribeCollection.getData());
    }

    public boolean buildHnswIndex(String collectionName, String metrics, int m, int efConstruction, int efSearch) {
        IndexType indexType = IndexType.HNSW;
        MetricType metricType = getMetricType(metrics);
        String extraParam = "{\"M\":" + m + "," +
                "\"efConstruction\":" + efConstruction + "," +
                "\"ef\":" + efSearch + "}";

        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName("embedding")
                .withIndexType(indexType)
                .withMetricType(metricType)
                .withExtraParam(extraParam)
                .withSyncMode(true)
                .build();

        R<RpcStatus> response = milvusServiceClient.createIndex(indexParam);

        if (response.getStatus() != R.Status.Success.getCode()) {
            System.out.println("Build index error: " + response.getMessage());
            LOG.error(response.getMessage());
            return false;
        } else {
            LOG.info("Index built successfully.");
            return true;
        }
    }

    public boolean getIndexBuildFinished(String collectionName) {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .build();
        R<DescribeIndexResponse> response = milvusServiceClient.describeIndex(param);
        if (response.getStatus() != R.Status.Success.getCode()) {
            System.out.println(response.getMessage());
        }
        DescribeIndexResponse data = response.getData();
        return data.getIndexDescriptionsList().get(0).getState() == IndexState.Finished;
    }

    public void flush(String collectionName, boolean async) {
        FlushParam.Builder builder = FlushParam.newBuilder()
                .addCollectionName(collectionName);
        if (async) {
            builder.withSyncFlush(false);
        }
        FlushParam param = builder.build();
        R<FlushResponse> response = milvusServiceClient.flush(param);
        if (response.getStatus() != R.Status.Success.getCode()) {
            System.out.println(response.getMessage());
        }
        LOG.info("Collection {} flushed.", collectionName);
    }

    public boolean loadCollection(String collectionName) {
        R<RpcStatus> response = milvusServiceClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(true)
                .build());
        if (response.getStatus() != R.Status.Success.getCode()) {
            System.out.println("Load collection error: " + response.getMessage());
            LOG.error(response.getMessage());
            return false;
        } else {
            LOG.info("Collection " + collectionName + " load success.");
            return true;
        }
    }

    public boolean isLoaded(String collectionName) {
        R<GetLoadStateResponse> response = milvusServiceClient.getLoadState(GetLoadStateParam.newBuilder().withCollectionName(collectionName).build());
        if (response.getStatus() != R.Status.Success.getCode()) {
            LOG.error(response.getMessage());
            return false;
        } else {
            GetLoadStateResponse loadState = response.getData();
            return loadState.getState().getNumber() == LoadState.LoadStateLoaded_VALUE;
        }
    }

    public boolean hasIndex(String collectionName) {
        R<GetIndexStateResponse> response = milvusServiceClient.getIndexState(GetIndexStateParam.newBuilder().withCollectionName(collectionName).build());

        if (response.getStatus() != R.Status.Success.getCode()) {
            LOG.error(response.getMessage());
            return false;
        } else {
            GetIndexStateResponse indexState = response.getData();
            return indexState.getState().getNumber() == IndexState.Finished_VALUE;
        }
    }

    @SuppressWarnings("unchecked")
    public void insert(List<FloatVector> vectors, String collectionName, String partitionName, boolean async) {

        List<InsertParam.Field> fields = new ArrayList<>(3);
        fields.add(new InsertParam.Field("id", new ArrayList<Long>()));
        fields.add(new InsertParam.Field("age", new ArrayList<Long>()));
        fields.add(new InsertParam.Field("embedding", new ArrayList<List<Float>>()));

        for (FloatVector vector : vectors) {
            ((List<Long>) fields.get(0).getValues()).add(vector.id());
            ((List<Long>) fields.get(1).getValues()).add(vector.getEventTime());
            ((List<List<Float>>) fields.get(2).getValues()).add(vector.list());
        }
        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .withFields(fields)
                .build();
        if (async) {
            milvusServiceClient.insertAsync(insertParam);
            // TODO: check insert success
        } else {
            R<MutationResult> response = milvusServiceClient.insert(insertParam);
            if (response.getStatus() != R.Status.Success.getCode()) {
                LOG.error(response.getMessage());
            }
        }
    }


    /**
     *
     * @param searchVectors
     * @param topK
     * @param efSearch
     * @param collectionName
     * @param partitionName
     * @param metrics
     * @param numPartitionsSent Only for logging.
     * @return
     */
    public SearchResultsWrapper search(List<FloatVector> searchVectors, int topK, int efSearch, String collectionName,
                                       String partitionName, String metrics, int numPartitionsSent) {

        MetricType metricType = getMetricType(metrics);
        List<String> searchOutputFields = Collections.singletonList("id");
        String param = "{\"ef\":" + efSearch + "}";

        int size = searchVectors.size();

        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionNames(Collections.singletonList(partitionName))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .withMetricType(metricType)
                .withOutFields(searchOutputFields)
                .withTopK(topK)
                .withVectorFieldName("embedding")
                .withParams(param)
                .withVectors(searchVectors.stream().map(FloatVector::list).collect(toList()))
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withExpr("age > " + (searchVectors.get(0).getEventTime() - searchVectors.get(0).getTTL()))  // NOTE: age filter is determined by the first query
                .build();
        R<SearchResults> response = milvusServiceClient.search(searchParam);
        if (response.getStatus() != 0) {
            LOG.warn("Search response status: " + response.getStatus() + ", message: " + response.getMessage());
        }
        SearchResults data = response.getData();
        if (data == null) {
            return null;
        } else {
            return new SearchResultsWrapper(data.getResults());
        }
    }

    public void clientClose() {
        milvusServiceClient.close();
    }


    private MetricType getMetricType(String value) {
        switch (value) {
            case "L2":
                return MetricType.L2;
            case "HAMMING":
                return MetricType.HAMMING;
            case "JACCARD":
                return MetricType.JACCARD;
            default:
                throw new IllegalArgumentException("Metric type " + value + " not supported.");
        }
    }
}
