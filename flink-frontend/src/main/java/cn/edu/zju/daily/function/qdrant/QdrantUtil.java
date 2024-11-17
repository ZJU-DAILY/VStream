package cn.edu.zju.daily.function.qdrant;

import static io.qdrant.client.ConditionFactory.range;
import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ShardKeyFactory.shardKey;
import static io.qdrant.client.ShardKeySelectorFactory.shardKeySelector;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.Parameters;
import com.google.common.util.concurrent.ListenableFuture;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.*;
import io.qdrant.client.grpc.Points.*;
import io.qdrant.client.grpc.QdrantOuterClass.HealthCheckReply;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QdrantUtil implements AutoCloseable {

    private final QdrantClient client;
    private static final Duration TIMEOUT = Duration.ofMinutes(10);
    private static final String TS_FIELD = "ts";
    private static final Duration WAIT_TIME = Duration.ofSeconds(120);
    private static final String CONSENSUS_TIMEOUT_EXCEPTION =
            "INTERNAL: Service internal error: Waiting for consensus operation commit failed. Timeout set at: 10 seconds";

    public QdrantUtil(String host, int port) {
        QdrantGrpcClient grpc =
                QdrantGrpcClient.newBuilder(host, port, false).withTimeout(TIMEOUT).build();
        this.client = new QdrantClient(grpc);
        try {
            checkHealth();
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Qdrant at " + host + ":" + port, e);
        }
    }

    private void checkHealth() throws ExecutionException, InterruptedException {
        Future<HealthCheckReply> future = client.healthCheckAsync();
        future.get();
    }

    public boolean collectionExists(String collectionName) {
        Future<Boolean> future = client.collectionExistsAsync(collectionName);
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to check collection existence", e);
        }
    }

    public CollectionInfo getCollectionInfo(String collectionName) {
        ListenableFuture<CollectionInfo> future = client.getCollectionInfoAsync(collectionName);
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get collection details", e);
        }
    }

    public void createCollectionAndShards(Parameters params) throws Exception {
        Distance distType;
        switch (params.getMetricType().toLowerCase()) {
            case "cosine":
                distType = Distance.Cosine;
                break;
            case "l2":
                distType = Distance.Euclid;
                break;
            case "ip":
                distType = Distance.Dot;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported metric type: " + params.getMetricType());
        }

        HnswConfigDiff hnswConfig =
                HnswConfigDiff.newBuilder()
                        .setM(params.getHnswM())
                        .setEfConstruct(params.getHnswEfConstruction())
                        .build();

        VectorParams vectorParams =
                VectorParams.newBuilder()
                        .setSize(params.getVectorDim())
                        .setDatatype(Datatype.Float32)
                        .setDistance(distType)
                        .setHnswConfig(hnswConfig)
                        .build();

        CreateCollection request =
                CreateCollection.newBuilder()
                        .setHnswConfig(hnswConfig)
                        .setVectorsConfig(
                                VectorsConfig.newBuilder().setParams(vectorParams).build())
                        .setCollectionName(params.getQdrantCollectionName())
                        .setShardingMethod(ShardingMethod.Custom)
                        .setShardNumber(1)
                        .setReplicationFactor(1)
                        .build();

        Future<CollectionOperationResponse> future = client.createCollectionAsync(request);
        try {
            future.get();
        } catch (Exception e) {
            LOG.error("Failed to create collection");
            throw e;
        }
        LOG.info("Created Qdrant collection {}", params.getQdrantCollectionName());

        // Create shards
        for (int i = 0; i < params.getParallelism(); i++) {
            CreateShardKey shardKey = CreateShardKey.newBuilder().setShardKey(shardKey(i)).build();
            CreateShardKeyRequest shardKeyRequest =
                    CreateShardKeyRequest.newBuilder()
                            .setCollectionName(params.getQdrantCollectionName())
                            .setRequest(shardKey)
                            .build();
            try {
                client.createShardKeyAsync(shardKeyRequest);
            } catch (Exception e) {
                LOG.error("Failed to create shard key");
                throw e;
            }

            LOG.info(
                    "Created shard key {} on Qdrant collection {}",
                    i,
                    params.getQdrantCollectionName());
        }

        // Wait for shards to become active
        LOG.info("Waiting {} s for shards to become active", WAIT_TIME.getSeconds());
        Thread.sleep(WAIT_TIME.toMillis());

        // Create ts index
        IntegerIndexParams tsIndexParams =
                IntegerIndexParams.newBuilder().setIsPrincipal(true).setRange(true).build();
        CreateFieldIndexCollection tsIndexRequest =
                CreateFieldIndexCollection.newBuilder()
                        .setCollectionName(params.getQdrantCollectionName())
                        .setFieldName(TS_FIELD)
                        .setFieldType(FieldType.FieldTypeInteger)
                        .setFieldIndexParams(
                                PayloadIndexParams.newBuilder()
                                        .setIntegerIndexParams(tsIndexParams)
                                        .build())
                        .setWait(true)
                        .build();
        ListenableFuture<UpdateResult> tsFuture =
                client.createPayloadIndexAsync(tsIndexRequest, TIMEOUT);
        try {
            tsFuture.get();
        } catch (ExecutionException e) {
            if (e.getMessage().contains(CONSENSUS_TIMEOUT_EXCEPTION)) {
                LOG.warn(
                        "Ignoring consensus timeout exception, waiting for {} s",
                        WAIT_TIME.getSeconds());
                Thread.sleep(WAIT_TIME.toMillis());
            } else {
                LOG.error("Failed to create ts index", e);
                throw e;
            }
        } catch (Exception e) {
            LOG.error("Failed to create ts index", e);
            throw e;
        }
    }

    private int getNumActiveShards(String collectionName) throws Exception {
        CollectionClusterInfoRequest request =
                CollectionClusterInfoRequest.newBuilder().setCollectionName(collectionName).build();
        ListenableFuture<CollectionClusterInfoResponse> future =
                client.grpcClient().collections().collectionClusterInfo(request);
        CollectionClusterInfoResponse response = future.get();
        int count = 0;
        for (LocalShardInfo shardInfo : response.getLocalShardsList()) {
            if (shardInfo.getState() == ReplicaState.Active) {
                count++;
            }
        }
        for (RemoteShardInfo shardInfo : response.getRemoteShardsList()) {
            if (shardInfo.getState() == ReplicaState.Active) {
                count++;
            }
        }
        return count;
    }

    public void insert(String collectionName, Collection<FloatVector> vectors, int shard) {
        Future<UpdateResult> future = client.upsertAsync(data(vectors, collectionName, shard));
        try {
            future.get();
        } catch (Exception e) {
            LOG.error("Failed to insert vectors", e);
        }
    }

    public void delete(String collectionName, Collection<Long> ids, int shard) {
        Future<UpdateResult> future = client.deleteAsync(deletions(ids, collectionName, shard));
        try {
            future.get();
        } catch (Exception e) {
            LOG.error("Failed to delete vectors", e);
        }
    }

    public List<SearchResult> search(
            String collectionName, List<PartitionedElement> queries, int shard, int k, int ef) {
        Future<List<BatchResult>> future =
                client.searchBatchAsync(collectionName, queries(queries, k, shard, ef), null);

        List<SearchResult> results = new ArrayList<>();
        try {
            // Translate BatchResult to SearchResult
            List<BatchResult> batchResults = future.get();

            for (int i = 0; i < queries.size(); i++) {
                List<ScoredPoint> scoredPoints = batchResults.get(i).getResultList();
                List<Long> ids =
                        scoredPoints.stream().map(p -> p.getId().getNum()).collect(toList());
                List<Float> distances =
                        scoredPoints.stream().map(ScoredPoint::getScore).collect(toList());
                results.add(
                        new SearchResult(
                                shard,
                                queries.get(i).getData().getId(),
                                ids,
                                distances,
                                1,
                                queries.get(i).getNumPartitionsSent(),
                                queries.get(i).getData().getEventTime()));
            }
        } catch (Exception e) {
            LOG.error("Failed to search vectors", e);
        }
        return results;
    }

    private static UpsertPoints data(
            Collection<FloatVector> vectors, String collectionName, int shard) {
        List<PointStruct> points =
                vectors.stream()
                        .map(
                                v ->
                                        PointStruct.newBuilder()
                                                .setId(id(v.getId()))
                                                .setVectors(vectors(v.vector()))
                                                .putAllPayload(
                                                        Collections.singletonMap(
                                                                TS_FIELD, value(v.getEventTime())))
                                                .build())
                        .collect(toList());

        return UpsertPoints.newBuilder()
                .addAllPoints(points)
                .setCollectionName(collectionName)
                .setShardKeySelector(shardKeySelector(shard))
                .setWait(true)
                .build();
    }

    private static DeletePoints deletions(Collection<Long> ids, String collectionName, int shard) {
        List<PointId> pointIds =
                ids.stream().map(id -> PointId.newBuilder().setNum(id).build()).collect(toList());

        return DeletePoints.newBuilder()
                .setPoints(
                        PointsSelector.newBuilder()
                                .setPoints(PointsIdsList.newBuilder().addAllIds(pointIds).build())
                                .build())
                .setCollectionName(collectionName)
                .setShardKeySelector(shardKeySelector(shard))
                .setWait(true)
                .build();
    }

    private static List<SearchPoints> queries(
            Collection<PartitionedElement> queries, int k, int shard, int ef) {
        long ttl = queries.stream().findFirst().map(d -> d.getData().getTTL()).orElse(-1L);
        SearchParams searchParams = SearchParams.newBuilder().setHnswEf(ef).build();

        Function<PartitionedElement, SearchPoints> converter;
        if (ttl == -1L) {
            converter =
                    q ->
                            SearchPoints.newBuilder()
                                    .addAllVector(q.getData().asVector().list())
                                    .setLimit(k)
                                    .setParams(searchParams)
                                    .setShardKeySelector(shardKeySelector(shard))
                                    .build();
        } else {
            converter =
                    q -> {
                        List<Float> vector = q.getData().asVector().list();
                        long ts = q.getData().getEventTime() - ttl;
                        return SearchPoints.newBuilder()
                                .addAllVector(vector)
                                .setLimit(k)
                                .setParams(searchParams)
                                .setShardKeySelector(shardKeySelector(shard))
                                .setFilter(
                                        Filter.newBuilder()
                                                .addMust(
                                                        range(
                                                                "ts",
                                                                Range.newBuilder()
                                                                        .setGte(ts)
                                                                        .build()))
                                                .build())
                                .build();
                    };
        }
        return queries.stream().map(converter).collect(toList());
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
