package cn.edu.zju.daily.util.chromadb;

import static cn.edu.zju.daily.util.chromadb.ChromaUtil.readAddresses;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import com.google.gson.internal.LinkedTreeMap;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.amikos.chromadb.EFException;
import tech.amikos.chromadb.embeddings.DefaultEmbeddingFunction;
import tech.amikos.chromadb.handler.ApiException;

public class ChromaClientTest {

    private static final String addressPath =
            "/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/chroma_addresses.txt";
    private static final String datasetPath =
            "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs";
    private static final int batchSize = 1000;
    private static final int rounds = 6;
    private List<String> chromaAddresses;

    private ExecutorService es;

    @BeforeEach
    void setUp() throws IOException {
        List<String> lines = readAddresses(addressPath);
        chromaAddresses = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split(":");
            String host = parts[0];
            int low = Integer.parseInt(parts[1]);
            int high = Integer.parseInt(parts[2]);
            for (int i = low; i <= high; i++) {
                chromaAddresses.add(host + ":" + i);
            }
        }
        es = Executors.newFixedThreadPool(chromaAddresses.size());
    }

    static class InsertRunnable implements Runnable {

        private final ChromaClient client;
        private final ChromaCollection collection;
        private final List<FloatVector> vectors;
        private final String collectionName;

        InsertRunnable(String chromaAddress, String collectionName, List<FloatVector> vectors)
                throws ApiException, EFException {
            this.client = new ChromaClient("http://" + chromaAddress);
            this.client.setTimeout(60000); // 1 min
            Map<String, Object> metadata = new LinkedTreeMap<>();
            metadata.put("hnsw:batch_size", 10000);
            metadata.put("hnsw:sync_threshold", 1000000);
            metadata.put("hnsw:M", 16);
            metadata.put("hnsw:search_ef", 16);
            metadata.put("hnsw:construction_ef", 128);
            metadata.put("hnsw:num_threads", 16);
            metadata.put("hnsw:space", "l2");
            metadata.put("hnsw:resize_factor", 1.2);

            //            try {
            //                client.deleteCollection(collectionName);
            //            } catch (Exception ignored) {
            //            }
            //            this.collection =
            //                    client.createCollection(
            //                            collectionName, metadata, true, new
            // DefaultEmbeddingFunction());
            this.collection = client.getCollection(collectionName, new DefaultEmbeddingFunction());
            this.vectors = vectors;
            this.collectionName = collectionName;
        }

        @Override
        public void run() {
            int total = 999000;
            long begin = System.currentTimeMillis();
            for (int r = 0; r < rounds; r++) {
                for (int start = 0; start + batchSize < vectors.size(); start += batchSize) {
                    int end = Math.min(start + batchSize, vectors.size());
                    List<FloatVector> batch = vectors.subList(start, end);
                    long idOffset = total - batch.get(0).getId();
                    List<float[]> embeddings =
                            batch.stream().map(FloatVector::getValue).collect(toList());
                    List<Map<String, String>> metadatas =
                            batch.stream().map(FloatVector::getMetadata).collect(toList());
                    List<String> ids =
                            batch.stream()
                                    .map(v -> v.getId() + idOffset)
                                    .map(Object::toString)
                                    .collect(toList());
                    try {
                        collection.add(embeddings, null, null, ids);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    total += batchSize;
                    if (collectionName.endsWith("_0") && total % 10000 == 0) {
                        long now = System.currentTimeMillis();
                        long millis = now - begin;
                        System.out.println(
                                millis
                                        + "ms: "
                                        + collectionName
                                        + " inserted "
                                        + total
                                        + " vectors");
                        begin = now;
                    }
                }
            }
        }
    }

    @Test
    void testConcurrentInsert()
            throws IOException, ApiException, InterruptedException, EFException {
        List<FloatVector> vectors =
                Collections.unmodifiableList(FloatVectorIterator.fromFile(datasetPath).toList());
        int index = 0;
        for (String chromaAddress : chromaAddresses) {
            es.submit(new InsertRunnable(chromaAddress, "testcol_" + index++, vectors));
        }
        es.awaitTermination(8, TimeUnit.HOURS);
    }

    @Test
    void testCreateCollection() throws ApiException, EFException {
        ChromaClient client = new ChromaClient("http://10.214.151.193:8000");
        try {
            client.deleteCollection("test_create_collection");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> metadata = new LinkedTreeMap<>();
        metadata.put("hnsw:batch_size", 10000);
        metadata.put("hnsw:sync_threshold", 1000000);
        metadata.put("hnsw:M", 16);
        metadata.put("hnsw:search_ef", 16);
        metadata.put("hnsw:construction_ef", 128);
        metadata.put("hnsw:num_threads", 16);
        metadata.put("hnsw:space", "l2");
        metadata.put("hnsw:resize_factor", 1.2);

        client.createCollection(
                "test_create_collection", metadata, true, new DefaultEmbeddingFunction());
    }
}
