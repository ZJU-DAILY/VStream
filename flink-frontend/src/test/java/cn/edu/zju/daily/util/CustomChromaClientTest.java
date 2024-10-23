package cn.edu.zju.daily.util;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.amikos.chromadb.handler.ApiException;

import static cn.edu.zju.daily.util.ChromaUtil.readAddresses;
import static java.util.stream.Collectors.toList;

public class CustomChromaClientTest {

    private static final String addressPath =
            "/home/auroflow/code/vector-search/VStream/flink-frontend/src/test/resources/chroma_addresses.txt";
    private static final String datasetPath =
            "/home/auroflow/code/vector-search/data/sift/sift_base.fvecs";
    private static final int batchSize = 1000;
    private static final int rounds = 10;
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

        private final CustomChromaClient client;
        private final CustomChromaCollection collection;
        private final List<FloatVector> vectors;
        private final String collectionName;

        InsertRunnable(String chromaAddress, String collectionName, List<FloatVector> vectors)
                throws ApiException {
            this.client = new CustomChromaClient("http://" + chromaAddress);
            this.collection =
                    client.createCollection(
                            collectionName,
                            new HashMap<>(),
                            true,
                            CustomEmptyChromaEmbeddingFunction.getInstance());
            this.vectors = vectors;
            this.collectionName = collectionName;
        }

        @Override
        public void run() {
            int total = 0;
            for (int r = 0; r < rounds; r++) {
                for (int start = 0; start + batchSize < vectors.size(); start += batchSize) {
                    int end = Math.min(start + batchSize, vectors.size());
                    List<FloatVector> batch = vectors.subList(start, end);
                    List<List<Float>> embeddings =
                            batch.stream().map(FloatVector::list).collect(toList());
                    List<String> ids =
                            batch.stream()
                                    .map(FloatVector::getId)
                                    .map(Object::toString)
                                    .collect(toList());
                    try {
                        collection.add(embeddings, null, ids, ids);
                    } catch (ApiException e) {
                        e.printStackTrace();
                    }
                    total += batchSize;
                    System.out.println(collectionName + ": inserted " + total + " vectors");
                }
            }
        }
    }

    @Test
    void testConcurrentInsert() throws IOException, ApiException, InterruptedException {
        List<FloatVector> vectors =
                Collections.unmodifiableList(FloatVectorIterator.fromFile(datasetPath).toList());
        int index = 0;
        for (String chromaAddress : chromaAddresses) {
            es.submit(new InsertRunnable(chromaAddress, "sift_" + index++, vectors));
        }
        es.awaitTermination(8, TimeUnit.HOURS);
    }
}
