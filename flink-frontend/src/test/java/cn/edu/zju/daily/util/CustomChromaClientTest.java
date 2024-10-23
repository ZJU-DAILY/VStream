package cn.edu.zju.daily.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.amikos.chromadb.handler.ApiException;

public class CustomChromaClientTest {

    private static final String addressPath = "src/test/resources/chroma-addresses.txt";
    private static final String datasetPath = "/home/auroflow/c";
    private static final int batchSize = 1000;
    private static final int rounds = 10;
    private List<String> chromaAddresses;

    @BeforeEach
    void setUp() throws IOException {
        chromaAddresses = ChromaUtil.readAddresses(addressPath);
    }

    static class InsertRunnable implements Runnable {

        private final CustomChromaClient client;
        private final CustomChromaCollection collection;
        private final List<FloatVector> vectors;

        InsertRunnable(String chromaAddress, String collectionName, List<FloatVector> vectors)
                throws ApiException {
            this.client = new CustomChromaClient(chromaAddress);
            this.collection =
                    client.createCollection(
                            collectionName,
                            new HashMap<>(),
                            true,
                            CustomEmptyChromaEmbeddingFunction.getInstance());
            this.vectors = vectors;
        }

        @Override
        public void run() {

        }
    }

    @Test
    void testConcurrentInsert() throws IOException {
        List<FloatVector> vectors =
                Collections.unmodifiableList(FloatVectorIterator.fromFile(datasetPath).toList());
    }
}
