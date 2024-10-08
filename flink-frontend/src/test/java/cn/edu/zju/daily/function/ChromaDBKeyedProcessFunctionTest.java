package cn.edu.zju.daily.function;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.zju.daily.util.CustomChromaClient;
import cn.edu.zju.daily.util.CustomChromaCollection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import tech.amikos.chromadb.DefaultEmbeddingFunction;

class ChromaDBKeyedProcessFunctionTest {

    @Test
    void testConnection() {
        CustomChromaClient client = new CustomChromaClient("http://localhost:8001");
        CustomChromaCollection collection = null;
        try {
            collection =
                    client.createCollection(
                            "test_collection",
                            new HashMap<>(),
                            true,
                            new DefaultEmbeddingFunction());
        } catch (Exception e) {
            System.err.println("Failed: " + e.toString());
        }
        List<List<Float>> vectors = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                vector.add((float) (j + i * 10));
            }
            vectors.add(vector);
            ids.add(Integer.toString(i));
        }
        try {
            Object add = collection.add(vectors, null, ids, ids);
            System.out.println(add);
        } catch (Exception e) {
            System.err.println("Failed: " + e.getMessage());
        }
    }

    @Test
    void testReadAddresses() throws IOException {
        List<String> addresses =
                ChromaDBKeyedProcessFunction.readAddresses(
                        "src/test/resources/test_chroma_addresses.txt");
        List<String> actualLines = new ArrayList<>();
        actualLines.add("localhost:8000");
        actualLines.add("localhost:8001");
        assertLinesMatch(addresses, actualLines);
    }
}
