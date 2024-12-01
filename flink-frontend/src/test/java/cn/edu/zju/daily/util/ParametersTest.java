package cn.edu.zju.daily.util;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ParametersTest {

    String path = "/home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml";

    @Test
    void test() throws IOException {
        Parameters params = Parameters.load(path, false);

        // Read the file, check get_hnsw_k
        List<String> lines = Files.readAllLines(Paths.get(path));
        List<String> hnswKLine =
                lines.stream().filter(line -> line.startsWith("hnswK: ")).collect(toList());

        if (hnswKLine.size() != 1) {
            throw new RuntimeException("hnswK line not found");
        }

        String hnswKStr = hnswKLine.get(0).split(":")[1].trim();
        assertThat(params.getK(), is(Integer.parseInt(hnswKStr)));
    }
}
