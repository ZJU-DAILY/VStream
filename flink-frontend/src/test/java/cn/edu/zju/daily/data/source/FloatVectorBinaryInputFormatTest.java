package cn.edu.zju.daily.data.source;

import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormat;
import cn.edu.zju.daily.data.source.rate.RateControllerBuilder;
import cn.edu.zju.daily.data.source.rate.StagedRateControllerBuilder;
import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.local.LocalDataInputStream;
import org.junit.jupiter.api.Test;

public class FloatVectorBinaryInputFormatTest {

    private static final long MAX_TTL = 1936;
    private static final int startId = 9995;
    private static final int dataSize = 10000;
    private static final int dim = 128;
    private static final int numLoops = 2;

    @Test
    void test() throws IOException {
        RateControllerBuilder.RateController rateController =
                new StagedRateControllerBuilder(
                                Arrays.asList(0L, 5L),
                                Arrays.asList(
                                        Duration.ofMillis(500).toNanos(),
                                        Duration.ofMillis(2000).toNanos()))
                        .build();
        FloatVectorBinaryInputFormat format =
                new FloatVectorBinaryInputFormat(
                        "test",
                        MAX_TTL,
                        FloatVectorBinaryInputFormat.FileType.B_VEC,
                        startId,
                        dataSize,
                        dim,
                        numLoops,
                        null);

        Configuration conf = new Configuration();
        conf.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(1024576));

        FloatVectorBinaryInputFormat.Reader reader =
                format.createReader(
                        conf,
                        new LocalDataInputStream(
                                new File(
                                        "/mnt/sda1/work/vector-search/dataset/bigann/bigann_query.bvecs")));

        while (true) {
            FloatVector vector = reader.read();
            if (vector == null) {
                break;
            }
            System.out.println(vector);
        }
    }
}
