package cn.edu.zju.daily.data.vector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class FvecIteratorTest {

    @Test
    void test() throws IOException {
        FvecIterator it =
                new FvecIterator(
                        new RandomAccessFile(
                                "/mnt/sda1/work/vector-search/dataset/bigann/bigann_query.bvecs",
                                "r"),
                        2,
                        0,
                        6,
                        FvecIterator.InputType.B_VEC);

        while (it.hasNext()) {
            float[] vector = it.next();
            System.out.println(Arrays.toString(vector));
        }
    }
}
