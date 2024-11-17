package cn.edu.zju.daily.partitioner.sax;

import cn.edu.zju.daily.data.vector.FloatVector;
import org.junit.jupiter.api.Test;

public class SAXTest {

    @Test
    void test() throws Exception {
        SAX sax = new SAX(16, 1, 0d);
        for (int i = 0; i < 100; i++) {
            FloatVector vector = FloatVector.getRandom(0, 128);
            int encode = sax.encode(vector.vector());
            System.out.println(encode);
        }
    }
}
