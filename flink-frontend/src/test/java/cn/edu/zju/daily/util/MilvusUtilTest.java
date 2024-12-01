package cn.edu.zju.daily.util;

import cn.edu.zju.daily.function.milvus.MilvusUtil;
import io.milvus.grpc.IndexDescription;
import org.junit.jupiter.api.Test;

public class MilvusUtilTest {

    @Test
    void test() {
        MilvusUtil util = new MilvusUtil();
        util.connect("10.214.242.182", 19530);
        IndexDescription indexDescription =
                util.getIndexDescription("vector_collection", "embedding");
        System.out.println(indexDescription);
    }
}
