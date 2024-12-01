package cn.edu.zju.daily.partitioner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class OdysseyPartitionFunctionTest {

    @Test
    void testCreateReplicationGroups() {
        List<List<Integer>> groups = OdysseyPartitionFunction.createReplicationGroups(16, 8);
        System.out.println(groups);
        assertEquals(2, groups.size());
        assertEquals(8, groups.get(0).size());
    }
}
