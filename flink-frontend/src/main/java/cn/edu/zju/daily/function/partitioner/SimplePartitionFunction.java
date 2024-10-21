package cn.edu.zju.daily.function.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import java.util.*;
import org.apache.flink.util.Collector;

/**
 * 简单哈希分区. 该函数实现了 CoFlatMapFunction 接口，因此需要同时处理数据和查询，这是为了确保数据和查询使用同样的分区策略。
 *
 * <p>Flink 内部对 KeyedStream 使用 hash 分区，具体的分区器是 KeyGroupStreamPartitioner。分区的计算流程：
 *
 * <ol>
 *   <li>{@code KeyGroupRangeAssignment::computeKeyGroupForKeyHash}，使用哈希方式计算当前 key 所在的
 *       keyGroup，keyGroup 数 量由 maxParallelism 指定，公式为 {@code keyGroupId =
 *       MathUtils.murmurHash(key.hashCode()) % maxParallelism}
 *   <li>{@code KeyGroupRangeAssignment::computeOperatorIndexForKeyGroup}，计算在当前操作符 parallelism（不大于
 *       maxParallelism）下，keyGroup 对应的分区 ID。keyGroup 与分区 ID 是多对一的关系。公式为 {@code operatorIndex =
 *       keyGroupId * parallelism / maxParallelism}
 * </ol>
 *
 * <p>因此，为了能够直接控制得到的分区 ID，我们需要确保 parallelism = maxParallelism，且 {@code murmurHash(key.hashCode())}
 * 等于分区 ID。getNodeIdMap 函数旨在寻找一组 key，这组 key 可以通过 murmurHash(key) 映射为各 个分区 ID。
 */
public class SimplePartitionFunction implements PartitionFunction {

    private final int numPartitions; // this should be set as parallelism and maxParallelism

    private final Map<Integer, Integer> nodeIdToKeyMap;
    private final Map<Integer, Integer> keyToNodeIdMap;

    private static Map<Integer, Integer> getKeyToNodeIdMap(Map<Integer, Integer> nodeIdToKeyMap) {
        Map<Integer, Integer> keyToNodeIdMap = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : nodeIdToKeyMap.entrySet()) {
            keyToNodeIdMap.put(entry.getValue(), entry.getKey());
        }
        return keyToNodeIdMap;
    }

    public int nodeIdToKey(int nodeId) {
        return nodeIdToKeyMap.get(nodeId);
    }

    public int keyToNodeId(int key) {
        return keyToNodeIdMap.get(key);
    }

    /** Creates an LSH partitioner. */
    public SimplePartitionFunction(int numPartitions) {
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionFunction.getNodeIdMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    private int getNodeId(FloatVector vector) {
        int hash = Long.hashCode(vector.getId());
        return hash % numPartitions;
    }

    //    private Set<Integer> getNodeIds(FloatVector vector) {
    //        // random
    //        List<Integer> nodeIds = new ArrayList<>();
    //        for (int i = 0; i < this.hashFamilies.size(); i++) {
    //            int nodeId = (int) (Math.random() * this.numPartitions);
    //            nodeIds.add(nodeId);
    //        }
    //        return new HashSet<>(nodeIds);
    //    }

    /**
     * @param vector data.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(FloatVector vector, Collector<PartitionedData> collector)
            throws Exception {
        int nodeId = getNodeId(vector);
        collector.collect(new PartitionedFloatVector(nodeIdToKey(nodeId), vector));
    }

    /**
     * @param vector query.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(FloatVector vector, Collector<PartitionedData> collector)
            throws Exception {
        for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
            collector.collect(new PartitionedQuery(nodeIdToKey(nodeId), numPartitions, vector));
        }
    }
}
