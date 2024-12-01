package cn.edu.zju.daily.partitioner;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.util.MathUtils;

/**
 * Flink 内部对 KeyedStream 使用 hash 分区，具体的分区器是 KeyGroupStreamPartitioner。分区的计算流程：
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
 * 等于分区 ID。getNodeIdMap 函数旨在寻找一组 key，这组 key 可以通过 murmurHash(key) 映射为各个分区 ID。
 */
public class PartitionToKeyMapper {

    private final Map<Integer, Integer> partitionToKey;
    private final Map<Integer, Integer> keyToPartition;

    public PartitionToKeyMapper(int numPartitions) {
        this.partitionToKey = getPartitionToKeyMap(numPartitions);
        this.keyToPartition = reverseMap(partitionToKey);
    }

    public static Map<Integer, Integer> getPartitionToKeyMap(int parallelism) {
        Map<Integer, Integer> nodeIdMap = new HashMap<>(); // physical node id -> key
        int count = 0;
        for (int key = 0; count < parallelism; key++) {
            int nodeId = MathUtils.murmurHash(key) % parallelism;
            if (!nodeIdMap.containsKey(nodeId)) {
                nodeIdMap.put(nodeId, key);
                count++;
            }
        }
        return nodeIdMap;
    }

    private static Map<Integer, Integer> reverseMap(Map<Integer, Integer> map) {
        Map<Integer, Integer> reversedMap = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            Integer value = reversedMap.put(entry.getValue(), entry.getKey());
            if (value != null) {
                throw new IllegalArgumentException("Duplicate value: " + entry.getValue());
            }
        }
        return reversedMap;
    }

    /**
     * Generates a partitioning key which will be sent to the required partition.
     *
     * @param partition the required partition
     * @return the partitioning key
     */
    public int getKey(int partition) {
        return partitionToKey.get(partition);
    }

    /**
     * Gets the partition that a given key is sent to.
     *
     * @param key the given key
     * @return the partition
     */
    public int getPartition(int key) {
        return keyToPartition.get(key);
    }
}
