package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.lsh.L2HashFamily;
import java.util.*;
import org.apache.flink.util.Collector;

/**
 * 使用 LSH 函数为向量数据和查询分配分区. 该函数实现了 CoFlatMapFunction 接口，因此需要同时处理数据和查询，这是为了确保数据和查询使用同样的
 * 分区策略。为了确保正确性，对于查询，再额外传入1/3的随机分区。
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
public class LSHAndRandomPartitionFunction implements PartitionFunction {

    private final List<L2HashFamily> hashFamilies;
    private final int numPartitions; // this should be set as parallelism and maxParallelism

    private final Map<Integer, Integer> nodeIdToKeyMap;
    private final Map<Integer, Integer> keyToNodeIdMap;
    private final Random random = new Random(23456789L);

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

    /**
     * Creates an LSH partitioner.
     *
     * @param hashFamilies list of LSH families, i.e. the max number of partitions an element
     *     belongs to
     * @param numPartitions total number of partitions. Should use {@code
     *     operator.setParallelism(numPartitions).setMaxParallelism(numPartitions)} on next operator
     */
    public LSHAndRandomPartitionFunction(List<L2HashFamily> hashFamilies, int numPartitions) {
        this.hashFamilies = hashFamilies;
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionToKeyMapper.getPartitionToKeyMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Creates an LSH partitioner.
     *
     * @param random random number generator
     * @param dim dimension of the vector
     * @param k1 number of LSH families, i.e. the max number of partitions an element belongs to
     * @param k2 number of hash functions per family
     * @param r width of hash bucket (see paper)
     */
    public LSHAndRandomPartitionFunction(
            Random random, int dim, int k1, int k2, int numPartitions, float r) {
        hashFamilies = new ArrayList<>();
        for (int i = 0; i < k1; i++) {
            hashFamilies.add(new L2HashFamily(dim, k2, r, new Random(random.nextLong())));
        }
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionToKeyMapper.getPartitionToKeyMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
    }

    /**
     * Creates an LSH partitioner.
     *
     * @param dim dimension of the vector
     * @param k1 number of LSH families
     * @param k2 number of hash functions per family
     * @param r width of hash bucket (see paper)
     */
    public LSHAndRandomPartitionFunction(int dim, int k1, int k2, int numPartitions, float r) {
        hashFamilies = new ArrayList<>();
        for (int i = 0; i < k1; i++) {
            hashFamilies.add(new L2HashFamily(dim, k2, r));
        }
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionToKeyMapper.getPartitionToKeyMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
    }

    private Set<Integer> getNodeIds(VectorData data) {
        List<Integer> nodeIds = new ArrayList<>();
        if (!data.hasValue()) {
            for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
                nodeIds.add(nodeId);
            }
        } else {
            for (L2HashFamily hashFamily : this.hashFamilies) {
                int[] hashValues = hashFamily.hash(data);
                int nodeId = L2HashFamily.getNodeId(hashValues, this.numPartitions);
                nodeIds.add(nodeId);
            }
        }
        return new HashSet<>(nodeIds);
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
     * @param data data.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(VectorData data, Collector<PartitionedElement> collector)
            throws Exception {
        Set<Integer> nodeIds = getNodeIds(data);
        for (int nodeId : nodeIds) {
            collector.collect(new PartitionedData(nodeIdToKey(nodeId), data));
        }
    }

    /**
     * @param data query.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(VectorData data, Collector<PartitionedElement> collector)
            throws Exception {
        if (data.isDeletion()) {
            throw new RuntimeException("Deletion query is not supported.");
        }
        FloatVector value = data.asVector();
        Set<Integer> nodeIds = getNodeIds(value);
        // Add 1/3 of the random nodes
        for (int i = 0; i < this.numPartitions / 3; i++) {
            nodeIds.add(random.nextInt(numPartitions));
        }
        int numPartitionsSent = nodeIds.size();
        for (int nodeId : nodeIds) {
            collector.collect(new PartitionedQuery(nodeIdToKey(nodeId), numPartitionsSent, value));
        }
    }
}
