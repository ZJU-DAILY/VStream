package cn.edu.zju.daily.function.partitioner;


import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.lsh.L2HashFamily;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.util.*;

/**
 * <p>使用 LSH 函数为向量数据和查询分配分区. 该函数实现了 CoFlatMapFunction 接口，因此需要同时处理数据和查询，这是为了确保数据和查询使用同样的
 * 分区策略。</p>
 *
 * <p>Flink 内部对 KeyedStream 使用 hash 分区，具体的分区器是 KeyGroupStreamPartitioner。分区的计算流程：
 * </p>
 *
 * <ol>
 *     <li>{@code KeyGroupRangeAssignment::computeKeyGroupForKeyHash}，使用哈希方式计算当前 key 所在的 keyGroup，keyGroup 数
 *     量由 maxParallelism 指定，公式为 {@code keyGroupId = MathUtils.murmurHash(key.hashCode()) % maxParallelism}</li>
 *     <li>{@code KeyGroupRangeAssignment::computeOperatorIndexForKeyGroup}，计算在当前操作符 parallelism（不大于
 *     maxParallelism）下，keyGroup 对应的分区 ID。keyGroup 与分区 ID 是多对一的关系。公式为
 *     {@code operatorIndex = keyGroupId * parallelism / maxParallelism}</li>
 * </ol>
 *
 * <p>因此，为了能够直接控制得到的分区 ID，我们需要确保 parallelism = maxParallelism，且
 * {@code murmurHash(key.hashCode())} 等于分区 ID。getNodeIdMap 函数旨在寻找一组 key，这组 key 可以通过 murmurHash(key) 映射为各
 * 个分区 ID。</p>
 */
public class LSHProximityPartitionFunction implements PartitionFunction {

    private final List<L2HashFamily> hashFamilies;
    private final int numPartitions;  // this should be set as parallelism and maxParallelism

    private final Map<Integer, Integer> nodeIdToKeyMap;
    private final Map<Integer, Integer> keyToNodeIdMap;
    private final int proximity;

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


    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Creates an LSH partitioner.
     *
     * @param random random number generator
     * @param dim    dimension of the vector
     * @param k1     number of LSH families, i.e. the max number of partitions an element belongs to
     * @param k2     number of hash functions per family
     * @param r      width of hash bucket (see paper)
     */
    public LSHProximityPartitionFunction(Random random, int dim, int k1, int k2, int numPartitions, float r, int proximity) {
        hashFamilies = new ArrayList<>();
        for (int i = 0; i < k1; i++) {
            hashFamilies.add(new L2HashFamily(dim, k2, r, new Random(random.nextLong())));
        }
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionFunction.getNodeIdMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
        this.proximity = proximity;
    }

    /**
     * Creates an LSH partitioner.
     *
     * @param dim dimension of the vector
     * @param k1  number of LSH families
     * @param k2  number of hash functions per family
     * @param r   width of hash bucket (see paper)
     */
    public LSHProximityPartitionFunction(int dim, int k1, int k2, int numPartitions, float r, int proximity) {
        hashFamilies = new ArrayList<>();
        for (int i = 0; i < k1; i++) {
            hashFamilies.add(new L2HashFamily(dim, k2, r));
        }
        this.numPartitions = numPartitions;
        this.nodeIdToKeyMap = PartitionFunction.getNodeIdMap(numPartitions);
        this.keyToNodeIdMap = getKeyToNodeIdMap(nodeIdToKeyMap);
        this.proximity = proximity;
    }

    Set<Integer> getNodeIds(FloatVector vector, int proximity) {
        List<Integer> nodeIds = new ArrayList<>();
        for (L2HashFamily hashFamily : this.hashFamilies) {
            int[] hashValues = hashFamily.hash(vector);
            ProximateHashValueGenerator gen = new ProximateHashValueGenerator(hashValues, proximity);
            while (gen.hasNext()) {
                int[] val = gen.next();
                int nodeId = L2HashFamily.getNodeId(val, this.numPartitions);
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
     * @param vector    data.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(FloatVector vector, Collector<PartitionedData> collector) throws Exception {
        Set<Integer> nodeIds = getNodeIds(vector, 0);
        for (int nodeId : nodeIds) {
            collector.collect(new PartitionedFloatVector(nodeIdToKey(nodeId), vector));
        }
    }

    /**
     * @param vector    query.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(FloatVector vector, Collector<PartitionedData> collector) throws Exception {
        Set<Integer> nodeIds = getNodeIds(vector, this.proximity);
        int numPartitionsSent = nodeIds.size();
        for (int nodeId : nodeIds) {
            collector.collect(new PartitionedQuery(nodeIdToKey(nodeId), numPartitionsSent, vector));
        }
    }

    static class ProximateHashValueGenerator implements Iterator<int[]> {

        int[] base;

        int[] offset;
        int proximity;
        boolean _hasNext;

        ProximateHashValueGenerator(int[] base, int proximity) {
            assert (proximity >= 0);
            if (proximity == 0) {
                this.base = base;
                this.offset = null;
                this._hasNext = true;
                this.proximity = 0;
            }
            this.base = base;
            this.offset = new int[base.length];
            for (int i = 0; i < base.length; i++) {
                this.offset[i] = -proximity;
            }
            this._hasNext = true;
            this.proximity = proximity;
        }

        @Override
        public boolean hasNext() {
            return _hasNext;
        }

        @Override
        public int[] next() {
            if (!_hasNext) {
                throw new NoSuchElementException();
            }
            if (this.proximity == 0) {
                _hasNext = false;
                return base;
            }
            int[] value = new int[base.length];
            for (int i = 0; i < base.length; i++) {
                value[i] = base[i] + offset[i];
            }

            for (int i = base.length - 1; ; i--) {
                if (i == -1) {
                    _hasNext = false;
                    break;
                }
                if (offset[i] <= proximity - 1) {
                    offset[i] += 1;
                    break;
                } else {
                    offset[i] = -proximity;
                }
            }

            return value;
        }
    }
}
