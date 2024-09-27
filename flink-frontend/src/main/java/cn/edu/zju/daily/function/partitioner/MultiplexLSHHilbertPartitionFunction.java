package cn.edu.zju.daily.function.partitioner;

import cn.edu.zju.daily.data.MultiPartitionQuery;
import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.lsh.L2HilbertPartitioner;
import java.util.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiplexLSHHilbertPartitionFunction
        implements FlatMapFunction<FloatVector, PartitionedData>,
                MapFunction<FloatVector, MultiPartitionQuery> {

    private static final Logger LOG = LoggerFactory.getLogger(LSHHilbertPartitionFunction.class);

    private final List<L2HilbertPartitioner> partitioners;

    private final Map<Integer, Integer> nodeIdToKeyMap;

    private final boolean isQuery;

    /**
     * Constructor.
     *
     * @param dim 向量维度
     * @param numHashFamilies hash family 数量
     * @param numHashFunctions 每个 hash family 中的 hash function 数量
     * @param hashWidth hash bucket 宽度（论文中的 r）
     * @param numHilbertBits Hilbert 曲线的位数
     * @param updateInterval 更新分区方案的频率 (ms)
     * @param maxRetainedElements 统计历史信息时保留的最大元素数量
     * @param maxTTL 支持的最大查询TTL
     * @param numPartitions 分区数量
     */
    public MultiplexLSHHilbertPartitionFunction(
            Random random,
            int dim,
            int numHashFamilies,
            int numHashFunctions,
            float hashWidth,
            int numHilbertBits,
            long updateInterval,
            int maxRetainedElements,
            long maxTTL,
            int numPartitions,
            boolean isQuery) {
        //        if (numHashFunctions * numHilbertBits > 63) {
        //            LOG.warn("numHashFunctions * numHilbertBits > 63, cannot use small options for
        // hilbert curve.");
        //        }

        LOG.info(
                "Initializing LSHHilbertPartitionFunction with dim: {}, numHashFamilies: {}, numHashFunctions: {}, "
                        + "hashWidth: {}, numHilbertBits: {}, updateInterval: {}, maxRetainedElements: {}, maxTTL: {}, numPartitions: {}",
                dim,
                numHashFamilies,
                numHashFunctions,
                hashWidth,
                numHilbertBits,
                updateInterval,
                maxRetainedElements,
                maxTTL,
                numPartitions);

        if (numHashFamilies < 1) {
            throw new IllegalArgumentException("numHashFamilies should be greater than 0.");
        }

        partitioners = new ArrayList<>(numHashFamilies);
        for (int i = 0; i < numHashFamilies; i++) {
            partitioners.add(
                    new L2HilbertPartitioner(
                            dim,
                            numHashFunctions,
                            hashWidth,
                            numHilbertBits,
                            updateInterval,
                            maxRetainedElements,
                            maxTTL,
                            numPartitions,
                            new Random(random.nextLong())));
        }

        nodeIdToKeyMap = PartitionFunction.getNodeIdMap(numPartitions);
        LOG.info("Node to key: {}", nodeIdToKeyMap);
        this.isQuery = isQuery;
    }

    public int nodeIdToKey(int nodeId) {
        return nodeIdToKeyMap.get(nodeId);
    }

    @Override
    public void flatMap(FloatVector value, Collector<PartitionedData> out) throws Exception {
        if (isQuery) {
            flatMapQuery(value, out);
        } else {
            flatMapData(value, out);
        }
    }

    @Override
    public MultiPartitionQuery map(FloatVector value) throws Exception {
        return mapQuery(value);
    }

    private void flatMapData(FloatVector value, Collector<PartitionedData> out) throws Exception {
        Set<Integer> partitions = new HashSet<>();
        for (L2HilbertPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(value));
        }
        for (int partition : partitions) {
            //            LOG.info("Partition: {} -> {} (ts: {})", value.getId(), partition,
            // value.getEventTime());
            out.collect(new PartitionedFloatVector(nodeIdToKey(partition), value));
        }
    }

    private void flatMapQuery(FloatVector value, Collector<PartitionedData> out) throws Exception {
        Set<Integer> partitions = new HashSet<>();
        for (L2HilbertPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(value));
        }
        int numPartitionsSent = partitions.size();
        for (int partition : partitions) {
            //            LOG.info("Partition: {} -> {} (ts: {})", value.getId(), partition,
            // value.getEventTime());
            out.collect(new PartitionedQuery(nodeIdToKey(partition), numPartitionsSent, value));
        }
    }

    private MultiPartitionQuery mapQuery(FloatVector value) {
        Set<Integer> partitions = new HashSet<>();
        for (L2HilbertPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(value));
        }
        int[] partitionKeys = new int[partitions.size()];
        int i = 0;
        for (int partition : partitions) {
            partitionKeys[i++] = nodeIdToKey(partition);
        }
        return new MultiPartitionQuery(partitionKeys, value);
    }
}
