package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.MultiPartitionQuery;
import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.curve.SpaceFillingCurve;
import cn.edu.zju.daily.partitioner.lsh.LSHashSpaceFillingPartitioner;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class MultiplexLSHHilbertPartitionFunction
        implements FlatMapFunction<VectorData, PartitionedElement>,
                MapFunction<VectorData, MultiPartitionQuery> {

    private final List<LSHashSpaceFillingPartitioner> partitioners;

    private final Map<Integer, Integer> nodeIdToKeyMap;

    private final boolean isQuery;
    private final int numPartitions;

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
            SpaceFillingCurve.Builder curveBuilder,
            boolean isQuery) {
        //        if (numHashFunctions * numHilbertBits > 63) {
        //            LOG.warn("numHashFunctions * numHilbertBits > 63, cannot use small options for
        // hilbert curve.");
        //        }

        LOG.info(
                "Initializing LSHWithSpaceFillingPartitionFunction with dim: {}, numHashFamilies: {}, numHashFunctions: {}, "
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
                    new LSHashSpaceFillingPartitioner(
                            dim,
                            numHashFunctions,
                            hashWidth,
                            numHilbertBits,
                            updateInterval,
                            maxRetainedElements,
                            maxTTL,
                            numPartitions,
                            curveBuilder,
                            new Random(random.nextLong())));
        }

        nodeIdToKeyMap = PartitionToKeyMapper.getPartitionToKeyMap(numPartitions);
        LOG.info("Node to key: {}", nodeIdToKeyMap);
        this.isQuery = isQuery;
        this.numPartitions = numPartitions;
    }

    public int nodeIdToKey(int nodeId) {
        return nodeIdToKeyMap.get(nodeId);
    }

    @Override
    public void flatMap(VectorData value, Collector<PartitionedElement> out) throws Exception {
        if (isQuery) {
            flatMapQuery(value, out);
        } else {
            flatMapData(value, out);
        }
    }

    @Override
    public MultiPartitionQuery map(VectorData value) throws Exception {
        if (value.isDeletion()) {
            throw new RuntimeException("Deletion query is not supported.");
        }
        return mapQuery(value.asVector());
    }

    private void flatMapData(VectorData data, Collector<PartitionedElement> out) throws Exception {
        if (!data.hasValue()) {
            for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
                out.collect(new PartitionedData(nodeIdToKey(nodeId), data));
            }
        } else {
            Set<Integer> partitions = new HashSet<>();
            for (LSHashSpaceFillingPartitioner partitioner : partitioners) {
                partitions.add(partitioner.getDataPartition(data));
            }
            for (int partition : partitions) {
                //            LOG.info("Partition: {} -> {} (ts: {})", value.getId(), partition,
                // value.getEventTime());
                out.collect(new PartitionedData(nodeIdToKey(partition), data));
            }
        }
    }

    private void flatMapQuery(VectorData query, Collector<PartitionedElement> out)
            throws Exception {
        if (query.isDeletion()) {
            throw new RuntimeException("Deletion queries are not supported.");
        }
        FloatVector value = query.asVector();
        Set<Integer> partitions = new HashSet<>();
        for (LSHashSpaceFillingPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(value));
        }
        int numPartitionsSent = partitions.size();
        for (int partition : partitions) {
            //            LOG.info("Partition: {} -> {} (ts: {})", value.getId(), partition,
            // value.getEventTime());
            out.collect(new PartitionedQuery(nodeIdToKey(partition), numPartitionsSent, value));
        }
    }

    private MultiPartitionQuery mapQuery(VectorData query) {
        if (query.isDeletion()) {
            throw new RuntimeException("Deletion queries are not supported.");
        }
        Set<Integer> partitions = new HashSet<>();
        for (LSHashSpaceFillingPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(query.asVector()));
        }
        int[] partitionKeys = new int[partitions.size()];
        int i = 0;
        for (int partition : partitions) {
            partitionKeys[i++] = nodeIdToKey(partition);
        }
        return new MultiPartitionQuery(partitionKeys, query.asVector());
    }
}
