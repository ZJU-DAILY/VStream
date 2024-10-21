package cn.edu.zju.daily.function.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.vector.FloatVector;
import java.util.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
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
public class SimpleUnaryPartitionFunction
        extends RichFlatMapFunction<FloatVector, PartitionedData> {

    private SimplePartitionFunction proxy;
    private final boolean isQuery;
    private final int numPartitions;

    public SimpleUnaryPartitionFunction(int numPartitions, boolean isQuery) {
        this.numPartitions = numPartitions;
        this.isQuery = isQuery;
    }

    @Override
    public void open(Configuration parameters) {
        this.proxy = new SimplePartitionFunction(numPartitions);
    }

    /**
     * @param vector data.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap(FloatVector vector, Collector<PartitionedData> collector) throws Exception {
        if (!isQuery) {
            this.proxy.flatMap1(vector, collector);
        } else {
            this.proxy.flatMap2(vector, collector);
        }
    }
}
