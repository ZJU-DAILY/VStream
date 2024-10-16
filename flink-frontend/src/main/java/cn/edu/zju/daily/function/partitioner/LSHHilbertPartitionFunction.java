package cn.edu.zju.daily.function.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.lsh.L2HilbertPartitioner;
import java.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 LSH 函数和 Hilbert 曲线为向量数据和查询分配分区. 该函数扩展 RichCoFlatMapFunction，需要同时处理输入向量和查询向量。
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
public class LSHHilbertPartitionFunction
        extends RichCoFlatMapFunction<FloatVector, FloatVector, PartitionedData>
        implements PartitionFunction {

    private static final Logger LOG = LoggerFactory.getLogger(LSHHilbertPartitionFunction.class);

    private final List<L2HilbertPartitioner> partitioners;

    private final Map<Integer, Integer> nodeIdToKeyMap;
    private ValueState<Long> count = null;

    private int[] counter;
    private int totalCounter = 0;
    private static final int COUNTER_REPORT_INTERVAL = 480000;

    private final int numPartitions;

    public LSHHilbertPartitionFunction(
            Random random,
            int dim,
            int numHashFamilies,
            int numHashFunctions,
            float hashWidth,
            int numHilbertBits,
            long updateInterval,
            int maxRetainedElements,
            long maxTTL,
            int numPartitions) {
        this(
                random,
                dim,
                numHashFamilies,
                numHashFunctions,
                hashWidth,
                numHilbertBits,
                updateInterval,
                maxRetainedElements,
                maxTTL,
                Collections.singletonList(0L),
                Collections.singletonList(0L),
                numPartitions);
    }

    /** Insert interval in nanoseconds, observed by this partitioner. */
    private final List<Long> observedInsertIntervals;

    private final List<Long> insertThresholds;

    private int currentObservedInsertIntervalIndex = 0;

    private long observedTsNano;

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
     * @param observedInsertIntervals 希望分区器观测到的插入向量时间戳间隔 (ns)
     * @param numPartitions 分区数量
     */
    public LSHHilbertPartitionFunction(
            Random random,
            int dim,
            int numHashFamilies,
            int numHashFunctions,
            float hashWidth,
            int numHilbertBits,
            long updateInterval,
            int maxRetainedElements,
            long maxTTL,
            List<Long> observedInsertIntervals,
            List<Long> insertThresholds,
            int numPartitions) {
        //        if (numHashFunctions * numHilbertBits > 63) {
        //            LOG.warn("numHashFunctions * numHilbertBits > 63, cannot use small options for
        // hilbert curve.");
        //        }

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
        this.observedInsertIntervals = observedInsertIntervals;
        this.insertThresholds = insertThresholds;
        this.numPartitions = numPartitions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        observedTsNano = 0;
        LOG.info(
                "LSHHilbertPartitionFunction initialized with insert interval {} ns.",
                observedInsertIntervals.get(0));
        counter = new int[numPartitions];
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        count.update(0L);
    }

    /**
     * Processes a data tuple.
     *
     * @param value The stream element
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void flatMap1(FloatVector value, Collector<PartitionedData> out) throws Exception {
        // 校准 ts
        value.setEventTime(observedTsNano / 1000000L);

        // 更新 observedTs
        count.update(count.value() + 1);
        while (currentObservedInsertIntervalIndex < observedInsertIntervals.size() - 1
                && count.value() >= insertThresholds.get(currentObservedInsertIntervalIndex + 1)) {
            currentObservedInsertIntervalIndex++;
        }
        long observedInsertInterval =
                observedInsertIntervals.get(currentObservedInsertIntervalIndex);
        if (observedInsertInterval > 0) {
            observedTsNano += observedInsertInterval;
        }

        Set<Integer> partitions = new HashSet<>();
        for (L2HilbertPartitioner partitioner : partitioners) {
            partitions.add(partitioner.getDataPartition(value));
        }
        for (int partition : partitions) {
            counter[partition]++;
            out.collect(new PartitionedFloatVector(nodeIdToKeyMap.get(partition), value));
        }

        // Report partition distribution periodically
        totalCounter++;
        if (totalCounter == COUNTER_REPORT_INTERVAL) {
            LOG.info("Partitions: {}", Arrays.toString(counter));
            IntSummaryStatistics stats = Arrays.stream(counter).summaryStatistics();
            LOG.info("Balance: {}", (stats.getMax() - stats.getAverage()) / stats.getAverage());
            totalCounter = 0;
            Arrays.fill(counter, 0);
        }
    }

    /**
     * Processes a query tuple.
     *
     * @param value The stream element
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void flatMap2(FloatVector value, Collector<PartitionedData> out) throws Exception {
        // 校准 ts
        value.setEventTime(observedTsNano / 1000000L);

        Set<Integer> partitions = new HashSet<>();
        for (L2HilbertPartitioner partitioner : partitioners) {
            partitions.addAll(partitioner.getQueryPartition(value));
        }
        int numPartitionsSent = partitions.size();
        for (int partition : partitions) {
            out.collect(
                    new PartitionedQuery(nodeIdToKeyMap.get(partition), numPartitionsSent, value));
        }
    }
}
