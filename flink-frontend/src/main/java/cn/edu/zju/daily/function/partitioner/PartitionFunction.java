package cn.edu.zju.daily.function.partitioner;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.function.partitioner.curve.HilbertCurve;
import cn.edu.zju.daily.util.Parameters;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

public interface PartitionFunction
        extends CoFlatMapFunction<VectorData, VectorData, PartitionedElement>,
                FlatMapFunction<PartitionedElement, PartitionedElement> {

    static Map<Integer, Integer> getNodeIdMap(int parallelism) {
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

    /**
     * Augments a data vector or a deletion marker with a partition ID.
     *
     * @param value The data vector or deletion marker
     * @param out The collector to emit resulting elements to
     */
    @Override
    void flatMap1(VectorData value, Collector<PartitionedElement> out) throws Exception;

    /**
     * Augments a query vector with a partition ID.
     *
     * @param value The query vector
     * @param out The collector to emit resulting elements to
     */
    @Override
    void flatMap2(VectorData value, Collector<PartitionedElement> out) throws Exception;

    /**
     * Augments a PartitionedData with a partition ID.
     *
     * @param value The input value.
     * @param out The collector for returning result values.
     */
    @Override
    default void flatMap(PartitionedElement value, Collector<PartitionedElement> out)
            throws Exception {
        if (value.getDataType().equals(PartitionedElement.DataType.INSERT_OR_DELETE)) {
            flatMap1(value.getData(), out);
        } else if (value.getDataType().equals(PartitionedElement.DataType.QUERY)) {
            flatMap2(value.getData(), out);
        } else {
            throw new RuntimeException("Unsupported data type: " + value.getDataType());
        }
    }

    static FlatMapFunction<VectorData, PartitionedElement> getUnaryPartitionFunction(
            Parameters params, Random random, boolean isQuery) {
        switch (params.getPartitioner()) {
            case "lsh+hilbert":
                return new MultiplexLSHHilbertPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getLshNumFamilies(),
                        params.getLshNumHashes(),
                        params.getLshBucketWidth(),
                        params.getLshNumHilbertBits(),
                        params.getLshPartitionUpdateInterval(),
                        params.getLshHilbertMaxRetainedElements(),
                        params.getMaxTTL(),
                        params.getParallelism(),
                        new HilbertCurve.Builder(),
                        isQuery);
            case "simple":
                return new SimpleUnaryPartitionFunction(params.getParallelism(), isQuery);
            default:
                throw new RuntimeException(
                        "Partitioner " + params.getPartitioner() + " not supported.");
        }
    }

    static PartitionFunction getPartitionFunction(Parameters params, Random random) {
        switch (params.getPartitioner()) {
            case "lsh":
                return new LSHPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getLshNumFamilies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth());
            case "lsh+random":
                return new LSHAndRandomPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getLshNumFamilies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth());
            case "lsh+proximity":
                return new LSHProximityPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getLshNumFamilies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth(),
                        params.getProximity());
            case "lsh+hilbert":
                List<Long> observedInsertIntervals =
                        ratesToIntervals(params.getObservedInsertRates());
                return new LSHHilbertPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getLshNumFamilies(),
                        params.getLshNumHashes(),
                        params.getLshBucketWidth(),
                        params.getLshNumHilbertBits(),
                        params.getLshPartitionUpdateInterval(),
                        params.getLshHilbertMaxRetainedElements(),
                        params.getMaxTTL(),
                        observedInsertIntervals,
                        params.getInsertThrottleThresholds(),
                        params.getParallelism(),
                        new HilbertCurve.Builder());
            case "simple":
                return new SimplePartitionFunction(params.getParallelism());
            default:
                throw new RuntimeException(
                        "Partitioner " + params.getPartitioner() + " not supported.");
        }
    }

    static long rateToInterval(long rate) {
        if (rate < 0L) return (-rate) * 1_000_000_000L;
        if (rate == 0L) return 0L; // 0 means no speed limit
        return 1_000_000_000L / rate;
    }

    static List<Long> ratesToIntervals(List<Long> rates) {
        return rates.stream().map(PartitionFunction::rateToInterval).collect(toList());
    }
}
