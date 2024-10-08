package cn.edu.zju.daily.function.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.Parameters;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

public interface PartitionFunction
        extends CoFlatMapFunction<FloatVector, FloatVector, PartitionedData>,
                FlatMapFunction<PartitionedData, PartitionedData> {

    static Map<Integer, Integer> getNodeIdMap(int parallelism) {
        Map<Integer, Integer> nodeIdMap = new HashMap<>();
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

    @Override
    default void flatMap(PartitionedData value, Collector<PartitionedData> out) throws Exception {
        if (value.getDataType().equals(PartitionedData.DataType.INSERT_OR_DELETE)) {
            flatMap1(value.getVector(), out);
        } else if (value.getDataType().equals(PartitionedData.DataType.QUERY)) {
            flatMap2(value.getVector(), out);
        } else {
            throw new RuntimeException("Unsupported data type: " + value.getDataType());
        }
    }

    static FlatMapFunction<FloatVector, PartitionedData> getUnaryPartitionFunction(
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
                        isQuery);
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
                        params.getNumCopies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth());
            case "lsh+random":
                return new LSHAndRandomPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getNumCopies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth());
            case "lsh+proximity":
                return new LSHProximityPartitionFunction(
                        random,
                        params.getVectorDim(),
                        params.getNumCopies(),
                        params.getLshNumHashes(),
                        params.getParallelism(),
                        params.getLshBucketWidth(),
                        params.getProximity());
            case "lsh+hilbert":
                long fakeInsertInterval = rateToInterval(params.getFakeInsertRate());
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
                        fakeInsertInterval,
                        params.getParallelism());
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
}
