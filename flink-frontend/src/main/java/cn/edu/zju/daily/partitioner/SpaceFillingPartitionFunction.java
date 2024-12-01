package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.curve.SpaceFillingCurve;
import java.math.BigInteger;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** Use a space-filling curve to partition the data (without using LSH). */
@Slf4j
public class SpaceFillingPartitionFunction extends RichPartitionFunction implements Initializable {

    private final int dim;
    private final int numSpaceFillingBits;
    private final int numPartitions;
    private final int replicationFactor;
    private final int windowSize;
    private float minValue;
    private float maxValue;
    private final SpaceFillingCurve.Builder curveBuilder;
    private PartitionToKeyMapper mapper;
    private SpaceFillingCurve curve;
    private List<BigInteger> partitionHeads;
    private List<BigInteger> window;

    /**
     * Constructor for SpaceFillingPartitionFunction.
     *
     * @param dim The dimension of the data vectors
     * @param minValue The minimum value of the data vectors
     * @param maxValue The maximum value of the data vectors
     * @param numSpaceFillingBits The number of bits to use for the space-filling curve
     * @param numPartitions The number of partitions
     * @param replicationFactor The replication factor
     * @param windowSize The size of the window
     * @param curveBuilder The builder for the space-filling curve
     */
    public SpaceFillingPartitionFunction(
            int dim,
            float minValue,
            float maxValue,
            int numSpaceFillingBits,
            int numPartitions,
            int replicationFactor,
            int windowSize,
            SpaceFillingCurve.Builder curveBuilder) {

        this.dim = dim;
        this.numSpaceFillingBits = numSpaceFillingBits;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.windowSize = windowSize;
        this.curveBuilder = curveBuilder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = new PartitionToKeyMapper(numPartitions);
        curve = curveBuilder.setBits(numSpaceFillingBits).setDimension(dim).build();
        partitionHeads = new ArrayList<>();
        window = new ArrayList<>();
        initializeWindow();
    }

    @Override
    public void initialize(List<FloatVector> vectors) {
        minValue = Float.MAX_VALUE;
        maxValue = Float.MIN_VALUE;
        for (FloatVector vector : vectors) {
            for (float value : vector.getValue()) {
                minValue = Math.min(minValue, value);
                maxValue = Math.max(maxValue, value);
            }
        }
    }

    private void initializeWindow() {
        partitionHeads.clear();
        BigInteger partitionHead = BigInteger.ZERO;
        BigInteger partitionStep =
                BigInteger.ONE
                        .shiftLeft(numSpaceFillingBits * dim)
                        .divide(BigInteger.valueOf(numPartitions));
        for (int i = 0; i < numPartitions; i++) {
            partitionHeads.add(partitionHead);
            partitionHead = partitionHead.add(partitionStep);
        }
    }

    private BigInteger getSpaceFillingValue(VectorData vector) {
        long[] coords = new long[vector.getValue().length];
        for (int i = 0; i < vector.getValue().length; i++) {
            float percent = (vector.getValue()[i] - minValue) / (maxValue - minValue);
            percent = Math.max(0, Math.min(1, percent));
            coords[i] =
                    Math.min(
                            (long) (percent * (1L << numSpaceFillingBits)),
                            (1L << numSpaceFillingBits) - 1);
        }
        return curve.index(coords);
    }

    private void createWindowIfFull() {
        if (window.size() == windowSize) {
            partitionHeads.clear();

            window.sort(BigInteger::compareTo);
            double percent = 0;
            for (int i = 0; i < numPartitions; i++) {
                int index = (int) (percent * window.size());
                partitionHeads.add(window.get(index));
                percent += 1.0 / numPartitions;
            }

            window.clear();
        }
    }

    private int getCentralIndex(BigInteger spaceFillingValue) {
        for (int i = 0; i < numPartitions; i++) {
            if (i == numPartitions - 1
                    || spaceFillingValue.compareTo(partitionHeads.get(i + 1)) < 0) {
                return i;
            }
        }
        throw new RuntimeException("Impossible branch.");
    }

    /**
     * Processes a data tuple.
     *
     * @param data The stream element
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void flatMap1(VectorData data, Collector<PartitionedElement> out) throws Exception {

        createWindowIfFull();

        Set<Integer> partitions = new HashSet<>();
        if (!data.hasValue()) {
            // This is a deletion without a vector value. Apply to all partitions.
            for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
                partitions.add(nodeId);
            }
        } else {
            getPartitionsUsingSFCurve(data, partitions);
        }

        for (int partition : partitions) {
            out.collect(new PartitionedData(mapper.getKey(partition), data));
        }
    }

    /**
     * Processes a query tuple.
     *
     * @param data The stream element
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void flatMap2(VectorData data, Collector<PartitionedElement> out) throws Exception {

        if (!data.hasValue()) {
            throw new RuntimeException("Query vector must have a value.");
        }
        Set<Integer> partitions = new HashSet<>();
        getPartitionsUsingSFCurve(data, partitions);
        for (int partition : partitions) {
            out.collect(
                    new PartitionedQuery(
                            mapper.getKey(partition), partitions.size(), data.asVector()));
        }
    }

    private void getPartitionsUsingSFCurve(VectorData data, Set<Integer> partitions) {
        BigInteger idx = getSpaceFillingValue(data);
        window.add(idx);
        int centralIndex = getCentralIndex(idx);
        partitions.add(centralIndex);
        int offset = 1;
        while (true) {
            int left = (centralIndex - offset + numPartitions) % numPartitions;
            partitions.add(left);
            if (partitions.size() >= replicationFactor) {
                break;
            }
            int right = (centralIndex + offset) % numPartitions;
            partitions.add(right);
            if (partitions.size() >= replicationFactor) {
                break;
            }
            offset++;
        }
    }
}
