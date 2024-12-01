package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.vector.VectorData;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** 简单哈希分区. */
public class SimpleUnaryPartitionFunction
        extends RichFlatMapFunction<VectorData, PartitionedElement> {

    private SimplePartitionFunction proxy;
    private final boolean isQuery;
    private final int numPartitions;

    public SimpleUnaryPartitionFunction(int numPartitions, boolean isQuery) {
        this.numPartitions = numPartitions;
        this.isQuery = isQuery;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.proxy = new SimplePartitionFunction(numPartitions);
        this.proxy.open(parameters);
    }

    /**
     * @param vector data.
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap(VectorData vector, Collector<PartitionedElement> collector)
            throws Exception {
        if (!isQuery) {
            this.proxy.flatMap1(vector, collector);
        } else {
            this.proxy.flatMap2(vector, collector);
        }
    }

    @Override
    public void close() throws Exception {
        this.proxy.close();
        super.close();
    }
}
