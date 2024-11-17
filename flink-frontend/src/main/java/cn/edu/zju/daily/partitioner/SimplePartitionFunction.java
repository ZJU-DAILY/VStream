package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.VectorData;
import lombok.Getter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** 简单哈希分区. 该函数实现了 CoFlatMapFunction 接口，同时处理数据和查询，这是为了确保数据和查询使用同样的分区策略。 */
public class SimplePartitionFunction extends RichPartitionFunction {

    @Getter private final int numPartitions; // this should be set as parallelism and maxParallelism

    private PartitionToKeyMapper mapper;

    /** Creates an LSH partitioner. */
    public SimplePartitionFunction(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.mapper = new PartitionToKeyMapper(numPartitions);
    }

    private int getNodeId(VectorData data) {
        int hash = Long.hashCode(data.getId());
        return Math.floorMod(hash, numPartitions);
    }

    @Override
    public void flatMap1(VectorData data, Collector<PartitionedElement> collector)
            throws Exception {
        if (mapper == null) {
            throw new IllegalStateException("Not initialized");
        }

        if (!data.hasValue()) {
            for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
                collector.collect(new PartitionedData(mapper.getKey(nodeId), data));
            }
        } else {
            int nodeId = getNodeId(data);
            collector.collect(new PartitionedData(mapper.getKey(nodeId), data));
        }
    }

    @Override
    public void flatMap2(VectorData data, Collector<PartitionedElement> collector)
            throws Exception {
        if (mapper == null) {
            throw new IllegalStateException("Not initialized");
        }

        if (data.isDeletion()) {
            throw new RuntimeException("Query does not have a value.");
        }
        for (int nodeId = 0; nodeId < numPartitions; nodeId++) {
            collector.collect(
                    new PartitionedQuery(mapper.getKey(nodeId), numPartitions, data.asVector()));
        }
    }
}
