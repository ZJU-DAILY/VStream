package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;

public class MultiPartitionQuery {

    private final int[] partitions;
    private final FloatVector query;

    public MultiPartitionQuery(int[] partitions, FloatVector query) {
        this.partitions = partitions;
        this.query = query;
    }

    public FloatVector getQuery() {
        return query;
    }

    public int[] getPartitions() {
        return partitions;
    }
}
