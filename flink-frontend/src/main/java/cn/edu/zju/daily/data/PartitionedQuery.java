package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;

/** A query {@link FloatVector} which has been assigned to a partition. */
public class PartitionedQuery extends PartitionedData {

    public PartitionedQuery(int partitionId, int numPartitionsSent, FloatVector vector) {
        super(DataType.QUERY, partitionId);
        this.vector = vector;
        this.numPartitionsSent = numPartitionsSent;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final FloatVector vector;
    private final int numPartitionsSent;

    public FloatVector getVector() {
        return vector;
    }

    public int getNumPartitionsSent() {
        return numPartitionsSent;
    }

    @Override
    public String toString() {
        return "PartitionedFloatVector{"
                + "dataType="
                + getDataType()
                + ", partitionId="
                + getPartitionId()
                + ", numPartitionsSent="
                + numPartitionsSent
                + ", vector="
                + vector
                + '}';
    }
}
