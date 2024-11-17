package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;
import lombok.Getter;

/** A query {@link FloatVector} which has been assigned to a partition. */
public class PartitionedQuery extends PartitionedElement {

    public PartitionedQuery(int partitionId, int numPartitionsSent, FloatVector query) {
        super(DataType.QUERY, partitionId);
        this.vector = query;
        this.numPartitionsSent = numPartitionsSent;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final FloatVector vector;
    @Getter private final int numPartitionsSent;

    public FloatVector getData() {
        return vector;
    }

    @Override
    public String toString() {
        return "PartitionedQuery{"
                + "dataType="
                + getDataType()
                + ", partitionId="
                + getPartitionId()
                + ", numPartitionsSent="
                + numPartitionsSent
                + ", query="
                + vector
                + '}';
    }
}
