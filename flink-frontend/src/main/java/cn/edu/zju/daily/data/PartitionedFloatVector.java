package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.DeletionMarker;
import cn.edu.zju.daily.data.vector.FloatVector;

/** A data {@link FloatVector} or {@link DeletionMarker} which has been assigned to a partition. */
public class PartitionedFloatVector extends PartitionedData {

    public PartitionedFloatVector(int partitionId, FloatVector vector) {
        super(DataType.INSERT_OR_DELETE, partitionId);
        this.vector = vector;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final FloatVector vector;

    public FloatVector getVector() {
        return vector;
    }

    @Override
    public String toString() {
        return "PartitionedFloatVector{"
                + "dataType="
                + getDataType()
                + ", partitionId="
                + getPartitionId()
                + ", vector="
                + vector
                + '}';
    }
}
