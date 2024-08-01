package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;

public class PartitionedFloatVector extends PartitionedData {

    public PartitionedFloatVector(int partitionId, FloatVector vector) {
        super(DataType.DATA, partitionId);
        this.vector = vector;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final FloatVector vector;

    public FloatVector getVector() {
        return vector;
    }

    @Override
    public String toString() {
        return "PartitionedFloatVector{" +
            "dataType=" + getDataType() +
            ", partitionId=" + getPartitionId() +
            ", vector=" + vector +
            '}';
    }
}
