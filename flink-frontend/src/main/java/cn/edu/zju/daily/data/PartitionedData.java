package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.data.vector.VectorDeletion;

/** A data {@link FloatVector} or {@link VectorDeletion} which has been assigned to a partition. */
public class PartitionedData extends PartitionedElement {

    public PartitionedData(int partitionId, VectorData data) {
        super(DataType.INSERT_OR_DELETE, partitionId);
        this.data = data;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final VectorData data;

    public VectorData getData() {
        return data;
    }

    @Override
    public String toString() {
        return "PartitionedFloatVector{"
                + "dataType="
                + getDataType()
                + ", partitionId="
                + getPartitionId()
                + ", data="
                + data
                + '}';
    }
}
