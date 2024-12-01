package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import lombok.Getter;

/** A data {@link FloatVector} or {@link VectorDeletion} which has been assigned to a partition. */
@Getter
public class PartitionedData extends PartitionedElement {

    public PartitionedData(int partitionId, VectorData data) {
        super(DataType.INSERT_OR_DELETE, partitionId);
        this.data = data;
        this.setPartitionedAt(System.currentTimeMillis());
    }

    private final VectorData data;

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
