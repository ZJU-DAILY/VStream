package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.VectorData;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/**
 * This class represents a partitioned data object, which can be a data vector or a query vector.
 */
@Getter
public class PartitionedElement implements Serializable {

    public PartitionedElement(DataType dataType, int partitionId) {
        this.dataType = dataType;
        this.partitionId = partitionId;
    }

    public enum DataType {
        /** Query vector. */
        QUERY,
        /** Data vector or delete marker. */
        INSERT_OR_DELETE,
        /** An operation for the dummy backend to dump the data. */
        DUMP
    }

    private final DataType dataType;
    private final int partitionId;
    @Setter private long partitionedAt;

    public int getNumPartitionsSent() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public VectorData getData() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String toString() {
        return "PartitionedData{" + "dataType=" + dataType + ", partitionId=" + partitionId + '}';
    }
}
