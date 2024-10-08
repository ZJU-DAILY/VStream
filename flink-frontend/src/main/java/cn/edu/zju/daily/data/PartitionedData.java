package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.Serializable;

/**
 * This class represents a partitioned data object, which can be a data vector or a query vector.
 */
public class PartitionedData implements Serializable {

    public PartitionedData(DataType dataType, int partitionId) {
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
    private long partitionedAt;

    public void setPartitionedAt(long partitionedAt) {
        this.partitionedAt = partitionedAt;
    }

    public long getPartitionedAt() {
        return partitionedAt;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getNumPartitionsSent() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public FloatVector getVector() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public String toString() {
        return "PartitionedData{" + "dataType=" + dataType + ", partitionId=" + partitionId + '}';
    }
}
