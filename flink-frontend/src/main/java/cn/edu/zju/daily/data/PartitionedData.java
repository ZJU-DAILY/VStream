package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.Serializable;

public class PartitionedData implements Serializable {

    public PartitionedData(DataType dataType, int partitionId) {
        this.dataType = dataType;
        this.partitionId = partitionId;
    }

    public enum DataType {
        QUERY,
        DATA,
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
