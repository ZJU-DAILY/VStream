package cn.edu.zju.daily.data.vector;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;

import java.io.Serializable;

/**
 * Parse a line of vector from HDFS.
 */
public class HDFSVectorParser implements Serializable {

    private PartitionedData.DataType parseDataType(String str) {
        if ("i".equals(str)) {
            return PartitionedData.DataType.DATA;
        } else if ("q".equals(str)) {
            return PartitionedData.DataType.QUERY;
        } else {
            throw new RuntimeException("Invalid data type: " + str);
        }
    }

    public FloatVector parseVector(String line) {
        if (line.startsWith("i") || line.startsWith("q")) {
            line = line.substring(2);
        }
        String[] parts = line.split(",");
        float[] array = new float[parts.length - 1];
        for (int i = 1; i < parts.length; i++) {
            array[i - 1] = Float.parseFloat(parts[i]);
        }
        return new FloatVector(Long.parseLong(parts[0]), array);
    }

    public PartitionedData parsePartitionedData(String line) {
        String[] parts = line.split(" ");
        if (parts.length != 2) {
            throw new RuntimeException("Invalid partitioned data: " + line);
        }
        PartitionedData.DataType dt = parseDataType(parts[0]);
        if (dt == PartitionedData.DataType.DATA) {
            return new PartitionedFloatVector(0, parseVector(parts[1]));
        } else if (dt == PartitionedData.DataType.QUERY) {
            return new PartitionedQuery(0, 0, parseVector(parts[1]));
        } else {
            throw new RuntimeException("Invalid partitioned data: " + line);
        }
    }

    public String unparse(FloatVector vector) {
        StringBuilder sb = new StringBuilder();
        sb.append(vector.getId());
        for (float f : vector.array()) {
            sb.append(",").append(f);
        }
        return sb.toString();
    }
}
