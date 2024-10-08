package cn.edu.zju.daily.data.vector;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedFloatVector;
import cn.edu.zju.daily.data.PartitionedQuery;
import java.io.Serializable;

/** Parse or unparse a line of vector from HDFS. */
public class HDFSVectorParser implements Serializable {

    private enum DataType {
        /** Query vector. */
        QUERY("q"),
        /** Insert vector. */
        INSERT("i"),
        /**
         * Delete marker. A line represents a delete marker if it starts with "d", or only includes
         * the vector ID.
         */
        DELETE("d");

        final String prefix;

        DataType(String prefix) {
            this.prefix = prefix;
        }
    }

    private DataType parseDataType(String str) {
        for (DataType dt : DataType.values()) {
            if (dt.prefix.equals(str)) {
                return dt;
            }
        }
        throw new RuntimeException("Invalid data type: " + str);
    }

    /**
     * Parse a line of vector from HDFS.
     *
     * @param line the line to parse
     * @return a {@link FloatVector} object
     */
    public FloatVector parseVector(String line) {

        // If the line starts with "d", it is a deletion marker.
        if (line.startsWith(DataType.DELETE.prefix)) {
            return parseDeletionMarker(line);
        }

        // Otherwise, it is an insert or query vector, unless the vector content is missing.
        if (line.startsWith(DataType.INSERT.prefix) || line.startsWith(DataType.QUERY.prefix)) {
            line = line.substring(2);
        }
        String[] parts = line.split(",");
        long id = Long.parseLong(parts[0]);

        if (parts.length == 1) {
            return new DeletionMarker(id);
        }

        float[] array = new float[parts.length - 1];
        for (int i = 1; i < parts.length; i++) {
            array[i - 1] = Float.parseFloat(parts[i]);
        }
        return new FloatVector(Long.parseLong(parts[0]), array);
    }

    public FloatVector parseDeletionMarker(String line) {
        if (line.startsWith(DataType.DELETE.prefix)) {
            line = line.substring(2);
        }
        long id = Long.parseLong(line);
        return new DeletionMarker(id);
    }

    /**
     * Parse a line of vector from HDFS, and return it as a {@link PartitionedData} object.
     *
     * @param line the line to parse
     * @return a {@link PartitionedData} object
     */
    public PartitionedData parsePartitionedData(String line) {
        String[] parts = line.split(" ");
        if (parts.length != 2) {
            throw new RuntimeException("Invalid partitioned data: " + line);
        }
        DataType dt = parseDataType(parts[0]);
        if (dt == DataType.INSERT) {
            return new PartitionedFloatVector(0, parseVector(parts[1]));
        } else if (dt == DataType.QUERY) {
            return new PartitionedQuery(0, 0, parseVector(parts[1]));
        } else if (dt == DataType.DELETE) {
            return new PartitionedFloatVector(0, parseDeletionMarker(parts[1]));
        } else {
            throw new RuntimeException("Invalid partitioned data: " + line);
        }
    }

    /**
     * Unparse a {@link FloatVector} object to a string.
     *
     * @param vector the vector to unparse
     * @return the string representation of the vector
     */
    public String unparseBare(FloatVector vector) {
        StringBuilder sb = new StringBuilder();
        unparseBareVector(sb, vector);
        return sb.toString();
    }

    public String unparseInsert(FloatVector vector) {
        StringBuilder sb = new StringBuilder();
        sb.append(DataType.INSERT.prefix);
        unparseBareVector(sb, vector);
        return sb.toString();
    }

    public String unparseDelete(long id) {
        return DataType.DELETE.prefix + id;
    }

    private void unparseBareVector(StringBuilder sb, FloatVector vector) {
        sb.append(vector.getId());
        for (float f : vector.array()) {
            sb.append(",").append(f);
        }
    }
}
