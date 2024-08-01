package cn.edu.zju.daily.data;

import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class DataSerializer {


    public static void serializeFloatVector(FloatVector value, byte[] id, byte[] array) {
        serializeLong(value.getId(), id);
        serializeFloatArray(value.array(), array);
    }

    public static void serializeFloatVector(FloatVector value, byte[] array) {
        serializeFloatArray(value.array(), array);
    }

    /**
     * Used for serializing data vectors when doing inserts.
     * @param value
     * @param id
     * @param array
     */
    public static void serializeFloatVectorWithTimestamp(FloatVector value, byte[] id, byte[] array) {
        assert (array.length == value.dim() * Float.BYTES + Long.BYTES);
        serializeLong(value.getId(), id);
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value.getEventTime());
        for (float v : value.array()) {
            buffer.putFloat(v);
        }
    }

    /**
     * Used for serializing query vectors. Format: [timestamp - TTL, vector]. The first item is used for constructing VectorSearchOptions.
     *
     * @param value
     * @param array
     */
    public static void serializeFloatVectorWithTimestampAndTTL(FloatVector value, byte[] array) {
        assert (array.length == value.dim() * Float.BYTES + Long.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(Math.max(0, value.getEventTime() - value.getTTL()));
        for (float v : value.array()) {
            buffer.putFloat(v);
        }
    }

    static void serializeLong(long value, byte[] target) {
        assert (target.length == Long.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(target);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
    }

    static void serializeFloatArray(float[] value, byte[] target) {
        assert (target.length == value.length * Float.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(target);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(value);
    }

    public static void serializeLongList(List<Long> value, byte[] target) {
        assert (target.length == value.size() * Long.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(target);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (long v : value) {
            buffer.putLong(v);
        }
    }

    public static long deserializeLong(byte[] byteArray) {
        assert (byteArray.length == Long.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getLong();
    }

    public static List<Long> deserializeLongList(byte[] byteArray) {
        assert (byteArray.length % Long.BYTES == 0);
        int length = byteArray.length / 8;
        List<Long> result = new ArrayList<>(length);
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < length; i++) {
            result.add(buffer.getLong());
        }
        return result;
    }

    public static void deserializeFloatArray(byte[] byteArray, float[] target) {
        assert (byteArray.length == target.length * Float.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(target);
    }


    public static SearchResult deserializeSearchResult(byte[] resultBytes, int nodeId, long queryId, int numPartitionsToCombine, long queryEventTime) {
        if (resultBytes == null) {
            return new SearchResult(nodeId, queryId, new ArrayList<>(), new ArrayList<>(), 1, numPartitionsToCombine, queryEventTime);
        }
        assert (resultBytes.length % (Long.BYTES + Float.BYTES) == 0);
        int length = resultBytes.length / (Long.BYTES + Float.BYTES);
        List<Long> ids = new ArrayList<>(length);
        List<Float> distances = new ArrayList<>(length);
        ByteBuffer buffer = ByteBuffer.wrap(resultBytes);  // [id1, distance1, id2, distance2, ...] in descending order
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < length; i++) {
            ids.add(buffer.getLong());
            distances.add(buffer.getFloat());
        }
        // reverse ids and distances
        for (int i = 0; i < length / 2; i++) {
            long tmpId = ids.get(i);
            float tmpDistance = distances.get(i);
            ids.set(i, ids.get(length - i - 1));
            distances.set(i, distances.get(length - i - 1));
            ids.set(length - i - 1, tmpId);
            distances.set(length - i - 1, tmpDistance);
        }
        return new SearchResult(nodeId, queryId, ids, distances, 1, numPartitionsToCombine, queryEventTime);
    }

    public static void serializeResultArray(long[] ids, float[] distances, byte[] target) {
        assert (ids.length == distances.length);
        assert (target.length == ids.length * (Long.BYTES + Float.BYTES));
        ByteBuffer buffer = ByteBuffer.wrap(target);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < ids.length; i++) {
            buffer.putLong(ids[i]);
            buffer.putFloat(distances[i]);
        }
    }
}
