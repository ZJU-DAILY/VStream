package cn.edu.zju.daily.data.vector;

import com.github.jelmerk.knn.Item;
import java.io.Serializable;
import java.util.*;

/** This class represents a float data vector or query vector. */
public class FloatVector implements Serializable, Item<Long, float[]> {

    public static final String METADATA_TS_FIELD = "ts";

    private long _id;
    private final float[] value;
    private long eventTime = 0L;
    private long TTL;

    /**
     * Construct a float vector.
     *
     * @param id vector ID or query ID
     * @param value vector value
     */
    public FloatVector(long id, float[] value) {
        this(id, value, 0L, Long.MAX_VALUE);
    }

    /**
     * Construct a float vector.
     *
     * @param id vector ID or query ID
     * @param value vector value
     * @param eventTime event time
     * @param TTL query time-to-live
     */
    public FloatVector(long id, float[] value, long eventTime, long TTL) {
        this._id = id;
        this.value = value;
        this.eventTime = eventTime;
        this.TTL = TTL;
    }

    /**
     * Construct a random float vector.
     *
     * @param id vector ID
     * @param dim vector dimension
     * @return a random float vector
     */
    public static FloatVector getRandom(int id, int dim) {
        float[] arr = new float[dim];
        for (int i = 0; i < dim; i++) {
            arr[i] = (float) Math.random();
        }
        return new FloatVector(id, arr);
    }

    // ==========================
    // Getters
    // ==========================
    public long getId() {
        return _id;
    }

    public float[] array() {
        return value;
    }

    public int dim() {
        return value.length;
    }

    public long getTTL() {
        return TTL;
    }

    public long getEventTime() {
        return eventTime;
    }

    /**
     * Get the metadata, currently in the format of {@code {"ts": eventTime}}.
     *
     * @return metadata
     */
    public Map<String, String> getMetadata() {
        return Collections.singletonMap(METADATA_TS_FIELD, String.valueOf(eventTime));
    }

    public List<Float> list() {
        List<Float> elements = new ArrayList<>(value.length);
        for (float f : value) {
            elements.add(f);
        }
        return elements;
    }

    // These methods are required by the HNSW Item interface
    @Override
    public Long id() {
        return _id;
    }

    @Override
    public float[] vector() {
        return value;
    }

    @Override
    public int dimensions() {
        return value.length;
    }

    // ==========================
    // Setters
    // ==========================
    public void setId(long id) {
        this._id = id;
    }

    public void setTTL(long TTL) {
        this.TTL = TTL;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    // ==========================
    // Calculation
    // ==========================
    public float dot(FloatVector other) {
        float sum = 0;
        for (int i = 0; i < value.length; i++) {
            sum += value[i] * other.value[i];
        }
        return sum;
    }

    /**
     * Whether this FloatVector is a {@link DeletionMarker}.
     *
     * @return true if this FloatVector is a {@link DeletionMarker}
     */
    public boolean isDeletion() {
        return value.length == 0;
    }

    @Override
    public String toString() {
        return "FloatVector{"
                + "_id="
                + _id
                + ", value="
                + Arrays.toString(value)
                + ", eventTime="
                + eventTime
                + ", TTL="
                + TTL
                + '}';
    }
}
