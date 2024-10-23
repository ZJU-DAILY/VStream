package cn.edu.zju.daily.data.vector;

import com.github.jelmerk.knn.Item;
import java.io.Serializable;
import java.util.*;

/** This class represents a float data vector or query vector. */
public class FloatVector implements VectorData, Serializable, Item<Long, float[]> {

    public static final String METADATA_TS_FIELD = "ts";

    private long id;
    private float[] value;
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
        this.id = id;
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
    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isDeletion() {
        return false;
    }

    @Override
    public float[] getValue() {
        return value;
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    public int dim() {
        return value.length;
    }

    @Override
    public long getTTL() {
        return TTL;
    }

    @Override
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
        return id;
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
    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public void setTTL(long TTL) {
        this.TTL = TTL;
    }

    @Override
    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public void setValue(float[] value) {
        this.value = value;
    }

    // ==========================
    // Calculation
    // ==========================

    @Override
    public FloatVector asVector() {
        return this;
    }

    @Override
    public String toString() {
        return "FloatVector{"
                + "_id="
                + id
                + ", value="
                + Arrays.toString(value)
                + ", eventTime="
                + eventTime
                + ", TTL="
                + TTL
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FloatVector that = (FloatVector) o;
        return id == that.id
                && eventTime == that.eventTime
                && TTL == that.TTL
                && Objects.deepEquals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, Arrays.hashCode(value), eventTime, TTL);
    }
}
