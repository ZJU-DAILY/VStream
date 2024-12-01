package cn.edu.zju.daily.data.vector;

import java.util.Arrays;
import java.util.Objects;

/** This class marks deletion of a vector with a given ID. */
public class VectorDeletion implements VectorData {

    private long id;
    private float[] value;
    private long eventTime;
    private long TTL;

    public VectorDeletion(long id) {
        this(id, null, 0L, Long.MAX_VALUE);
    }

    public VectorDeletion(long id, long eventTime, long TTL) {
        this(id, null, eventTime, TTL);
    }

    public VectorDeletion(long id, float[] value, long eventTime, long TTL) {
        this.id = id;
        this.eventTime = eventTime;
        this.value = value;
        this.TTL = TTL;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public float[] getValue() {
        return value;
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public boolean isDeletion() {
        return true;
    }

    @Override
    public long getEventTime() {
        return eventTime;
    }

    @Override
    public long getTTL() {
        return TTL;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public void setValue(float[] value) {
        this.value = value;
    }

    @Override
    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public void setTTL(long TTL) {
        this.TTL = TTL;
    }

    @Override
    public VectorDeletion asDeletion() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorDeletion that = (VectorDeletion) o;
        return id == that.id && eventTime == that.eventTime && TTL == that.TTL;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, eventTime, TTL);
    }

    @Override
    public String toString() {
        return "VectorDeletion{"
                + "id="
                + id
                + ", value="
                + Arrays.toString(value)
                + ", eventTime="
                + eventTime
                + ", TTL="
                + TTL
                + '}';
    }
}
