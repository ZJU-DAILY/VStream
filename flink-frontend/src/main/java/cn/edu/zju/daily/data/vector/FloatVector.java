package cn.edu.zju.daily.data.vector;

import com.github.jelmerk.knn.Item;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FloatVector implements Serializable, Item<Long, float[]> {

    private final long _id;  // vector ID, or query ID
    private final float[] value;
    private long eventTime = 0L; // event time
    private long TTL; // for query, this means search vectors with event time in [eventTime - TTL, eventTime]; for data, this field is reserved

    public FloatVector(long id, float[] value) {
        this(id, value, 0L, Long.MAX_VALUE);
    }

    public FloatVector(long id, float[] value, long eventTime, long TTL) {
        this._id = id;
        this.value = value;
        this.eventTime = eventTime;
        this.TTL = TTL;
    }

    public long getId() {
        return _id;
    }

    public float[] array() {
        return value;
    }

    public List<Float> list() {
        List<Float> elements = new ArrayList<>(value.length);
        for (float f : value) {
            elements.add(f);
        }
        return elements;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getEventTime() {
        return eventTime;
    }

    public float dot(FloatVector other) {
        float sum = 0;
        for (int i = 0; i < value.length; i++) {
            sum += value[i] * other.value[i];
        }
        return sum;
    }

    public static FloatVector getRandom(int id, int dim) {
        float[] arr = new float[dim];
        for (int i = 0; i < dim; i++) {
            arr[i] = (float) Math.random();
        }
        return new FloatVector(id, arr);
    }

    public int dim() {
        return value.length;
    }

    @Override
    public String toString() {
        return "FloatVector{" +
                "_id=" + _id +
                ", value=" + Arrays.toString(value) +
                ", eventTime=" + eventTime +
                ", TTL=" + TTL +
                '}';
    }

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

    public void setTTL(long TTL) {
        this.TTL = TTL;
    }

    public long getTTL() {
        return TTL;
    }
}
