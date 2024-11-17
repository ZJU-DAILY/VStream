package cn.edu.zju.daily.data.vector;

/** This interface represents a data vector or a deletion marker. */
public interface VectorData {

    long getId();

    float[] getValue();

    default double[] getDoubleValue() {
        if (!hasValue()) {
            return null;
        }
        float[] value = getValue();
        double[] array = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            array[i] = value[i];
        }
        return array;
    }

    long getEventTime();

    long getTTL();

    void setId(long id);

    void setValue(float[] value);

    void setEventTime(long eventTime);

    void setTTL(long TTL);

    boolean isDeletion();

    boolean hasValue();

    default float dot(VectorData other) {
        if (!hasValue() || !other.hasValue()) {
            throw new RuntimeException("No value.");
        }
        float sum = 0;
        for (int i = 0; i < getValue().length; i++) {
            sum += getValue()[i] * other.getValue()[i];
        }
        return sum;
    }

    default FloatVector asVector() {
        throw new RuntimeException("Not a vector.");
    }

    default VectorDeletion asDeletion() {
        throw new RuntimeException("Not a deletion marker.");
    }
}
