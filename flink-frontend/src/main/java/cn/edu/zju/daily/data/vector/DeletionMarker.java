package cn.edu.zju.daily.data.vector;

/** This class marks deletion of a vector with a given ID. */
public class DeletionMarker extends FloatVector {

    public DeletionMarker(long id) {
        super(id, new float[0]);
    }

    public DeletionMarker(long id, long eventTime, long TTL) {
        super(id, new float[0], eventTime, TTL);
    }
}
