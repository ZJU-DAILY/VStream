package cn.edu.zju.daily.rate;

import cn.edu.zju.daily.data.vector.FloatVector;

import java.util.Collections;
import java.util.List;

/**
 * Limits the input rate and logs the event time in FloatVector.
 */
public class FloatVectorRateLimiter extends RateLimiter<FloatVector, FloatVector> {


    public FloatVectorRateLimiter(List<Long> thresholds, List<Long> intervals) {
        super(thresholds, intervals);
    }

    public FloatVectorRateLimiter(long interval) {
        super(Collections.singletonList(0L), Collections.singletonList(interval));
    }

    @Override
    public FloatVector transform(FloatVector value, long timestamp) {
        value.setEventTime(timestamp);
        return value;
    }
}
