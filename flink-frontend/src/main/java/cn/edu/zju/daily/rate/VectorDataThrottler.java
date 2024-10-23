package cn.edu.zju.daily.rate;

import cn.edu.zju.daily.data.vector.VectorData;
import java.util.Collections;
import java.util.List;

/** Limits the input rate and logs the event time in FloatVector. */
public class VectorDataThrottler extends Throttler<VectorData, VectorData> {

    private final boolean reassignId;

    public VectorDataThrottler(List<Long> thresholds, List<Long> intervals, boolean reassignId) {
        super(thresholds, intervals);
        this.reassignId = reassignId;
    }

    public VectorDataThrottler(long interval, boolean reassignId) {
        super(Collections.singletonList(0L), Collections.singletonList(interval));
        this.reassignId = reassignId;
    }

    @Override
    public VectorData transform(VectorData value, long timestamp, long count) {
        value.setEventTime(timestamp);
        if (reassignId) {
            value.setId(count);
        }
        return value;
    }
}
