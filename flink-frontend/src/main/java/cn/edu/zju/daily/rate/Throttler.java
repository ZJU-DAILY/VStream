package cn.edu.zju.daily.rate;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/** @author auroflow */
@Slf4j
public abstract class Throttler<U, V> extends RichMapFunction<U, V> {

    private final List<Long> thresholds;
    private final List<Long> intervals; // nanoseconds

    // State: timestamp of last emission, in nanoseconds
    private long lastEmitted = 0L;
    // State: how many elements this limiter has processed
    private long count = 0L;
    // State: the index of counts and intervals we are at
    private int index = 0;

    /**
     * Creates a rate limiter. Thresholds and intervals define a set of stages, each with different
     * rates.
     *
     * @param thresholds a list of numbers of processed elements
     * @param intervals corresponding process intervals
     */
    protected Throttler(List<Long> thresholds, List<Long> intervals) {
        this.thresholds = thresholds;
        this.intervals = intervals;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastEmitted = System.nanoTime();
        count = 0;
    }

    private void updateIndexAndCount() {
        // Assume: `thresholds` are sorted, and `processed` and `index` never decreases.
        if (index < thresholds.size() - 1 && count >= thresholds.get(index + 1)) {
            index++;
            LOG.info("Count reaches {}, interval is now {}", count, getThisInterval());
        }
        count++;
    }

    private long getThisInterval() {
        return intervals.get(index);
    }

    @Override
    public V map(U value) throws Exception {

        updateIndexAndCount();
        long interval = getThisInterval();

        if (interval == 0) {
            return transform(value, System.currentTimeMillis(), count);
        }

        // Retrieves the current state
        long current = System.nanoTime();

        // If interval is not reached, busy wait
        while (current - lastEmitted < interval) {
            current = System.nanoTime();
        }

        lastEmitted = current;
        return transform(value, System.currentTimeMillis(), count);
    }

    /**
     * Transforms the input value into the output value.
     *
     * @param value the input value
     * @param timestamp the timestamp when the input value is emitted
     * @return the transformed value
     */
    public abstract V transform(U value, long timestamp, long count);
}
