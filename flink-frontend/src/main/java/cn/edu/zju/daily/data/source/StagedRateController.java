package cn.edu.zju.daily.data.source;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagedRateController implements RateController {

    private static final Logger LOG = LoggerFactory.getLogger(StagedRateController.class);
    private final List<Long> stages;
    private final List<Long> delays; // in nanoseconds
    private int index = 0;
    private long maxCount = -1;

    public StagedRateController(List<Long> stages, List<Long> delays) {
        if (stages.size() != delays.size()) {
            throw new IllegalArgumentException("stages.size() != delays.size()");
        }
        for (int i = 1; i < stages.size(); i++) {
            if (stages.get(i - 1) >= stages.get(i)) {
                throw new IllegalArgumentException("stages must be monotonically increasing.");
            }
        }
        this.stages = stages;
        this.delays = delays;
    }

    @Override
    public long getDelayNanos(long count) {
        if (count <= maxCount) {
            throw new IllegalArgumentException("count should be increasing across invocations.");
        }
        maxCount = count;
        if (index < stages.size() - 1 && count >= stages.get(index + 1)) {
            index++;
            LOG.info("Reached threshold {}, new delay {} ns", stages.get(index), delays.get(index));
        }
        return delays.get(index);
    }
}
