package cn.edu.zju.daily.data.source;

import java.util.List;

public class StagedRateController implements RateController {

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
        }
        return delays.get(index);
    }
}
