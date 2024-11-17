package cn.edu.zju.daily.data.source.rate;

import java.util.List;
import lombok.extern.slf4j.Slf4j;

public class WaitingIndexBuildStagedRateControllerBuilder implements RateControllerBuilder {

    private final List<Long> stages;
    private final List<Long> delays; // in nanoseconds
    private final List<Float> waitRatios;
    private final WaitingIndexBuildStrategy strategy;

    public WaitingIndexBuildStagedRateControllerBuilder(
            List<Long> stages,
            List<Long> delays,
            List<Float> waitRatios,
            WaitingIndexBuildStrategy strategy) {
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
        this.waitRatios = waitRatios;
        this.strategy = strategy;
    }

    @Override
    public WaitingIndexBuildStagedRateControllerBuilder.Controller build() {
        return new WaitingIndexBuildStagedRateControllerBuilder.Controller(
                stages, delays, waitRatios, strategy);
    }

    @Slf4j
    public static class Controller implements RateController {

        private final List<Long> stages;
        private final List<Long> delays; // in nanoseconds
        private final List<Float> waitRatios;
        private final WaitingIndexBuildStrategy strategy;
        private int index = 0;
        private long maxCount = -1;

        private Controller(
                List<Long> stages,
                List<Long> delays,
                List<Float> waitRatios,
                WaitingIndexBuildStrategy strategy) {
            this.stages = stages;
            this.delays = delays;
            this.waitRatios = waitRatios;
            this.strategy = strategy;
        }

        @Override
        public long getDelayNanos(long count) {
            if (count <= maxCount) {
                throw new IllegalArgumentException(
                        "count should be increasing across invocations.");
            }
            maxCount = count;
            if (index < stages.size() - 1 && count >= stages.get(index + 1)) {
                index++;
                LOG.info("Reached threshold {}, waiting for index build...", stages.get(index));
                waitIndexBuild(waitRatios.get(index));
                LOG.info("New delay: {} ns", delays.get(index));
            }
            return delays.get(index);
        }

        private void waitIndexBuild(float waitRatio) {
            strategy.waitIndexBuild(waitRatio);
        }
    }
}
