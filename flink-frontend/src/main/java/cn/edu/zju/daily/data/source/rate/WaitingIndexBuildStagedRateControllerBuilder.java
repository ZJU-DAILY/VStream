package cn.edu.zju.daily.data.source.rate;

import cn.edu.zju.daily.util.MilvusUtil;
import io.milvus.grpc.IndexDescription;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitingIndexBuildStagedRateControllerBuilder implements RateControllerBuilder {

    private final List<Long> stages;
    private final List<Long> delays; // in nanoseconds
    private final List<Float> waitRatios;
    private final String milvusHost;
    private final int milvusPort;
    private final String milvusCollectionName;
    private final String milvusIndexName;

    public WaitingIndexBuildStagedRateControllerBuilder(
            List<Long> stages,
            List<Long> delays,
            List<Float> waitRatios,
            String milvusHost,
            int milvusPort,
            String milvusCollectionName,
            String milvusIndexName) {
        this.stages = stages;
        this.delays = delays;
        this.waitRatios = waitRatios;
        this.milvusHost = milvusHost;
        this.milvusPort = milvusPort;
        this.milvusCollectionName = milvusCollectionName;
        this.milvusIndexName = milvusIndexName;
    }

    @Override
    public WaitingIndexBuildStagedRateControllerBuilder.Controller build() {
        return new WaitingIndexBuildStagedRateControllerBuilder.Controller(
                stages,
                delays,
                waitRatios,
                milvusHost,
                milvusPort,
                milvusCollectionName,
                milvusIndexName);
    }

    public static class Controller implements RateController {

        private static final long SLEEP_INTERVAL = 10000;

        private static final Logger LOG =
                LoggerFactory.getLogger(
                        WaitingIndexBuildStagedRateControllerBuilder.Controller.class);
        private final List<Long> stages;
        private final List<Long> delays; // in nanoseconds
        private final List<Float> waitRatios;
        private final String milvusCollectionName;
        private final String milvusIndexName;
        private int index = 0;
        private long maxCount = -1;
        private final MilvusUtil util;

        private Controller(
                List<Long> stages,
                List<Long> delays,
                List<Float> waitRatios,
                String milvusHost,
                int milvusPort,
                String milvusCollectionName,
                String milvusIndexName) {
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
            this.milvusCollectionName = milvusCollectionName;
            this.milvusIndexName = milvusIndexName;
            this.util = new MilvusUtil();
            util.connect(milvusHost, milvusPort);
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
            while (true) {
                IndexDescription desc =
                        util.getIndexDescription(milvusCollectionName, milvusIndexName);
                float ratio;
                if (desc.getTotalRows() == 0) {
                    ratio = 1;
                } else {
                    ratio = (float) desc.getIndexedRows() / desc.getTotalRows();
                }
                if (ratio >= waitRatio) {
                    LOG.info(
                            "Total rows: {}, indexed rows: {}, pending index rows: {}, ratio ({}) above required {}",
                            desc.getTotalRows(),
                            desc.getIndexedRows(),
                            desc.getPendingIndexRows(),
                            ratio,
                            waitRatio);
                    break;
                } else {
                    LOG.info(
                            "Total rows: {}, indexed rows: {}, pending index rows: {}, ratio ({}) still below required {}",
                            desc.getTotalRows(),
                            desc.getIndexedRows(),
                            desc.getPendingIndexRows(),
                            ratio,
                            waitRatio);
                }
                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for index build", e);
                }
            }
        }
    }
}
