package cn.edu.zju.daily.data.source.rate;

import cn.edu.zju.daily.util.HadoopFileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollingRateControllerBuilder implements RateControllerBuilder {

    private final String hdfsUser;
    private final String hdfsAddress;
    private final String hdfsPath;
    private final long pollIntervalMillis;
    private final long initialDelayNanos;

    public PollingRateControllerBuilder(
            String hdfsAddress,
            String hdfsUser,
            String hdfsPath,
            long pollIntervalMillis,
            long initialDelayNanos) {
        this.hdfsAddress = hdfsAddress;
        this.hdfsUser = hdfsUser;
        this.hdfsPath = hdfsPath;
        this.pollIntervalMillis = pollIntervalMillis;
        this.initialDelayNanos = initialDelayNanos;
    }

    @Override
    public RateController build() {
        return new Controller(
                hdfsAddress, hdfsUser, hdfsPath, pollIntervalMillis, initialDelayNanos);
    }

    public static class Controller implements RateController {

        private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
        private final String hdfsPath;
        private final long pollIntervalMillis;
        private final HadoopFileHelper hdfs;

        private long delayNanos;
        private long lastPollMillis;

        private Controller(
                String hdfsAddress,
                String hdfsUser,
                String hdfsPath,
                long pollIntervalMillis,
                long initialDelayNanos) {
            try {
                this.hdfs = new HadoopFileHelper(hdfsAddress, hdfsUser);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Could not create PollingRateController due to failing to connect to HDFS",
                        e);
            }
            this.hdfsPath = hdfsPath;
            this.pollIntervalMillis = pollIntervalMillis;
            this.delayNanos = initialDelayNanos;
        }

        @Override
        public long getDelayNanos(long count) {
            long now = System.currentTimeMillis();
            if (lastPollMillis == 0) {
                lastPollMillis = now;
            }
            if (now - lastPollMillis >= pollIntervalMillis) {
                pollNextDelay();
                lastPollMillis = now;
            }
            return delayNanos;
        }

        private void pollNextDelay() {
            boolean exists;
            try {
                exists = hdfs.exists(hdfsPath);
            } catch (Exception e) {
                LOG.warn("Failed to check if the delay file exists, keeping the current delay.");
                return;
            }

            if (exists) {
                try {
                    long newDelayNanos = Long.parseLong(hdfs.readAllLines(hdfsPath).get(0));
                    if (newDelayNanos != delayNanos) {
                        delayNanos = newDelayNanos;
                        LOG.info("Successfully polled the new delay: {} ns", delayNanos);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to read the delay file, keeping the current delay.");
                }
            }
        }
    }
}
