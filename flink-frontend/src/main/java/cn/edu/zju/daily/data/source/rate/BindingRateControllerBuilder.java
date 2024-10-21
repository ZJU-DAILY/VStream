package cn.edu.zju.daily.data.source.rate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BindingRateControllerBuilder implements RateControllerBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BindingRateControllerBuilder.class);

    private final String hdfsUser;
    private final String hdfsAddress;
    private final String hdfsPath;
    private final long initialDelayNanos;
    private final long newDelayNanos;
    private final long callbackCount;
    private final RateControllerBuilder builder;

    public BindingRateControllerBuilder(
            RateControllerBuilder builder,
            String hdfsAddress,
            String hdfsUser,
            String hdfsPath,
            long initialDelayNanos,
            long newDelayNanos,
            long callbackCount) {
        this.hdfsAddress = hdfsAddress;
        this.hdfsUser = hdfsUser;
        this.hdfsPath = hdfsPath;
        this.initialDelayNanos = initialDelayNanos;
        this.newDelayNanos = newDelayNanos;
        this.callbackCount = callbackCount;
        this.builder = builder;
    }

    @Override
    public RateController build() {
        DelayPusher pusher = null;
        try {
            pusher = new DelayPusher(hdfsAddress, hdfsUser, hdfsPath);
            pusher.push(initialDelayNanos);
        } catch (Exception e) {
            LOG.error(
                    "Could not create PollingRateWriter due to failing to connect to HDFS, rate will not be updated.",
                    e);
        }
        return new Controller(newDelayNanos, callbackCount, builder.build(), pusher);
    }

    public static class Controller implements RateController {

        private final long newDelayNanos;
        private final long callbackCount;
        private final RateController controller;
        private final DelayPusher pusher;

        private boolean called;

        private Controller(
                long newDelayNanos,
                long callbackCount,
                RateController controller,
                DelayPusher pusher) {
            this.newDelayNanos = newDelayNanos;
            this.callbackCount = callbackCount;
            this.controller = controller;
            this.pusher = pusher;
            this.called = false;
        }

        @Override
        public long getDelayNanos(long count) {
            long delay = controller.getDelayNanos(count);
            if (!called && count >= callbackCount) {
                pushNewDelay();
                called = true;
            }
            return delay;
        }

        private void pushNewDelay() {
            if (pusher != null) {
                pusher.push(newDelayNanos);
            } else {
                LOG.error(
                        "Could not push new delay to HDFS (pusher is null), rate will not be updated.");
            }
        }
    }
}
