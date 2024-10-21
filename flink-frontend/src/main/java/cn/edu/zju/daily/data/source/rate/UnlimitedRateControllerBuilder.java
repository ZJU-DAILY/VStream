package cn.edu.zju.daily.data.source.rate;

public class UnlimitedRateControllerBuilder implements RateControllerBuilder {

    @Override
    public RateController build() {
        return new Controller();
    }

    public static class Controller implements RateController {

        private Controller() {}

        @Override
        public long getDelayNanos(long count) {
            return 0;
        }
    }
}
