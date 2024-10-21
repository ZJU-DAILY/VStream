package cn.edu.zju.daily.data.source.rate;

import java.io.Serializable;

/**
 * This interface is used to build a rate controller, which returns the required delay before
 * emitting a record given the current count.
 */
public interface RateControllerBuilder extends Serializable {

    /**
     * Build the rate controller on the cluster node to avoid serialization issues.
     *
     * @return the rate controller
     */
    RateController build();

    interface RateController {

        /** Get the delay for the next emit in nanoseconds. */
        long getDelayNanos(long count);
    }
}
