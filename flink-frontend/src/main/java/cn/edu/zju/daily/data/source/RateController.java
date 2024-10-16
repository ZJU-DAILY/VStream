package cn.edu.zju.daily.data.source;

import java.io.Serializable;

public interface RateController extends Serializable {

    /** Get the delay for the next emit in nanoseconds. */
    long getDelayNanos(long count);
}
