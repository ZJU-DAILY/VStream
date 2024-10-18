package cn.edu.zju.daily.function;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PassThroughFunction<T> extends ProcessFunction<T, T> {

    @Override
    public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out)
            throws Exception {
        out.collect(value);
    }
}
