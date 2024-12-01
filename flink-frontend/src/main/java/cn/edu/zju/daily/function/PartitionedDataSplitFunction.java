package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedElement;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class PartitionedDataSplitFunction
        extends ProcessFunction<PartitionedElement, PartitionedElement> {

    private final OutputTag<PartitionedElement> queryOutput;

    public PartitionedDataSplitFunction(OutputTag<PartitionedElement> queryOutput) {
        this.queryOutput = queryOutput;
    }

    @Override
    public void processElement(
            PartitionedElement value,
            ProcessFunction<PartitionedElement, PartitionedElement>.Context ctx,
            Collector<PartitionedElement> out)
            throws Exception {
        if (value.getDataType() == PartitionedElement.DataType.QUERY) {
            ctx.output(queryOutput, value);
        } else {
            out.collect(value);
        }
    }
}
