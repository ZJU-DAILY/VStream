package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedData;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class PartitionedDataSplitFunction extends ProcessFunction<PartitionedData, PartitionedData> {

    private final OutputTag<PartitionedData> queryOutput;

    public PartitionedDataSplitFunction(OutputTag<PartitionedData> queryOutput) {
        this.queryOutput = queryOutput;
    }

    @Override
    public void processElement(PartitionedData value,
                               ProcessFunction<PartitionedData, PartitionedData>.Context ctx,
                               Collector<PartitionedData> out) throws Exception {
        if (value.getDataType() == PartitionedData.DataType.QUERY) {
            ctx.output(queryOutput, value);
        } else {
            out.collect(value);
        }
    }
}
