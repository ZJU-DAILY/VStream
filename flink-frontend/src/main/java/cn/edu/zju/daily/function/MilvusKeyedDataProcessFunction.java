package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.MilvusUtil;
import cn.edu.zju.daily.util.Parameters;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Milvus insert function.
 */
public class MilvusKeyedDataProcessFunction extends KeyedProcessFunction<Integer, PartitionedData, Object> {

    private final Parameters params;
    ListState<FloatVector> buffer;
    ValueState<Integer> count;
    private MilvusUtil milvusUtil = null;
    private static final Logger LOG = LoggerFactory.getLogger(MilvusKeyedDataProcessFunction.class);

    public MilvusKeyedDataProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<FloatVector> listStateDescriptor = new ListStateDescriptor<>("buffer", FloatVector.class);
        buffer = getRuntimeContext().getListState(listStateDescriptor);
        ValueStateDescriptor<Integer> countState = new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(countState);
        milvusUtil = new MilvusUtil();
        milvusUtil.connect(params.getMilvusHost(), params.getMilvusPort());
    }

    @Override
    public void processElement(PartitionedData value,
                               KeyedProcessFunction<Integer, PartitionedData, Object>.Context ctx,
                               Collector<Object> out) throws Exception {

        // initialize state
        if (count.value() == null) {
            count.update(0);
        }

        int bufferCapacity = params.getMilvusInsertBufferCapacity();
        String partitionName = Integer.toString(ctx.getCurrentKey());

        // if buffer is full, insert to Milvus
        if (count.value() + 1 == bufferCapacity) {
            List<FloatVector> vectors = new ArrayList<>();
            for (FloatVector vector : buffer.get()) {
                vectors.add(vector);
            }
            vectors.add(value.getVector());

            long start = System.currentTimeMillis();
            milvusUtil.insert(vectors, params.getMilvusCollectionName(), partitionName, false);
            if (vectors.size() > 1) {
                LOG.info("Partition {}: {} vectors (from #{}) inserted in {} ms.",  ctx.getCurrentKey(), vectors.size(),
                        vectors.get(0).getId(), System.currentTimeMillis() - start);
            } else {
                LOG.info("Partition {}: Vector #{} inserted in {} ms.", ctx.getCurrentKey(), vectors.get(0).getId(),
                        System.currentTimeMillis() - start);
            }

            buffer.clear();
            count.update(0);
        } else {
            buffer.add(value.getVector());
            count.update(count.value() + 1);
        }
    }
}
