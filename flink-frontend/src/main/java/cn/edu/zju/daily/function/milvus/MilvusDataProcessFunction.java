package cn.edu.zju.daily.function.milvus;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import cn.edu.zju.daily.function.VectorKeyedDataProcessFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

/** Milvus insert function. */
@Slf4j
public class MilvusDataProcessFunction extends VectorKeyedDataProcessFunction {

    private final Parameters params;
    private MilvusUtil milvusUtil = null;

    public MilvusDataProcessFunction(Parameters params) {
        super(params.getMilvusInsertBufferCapacity());
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        milvusUtil = new MilvusUtil();
        milvusUtil.connect(params.getMilvusHost(), params.getMilvusPort());
    }

    @Override
    protected void flushInserts(List<FloatVector> pendingInserts) {
        long start = System.currentTimeMillis();
        String partitionName = Integer.toString(getRuntimeContext().getIndexOfThisSubtask());
        milvusUtil.insert(pendingInserts, params.getMilvusCollectionName(), partitionName, false);
        if (pendingInserts.size() > 1) {
            LOG.info(
                    "Partition {}: {} vectors (from #{}) inserted in {} ms.",
                    partitionName,
                    pendingInserts.size(),
                    pendingInserts.get(0).getId(),
                    System.currentTimeMillis() - start);
        } else {
            LOG.info(
                    "Partition {}: Vector #{} inserted in {} ms.",
                    partitionName,
                    pendingInserts.get(0).getId(),
                    System.currentTimeMillis() - start);
        }
    }

    @Override
    protected void flushDeletes(Map<Long, VectorDeletion> pendingDeletes) {
        long start = System.currentTimeMillis();
        String partitionName = Integer.toString(getRuntimeContext().getIndexOfThisSubtask());
        milvusUtil.delete(pendingDeletes.keySet(), params.getMilvusCollectionName(), partitionName);
        LOG.info(
                "Partition {}: {} vectors deleted in {} ms.",
                partitionName,
                pendingDeletes.size(),
                System.currentTimeMillis() - start);
    }
}
