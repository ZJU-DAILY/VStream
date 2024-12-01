package cn.edu.zju.daily.function.qdrant;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import cn.edu.zju.daily.function.VectorKeyedDataProcessFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class QdrantDataProcessFunction extends VectorKeyedDataProcessFunction {

    private final Parameters params;
    private QdrantUtil util;
    private String collectionName;
    private int shard;

    public QdrantDataProcessFunction(Parameters params) {
        super(params.getQdrantInsertBatchSize());
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.util = new QdrantUtil(params.getQdrantHost(), params.getQdrantPort());
        this.collectionName = params.getQdrantCollectionName();
        this.shard = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    protected void flushInserts(List<FloatVector> pendingInserts) {
        long start = System.currentTimeMillis();
        util.insert(collectionName, pendingInserts, shard);
        LOG.info(
                "Partition {}: {} vectors (from #{}) inserted in {} ms.",
                getRuntimeContext().getIndexOfThisSubtask(),
                pendingInserts.size(),
                pendingInserts.get(0).getId(),
                System.currentTimeMillis() - start);
    }

    @Override
    protected void flushDeletes(Map<Long, VectorDeletion> pendingDeletes) {
        long start = System.currentTimeMillis();
        util.delete(collectionName, pendingDeletes.keySet(), shard);
        LOG.info(
                "Partition {}: {} vectors deleted in {} ms.",
                getRuntimeContext().getIndexOfThisSubtask(),
                pendingDeletes.size(),
                System.currentTimeMillis() - start);
    }

    @Override
    public void close() throws Exception {
        super.close();
        util.close();
    }
}
