package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public abstract class VectorKeyedDataProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, Object>
        implements CheckpointedFunction {

    private final int batchSize;
    private List<FloatVector> pendingInserts;
    private Map<Long, VectorDeletion> pendingDeletes;

    public VectorKeyedDataProcessFunction(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pendingInserts = new LinkedList<>();
        pendingDeletes = new HashMap<>();
    }

    @Override
    public void processElement(
            PartitionedElement value,
            KeyedProcessFunction<Integer, PartitionedElement, Object>.Context ctx,
            Collector<Object> out)
            throws Exception {

        if (value.getDataType() == PartitionedElement.DataType.INSERT_OR_DELETE) {
            VectorData data = value.getData();
            if (data.isDeletion()) {
                // process deletions
                VectorDeletion marker = data.asDeletion();
                VectorDeletion existingMarker = pendingDeletes.get(marker.getId());
                if (existingMarker != null
                        && existingMarker.getEventTime() >= marker.getEventTime()) {
                    return;
                }
                pendingDeletes.put(marker.getId(), marker);
                if (pendingDeletes.size() >= batchSize) {
                    performLocalDeletes();
                    flushDeletes(pendingDeletes);
                    pendingDeletes.clear();
                }
            } else {
                // process inserts
                pendingInserts.add(data.asVector());
                if (pendingInserts.size() >= batchSize) {
                    performLocalDeletes();
                    flushInserts(pendingInserts);
                    pendingInserts.clear();
                }
            }
        } else {
            throw new RuntimeException("Unsupported data type: " + value.getDataType());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        performLocalDeletes();
        if (!pendingInserts.isEmpty()) {
            flushInserts(pendingInserts);
        }
        if (!pendingDeletes.isEmpty()) {
            flushDeletes(pendingDeletes);
        }
    }

    private void performLocalDeletes() {
        Iterator<FloatVector> it = pendingInserts.iterator();
        while (it.hasNext()) {
            FloatVector vector = it.next();
            VectorDeletion marker = pendingDeletes.get(vector.getId());
            if (marker != null && marker.getEventTime() > vector.getEventTime()) {
                it.remove();
            }
        }
    }

    protected abstract void flushInserts(List<FloatVector> pendingInserts);

    protected abstract void flushDeletes(Map<Long, VectorDeletion> pendingDeletes);
}
