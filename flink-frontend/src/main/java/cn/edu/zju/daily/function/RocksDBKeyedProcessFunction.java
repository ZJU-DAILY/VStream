package cn.edu.zju.daily.function;

import static cn.edu.zju.daily.data.DataSerializer.*;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import java.util.List;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Main insert & search process function.
 *
 * <p>为插入和搜索分别创建一个线程池，
 */
@Slf4j
public class RocksDBKeyedProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult> {

    private MapState<byte[], byte[]> mapState = null;

    private final long sortInterval;

    private long searchCount;

    private ExecutorService executor;

    private long lastSearchTs;

    public RocksDBKeyedProcessFunction(long sortInterval) {
        this.sortInterval = sortInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<byte[], byte[]> mapStateDescriptor =
                new MapStateDescriptor<>("vector-mapState", byte[].class, byte[].class);

        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        searchCount = 0;
        lastSearchTs = -1;
    }

    private void insert(FloatVector vector, int nodeId) throws Exception {

        if (vector.getId() % 15193 == 0) { // gcd(1z
            LOG.info(
                    "Partition {} (ts = {}): Inserting vector #{}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    vector.getEventTime(),
                    vector.getId());
        }
        byte[] id = new byte[Long.BYTES];
        byte[] array = new byte[vector.dim() * Float.BYTES + Long.BYTES];
        serializeFloatVectorWithTimestamp(vector, id, array);
        mapState.put(id, array);
    }

    private void delete(VectorDeletion marker, int nodeId) throws Exception {
        if (marker.getId() % 15193 == 0) { // gcd(1z
            LOG.info(
                    "Partition {}: Deleting vector #{}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    marker.getId());
        }
        byte[] id = new byte[Long.BYTES];
        serializeLong(marker.getId(), id);
        mapState.remove(id);
    }

    public void search(
            StreamRecord<PartitionedElement> query,
            int nodeId,
            int numPartitionsSent,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context context,
            Collector<SearchResult> out) {
        long partitionedAt = query.getValue().getPartitionedAt();
        FloatVector queryVector = query.getValue().getData().asVector();

        if (queryVector.getEventTime() < lastSearchTs) {
            return;
        }
        lastSearchTs = queryVector.getEventTime();

        searchCount++;
        long start = 0;
        byte[] array = new byte[queryVector.dim() * Float.BYTES + Long.BYTES];
        serializeFloatVectorWithTimestampAndTTL(queryVector, array);

        // XXX: bigger output interval
        start = System.currentTimeMillis();

        if (searchCount % sortInterval == 0) {
            try {
                mapState.get(new byte[1]); // trigger sort
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        byte[] resultBytes = null;
        try {
            resultBytes = mapState.get(array);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (searchCount % sortInterval == 0) {
            try {
                mapState.get(new byte[2]); // turn off sort
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        long current = System.currentTimeMillis();
        LOG.info(
                "Partition {} ({} ms since partition): Searching query #{} in {} ms",
                getRuntimeContext().getIndexOfThisSubtask(),
                (current - partitionedAt),
                queryVector.getId(),
                (current - start));

        SearchResult r =
                deserializeSearchResult(
                        resultBytes,
                        nodeId,
                        queryVector.getId(),
                        numPartitionsSent,
                        queryVector.getEventTime());

        out.collect(r);
    }

    private void dump(int nodeId) throws Exception {
        // Only for testing on mock db.
        List<Long> keys = deserializeLongList(mapState.get(new byte[0]));
        System.out.println("Partition " + nodeId + " dump: " + keys);
    }

    @Override
    public void processElement(
            PartitionedElement data,
            KeyedProcessFunction<Integer, PartitionedElement, SearchResult>.Context context,
            Collector<SearchResult> collector)
            throws Exception {

        int currentKey = context.getCurrentKey();
        if (currentKey != data.getPartitionId()) {
            throw new RuntimeException(
                    "Key mismatch: " + currentKey + " != " + data.getPartitionId());
        }

        Long currentTimestamp = context.timestamp();
        // reconstruct the record
        StreamRecord<PartitionedElement> record =
                (currentTimestamp != null)
                        ? new StreamRecord<>(data, currentTimestamp)
                        : new StreamRecord<>(data);

        if (data.getDataType() == PartitionedElement.DataType.QUERY) {
            search(record, currentKey, data.getNumPartitionsSent(), context, collector);
        } else if (data.getDataType() == PartitionedElement.DataType.INSERT_OR_DELETE) {
            if (data.getData().isDeletion()) {
                delete(data.getData().asDeletion(), currentKey);
            } else {
                insert(data.getData().asVector(), currentKey);
            }
        } else {
            dump(currentKey);
        }
    }
}
