package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.kmeans.NKMeans;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** 每个窗口做一次kMeans，下一个窗口的每个向量找到距离最近的若干个聚类中心发到对应的分区 */
@Slf4j
public class KMeansPartitionFunction extends RichPartitionFunction {

    private static final long UNINITIALIZED = -1;

    private final long windowSize;
    private final int numClusters;
    private final int replicationFactor;
    private final int maxHistorySize;
    private final int maxIter;

    private NKMeans nkMeans;
    private long lastUpdateTS;
    private Deque<double[]> history;
    private Random random;
    private PartitionToKeyMapper mapper;

    public KMeansPartitionFunction(
            long windowSize,
            int numClusters,
            int replicationFactor,
            int maxHistorySize,
            int maxIter) {
        this.windowSize = windowSize;
        this.numClusters = numClusters;
        this.replicationFactor = replicationFactor;
        this.maxHistorySize = maxHistorySize;
        this.maxIter = maxIter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        nkMeans = null;
        history = new ArrayDeque<>(maxHistorySize);
        random = new Random(38324);
        lastUpdateTS = UNINITIALIZED;
        mapper = new PartitionToKeyMapper(numClusters);
    }

    @Override
    public void flatMap1(VectorData value, Collector<PartitionedElement> out) throws Exception {
        long now = System.currentTimeMillis();
        updateWindowIfExpired(now);

        history.addLast(value.getDoubleValue());
        if (history.size() > maxHistorySize) {
            history.pollFirst();
        }
        if (nkMeans != null) {
            List<Integer> nearest = nkMeans.nearest(value.getDoubleValue(), replicationFactor);
            for (int i : nearest) {
                out.collect(new PartitionedData(mapper.getKey(i), value));
            }
        } else {
            int randomPartition = random.nextInt(numClusters);
            out.collect(new PartitionedData(mapper.getKey(randomPartition), value));
        }
    }

    @Override
    public void flatMap2(VectorData value, Collector<PartitionedElement> out) throws Exception {
        if (nkMeans != null) {
            List<Integer> nearest = nkMeans.nearest(value.getDoubleValue(), replicationFactor);
            for (int i : nearest) {
                out.collect(
                        new PartitionedQuery(mapper.getKey(i), nearest.size(), value.asVector()));
            }
        } else {
            for (int i = 0; i < numClusters; i++) {
                out.collect(new PartitionedQuery(mapper.getKey(i), numClusters, value.asVector()));
            }
        }
    }

    private void updateWindowIfExpired(long now) {
        if (lastUpdateTS == UNINITIALIZED) {
            lastUpdateTS = now;
        }
        if (now - lastUpdateTS > windowSize) {
            long start = System.currentTimeMillis();
            try {
                nkMeans = NKMeans.fit(history.toArray(new double[0][]), numClusters, maxIter);
            } catch (Exception e) {
                LOG.warn("Failed to fit kMeans, centroids not updated", e);
            } finally {
                LOG.info("Fitting kMeans took {} ms", System.currentTimeMillis() - start);
            }
            lastUpdateTS = now;
            history.clear();
        }
    }
}
