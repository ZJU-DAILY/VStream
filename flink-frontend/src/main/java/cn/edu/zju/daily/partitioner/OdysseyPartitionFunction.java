package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.PartitionedQuery;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.sax.HotTracer;
import cn.edu.zju.daily.partitioner.sax.SAX;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * This class migrates the partition strategy in Odyssey to the stream task.
 *
 * <p>Each parallelism is designated to a replication group. Each replication group contains a set
 * of workers that are replicas of each other. In order to partition a data vector:
 *
 * <ul>
 *   <li>First, obtain the gray code of the data vector's SAX word.
 *   <li>If the SAX word is hot in the previous window (lambda controls the number of hot words), it
 *       is randomly assigned a group.
 *   <li>Otherwise, we first try to assign it to a group in the round-robin fashion according to the
 *       gray code.
 *   <li>If the chosen group is skewed in this window, we randomly assign it. If not, we assign it
 *       to this group.
 *   <li>The data is sent to every worker in the assigned group.
 * </ul>
 *
 * <p>To partition a query vector, we simply choose a worker in each group in the round-robin
 * fashion.
 */
@Slf4j
public class OdysseyPartitionFunction extends RichPartitionFunction {

    private final int numPartitions;
    private final int replicationFactor;
    private final int saxPaaSize;
    private final int saxWidth;
    private final long windowSize;
    private final float skewFactor;

    /** Number of largest SAX bins to divide. */
    private final int lambda;

    private PartitionToKeyMapper mapper;

    private List<List<Integer>> replicationGroups;
    private int[] groupIndices;

    private SAX sax;

    private HotTracer hotTracer;

    /** Hot SAX words in the previous window. */
    private Set<Integer> hotSAXWords;

    private long windowStartTs;
    private int[] groupCounts;
    private int lastWindowCount;

    private Random random;

    /**
     * Constructor
     *
     * @param numPartitions parallelism
     * @param replicationFactor size of each replication group. A replication group holds a set of
     *     partitions that are replicas of each other.
     */
    public OdysseyPartitionFunction(
            int numPartitions,
            int replicationFactor,
            int saxPaaSize,
            int saxWidth,
            int lambda,
            float skewFactor,
            long windowSize) {
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.saxPaaSize = saxPaaSize;
        this.saxWidth = saxWidth;
        this.lambda = lambda;
        this.skewFactor = skewFactor;
        this.windowSize = windowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.replicationGroups = createReplicationGroups(numPartitions, replicationFactor);
        this.sax = new SAX(saxPaaSize, saxWidth, 0d);
        this.hotTracer = new HotTracer(this.sax.getMaxWord(), lambda);
        this.hotSAXWords = new HashSet<>();
        this.windowStartTs = 0;
        this.groupCounts = new int[replicationGroups.size()];
        this.groupIndices = new int[replicationGroups.size()];
        this.lastWindowCount = 0;
        this.random = new Random(38324);
        this.mapper = new PartitionToKeyMapper(numPartitions);
    }

    @Override
    public void flatMap1(VectorData data, Collector<PartitionedElement> out) throws Exception {

        long now = System.currentTimeMillis();
        // initialize
        if (windowStartTs == 0) {
            windowStartTs = now;
        }

        if (now - windowStartTs > windowSize) {
            hotSAXWords = hotTracer.getHotKeys();
            hotTracer.clear();
            lastWindowCount = Arrays.stream(groupCounts).sum();
            LOG.info("New window, previous window imbalance: {}", getImbalanceBetweenWindows());
            Arrays.fill(groupCounts, 0);
            windowStartTs = now;
        }

        if (!data.hasValue()) {
            for (int i = 0; i < numPartitions; i++) {
                out.collect(new PartitionedData(i, data));
            }
        }
        int encode = sax.encode(data.getValue());

        int groupId;
        if (hotSAXWords.contains(encode)) {
            groupId = random.nextInt(replicationGroups.size());
        } else {
            groupId = Math.floorMod(encode, replicationGroups.size());
            if (isSkewed(groupId)) {
                groupId = random.nextInt(replicationGroups.size());
            }
        }

        // Send to each partition in this group
        List<Integer> partitions = replicationGroups.get(groupId);
        for (int partition : partitions) {
            out.collect(new PartitionedData(mapper.getKey(partition), data));
        }

        // bookkeeping
        groupCounts[groupId]++;
        hotTracer.add(encode);
    }

    @Override
    public void flatMap2(VectorData data, Collector<PartitionedElement> out) throws Exception {
        if (!data.hasValue()) {
            throw new RuntimeException("Query vector must have a value.");
        }
        // Round-robin each group
        for (int i = 0; i < replicationGroups.size(); i++) {
            int partition = getNextQueryPartition(i);
            out.collect(
                    new PartitionedQuery(
                            mapper.getKey(partition), replicationGroups.size(), data.asVector()));
        }
    }

    static List<List<Integer>> createReplicationGroups(int numPartitions, int replicationFactor) {
        int numGroups = Math.floorDiv(numPartitions, replicationFactor);
        int remainder = Math.floorMod(numPartitions, replicationFactor);
        int partitionId = 0;
        List<List<Integer>> groups = new ArrayList<>(numGroups);
        for (int groupId = 0; groupId < numGroups; groupId++) {
            int rf = groupId < remainder ? replicationFactor + 1 : replicationFactor;
            List<Integer> partitions = new ArrayList<>(rf);
            for (int i = 0; i < rf; i++) {
                partitions.add(partitionId);
                partitionId++;
            }
            groups.add(partitions);
        }
        return groups;
    }

    private int getNextQueryPartition(int groupId) {
        List<Integer> partitions = replicationGroups.get(groupId);
        int partition = partitions.get(groupIndices[groupId]);
        groupIndices[groupId] = (groupIndices[groupId] + 1) % partitions.size();
        return partition;
    }

    private boolean isSkewed(int groupId) {
        if (lastWindowCount == 0) {
            return false;
        }

        float average = (float) lastWindowCount / replicationGroups.size();
        return groupCounts[groupId] > average * skewFactor;
    }

    private float getImbalanceBetweenWindows() {
        int max = Arrays.stream(groupCounts).max().orElse(0);
        float mean = (float) lastWindowCount / replicationGroups.size();
        return (max - mean) / (mean + EPSILON);
    }

    private static final float EPSILON = 1e-6F;
}
