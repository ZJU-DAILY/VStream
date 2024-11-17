package cn.edu.zju.daily.partitioner.lsh;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.curve.SpaceFillingCurve;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LSHashSpaceFillingPartitioner implements Serializable {

    private class HashFamilyRange {
        final int[] minValues;
        final int[] maxValues;
        long updateTime;
        final LinkedList<int[]> history; // last M hash value lists
        final SpaceFillingCurve curve;
        final List<BigInteger> partitionHeads;
        boolean firstInterval;
        boolean initialized;

        //  boolean relaxed = true;  todo: relax the query partition scheme

        HashFamilyRange(int numHashFunctions, boolean firstInterval) {
            minValues = new int[numHashFunctions];
            maxValues = new int[numHashFunctions];
            this.history = new LinkedList<>();
            this.partitionHeads = new ArrayList<>();
            this.updateTime = -1;
            curve =
                    curveBuilder
                            .setBits(numSpaceFillingBits)
                            .setDimension(numHashFunctions)
                            .build();

            if (firstInterval) {
                for (int i = 0; i < numHashFunctions; i++) {
                    minValues[i] = -128;
                    maxValues[i] = 127;
                }
                this.firstInterval = true;
                this.initialized = true;
                BigInteger length =
                        BigInteger.ONE
                                .shiftLeft(numSpaceFillingBits * numHashFunctions)
                                .divide(BigInteger.valueOf(numPartitions));

                for (int i = 0; i < numPartitions; i++) {
                    partitionHeads.add(length.multiply(BigInteger.valueOf(i)));
                }
            } else {
                for (int i = 0; i < numHashFunctions; i++) {
                    minValues[i] = Integer.MAX_VALUE;
                    maxValues[i] = Integer.MIN_VALUE;
                }
                this.initialized = false;
                this.firstInterval = false;
            }
        }

        /**
         * The range will be used to determine the partition of tuples from updateTime to updateTime
         * + updateInterval
         *
         * @param updateTime
         */
        void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }

        long[] toZOrderSpace(int[] hash) {
            if (!initialized) {
                throw new IllegalStateException("HashFamilyRange not initialized");
            }
            long[] array = new long[hash.length];
            for (int i = 0; i < hash.length; i++) {
                double percent = (double) (hash[i] - minValues[i]) / (maxValues[i] - minValues[i]);
                percent = Math.max(0, Math.min(1, percent));
                array[i] =
                        Math.min(
                                (long) (percent * (1L << numSpaceFillingBits)),
                                (1L << numSpaceFillingBits) - 1);
            }
            return array;
        }

        BigInteger zOrderValue(long[] hashInZOrderSpace) {
            if (hashInZOrderSpace.length == 0) {
                return BigInteger.ZERO;
            }
            return curve.index(hashInZOrderSpace);
        }

        int partition(BigInteger zOrderValue, int numPartitions) {
            if (!initialized) {
                throw new IllegalStateException("HashFamilyRange not initialized");
            }

            int index = Collections.binarySearch(partitionHeads, zOrderValue);
            if (index < 0) {
                index = -index - 2;
            }
            return Math.max(0, Math.min(numPartitions - 1, index));
        }

        void update(int[] hash) {
            if (initialized) {
                throw new IllegalStateException("HashFamilyRange already initialized");
            }
            history.addLast(hash);
            if (history.size() > maxRetainedElements) {
                history.removeFirst();
            }
        }

        void summarizeHistory() {
            List<BigInteger> hilbertValues = new ArrayList<>();
            int historySize = history.size();

            long startTime = System.currentTimeMillis();
            for (int[] hash : history) {
                for (int i = 0; i < hash.length; i++) {
                    minValues[i] = Math.min(minValues[i], hash[i]);
                    maxValues[i] = Math.max(maxValues[i], hash[i]);
                }
            }
            initialized = true;
            for (int[] hash : history) {
                hilbertValues.add(zOrderValue(toZOrderSpace(hash)));
            }
            Collections.sort(hilbertValues);
            for (int i = 0; i < numPartitions; i++) {
                int index = (int) Math.floor((double) hilbertValues.size() * i / numPartitions);
                partitionHeads.add(hilbertValues.get(index));
            }
            history.clear();

            LOG.info(
                    "History of range {} summarized in {} ms, new partition heads: {}",
                    updateTime,
                    System.currentTimeMillis() - startTime,
                    partitionHeads);
        }
    }

    private class TrackedHashFamily implements LSHashFamily {

        private final LSHashFamily hashFamily;
        private final LinkedList<HashFamilyRange> ranges;

        private HashFamilyRange rangeInEffect; // second to last range

        TrackedHashFamily(LSHashFamily hashFamily) {
            this.hashFamily = hashFamily;
            this.ranges = new LinkedList<>();
            rangeInEffect = null;
        }

        boolean isInitialized() {
            return rangeInEffect != null;
        }

        HashFamilyRange getRangeInEffect() {
            return rangeInEffect;
        }

        @Override
        public int[] hash(VectorData vector) {
            return hashFamily.hash(vector);
        }

        @Override
        public int getNumHashFunctions() {
            return hashFamily.getNumHashFunctions();
        }

        void createNewInterval(long updateTime) {

            // update time is the smallest timestamps that falls into the new interval

            // clear old ranges
            while (!ranges.isEmpty()
                    && ranges.getFirst().updateTime + updateInterval < updateTime - maxTTL) {
                ranges.removeFirst();
            }

            if (rangeInEffect == null) {
                // still uninitialized
                rangeInEffect = new HashFamilyRange(getNumHashFunctions(), true);
                rangeInEffect.setUpdateTime(updateTime);
                ranges.add(rangeInEffect);
                ranges.add(new HashFamilyRange(getNumHashFunctions(), false));
            } else {
                // OPTIM: if similar, do not create a new range
                rangeInEffect = ranges.getLast();
                rangeInEffect.setUpdateTime(updateTime);
                rangeInEffect.summarizeHistory();
                ranges.add(new HashFamilyRange(getNumHashFunctions(), false));
            }
        }
    }

    private final TrackedHashFamily hashFamily;
    private final long updateInterval;
    private long nextUpdateTime; // next update time, in milli epoch. -1 means uninitialized
    private final int numPartitions;
    private final int maxRetainedElements;
    private final long maxTTL;
    private final int numSpaceFillingBits;
    private final SpaceFillingCurve.Builder curveBuilder;

    /**
     * Constructor.
     *
     * @param numHashFunctions number of hash functions in a hash family
     * @param numSpaceFillingBits number of bits for hilbert curve
     * @param updateInterval interval for updating the partitioning strategy
     */
    public LSHashSpaceFillingPartitioner(
            int dim,
            int numHashFunctions,
            float hashWidth,
            int numSpaceFillingBits,
            long updateInterval,
            int maxRetainedElements,
            long maxTTL,
            int numPartitions,
            SpaceFillingCurve.Builder curveBuilder,
            Random random) {
        if (numHashFunctions * numSpaceFillingBits > 63) {
            LOG.warn(
                    "numHashFunctions * numHilbertBits > 63, cannot use small options for hilbert curve.");
        }

        this.hashFamily =
                new TrackedHashFamily(new L2HashFamily(dim, numHashFunctions, hashWidth, random));
        this.updateInterval = updateInterval;
        this.nextUpdateTime = -1;
        this.numPartitions = numPartitions;
        this.maxRetainedElements = maxRetainedElements;
        this.numSpaceFillingBits = numSpaceFillingBits;
        this.curveBuilder = curveBuilder;
        this.maxTTL = maxTTL;
    }

    public void initializeWith(Iterator<FloatVector> vectors, long ts) {
        if (hashFamily.isInitialized()) {
            throw new IllegalStateException("Partitioner already initialized");
        }
        long updateTime = Math.floorDiv(ts, updateInterval) * updateInterval;
        hashFamily.createNewInterval(updateTime - updateInterval);
        nextUpdateTime = updateTime + updateInterval;

        while (vectors.hasNext()) {
            FloatVector vector = vectors.next();
            // this will update the first range
            int[] hash = hashFamily.hash(vector);
            hashFamily.ranges.getLast().update(hash);
        }
        hashFamily.createNewInterval(updateTime);
        hashFamily.ranges.removeFirst();
    }

    public int getDataPartition(VectorData vector) {
        long ts = vector.getEventTime();

        // if this is the first range, the update interval is less than 10 minutes
        if (nextUpdateTime == -1) {
            long updateTime = Math.floorDiv(ts, updateInterval) * updateInterval;
            hashFamily.createNewInterval(updateTime);
            nextUpdateTime = ts + Math.min(5 * 60 * 1000, updateInterval); // <= 5 minutes
        }

        // initialize
        else if (ts > nextUpdateTime) {
            // if less than 3 minute before the next update, skip this time
            if (updateInterval > 180000 && ts + 180000 < nextUpdateTime) {
                LOG.info("Skipping update interval: {} -> {}", ts, nextUpdateTime);
            }

            long updateTime =
                    Math.max(nextUpdateTime, Math.floorDiv(ts, updateInterval) * updateInterval);
            hashFamily.createNewInterval(updateTime);
            nextUpdateTime = updateTime + updateInterval;
        }

        // 1. get hash family
        int[] hash = hashFamily.hash(vector);
        hashFamily.ranges.getLast().update(hash);

        //        LOG.info("hash: {} -> {}", vector.id(), Arrays.toString(hash));

        // 2. get hash in z-order space
        HashFamilyRange range = hashFamily.getRangeInEffect();
        long[] hashInZOrderSpace = range.toZOrderSpace(hash);

        //        LOG.info("hashInZOrderSpace: {} -> {}", vector.id(),
        // Arrays.toString(hashInZOrderSpace));

        // 3. get z-order value
        BigInteger zOrderValue = range.zOrderValue(hashInZOrderSpace);

        //        LOG.info("zOrderValue: {} -> {}", vector.id(), zOrderValue);

        // 4. get partition
        int partition = range.partition(zOrderValue, numPartitions);
        //        LOG.info("partition: {} -> {} (partition heads: {})", vector.id(), partition,
        // range.partitionHeads);
        return partition;
    }

    public List<Integer> getQueryPartition(FloatVector vector) {
        long ts = vector.getEventTime();
        long ttl = vector.getTTL();

        // 1. get hash family
        int[] hash = hashFamily.hash(vector);

        Set<Integer> partitionIds = new HashSet<>();
        Iterator<HashFamilyRange> iter = hashFamily.ranges.descendingIterator();
        if (iter.hasNext()) {
            iter.next(); // skip the range that is being updated
            while (iter.hasNext()) {
                HashFamilyRange range = iter.next();
                if (!isFresh(range, vector)) {
                    break;
                }
                partitionIds.addAll(getQueryPartition(hash, range));
            }
        }
        if (partitionIds.isEmpty()) {
            // there is no range, probably no data. Just send to a random partition, the search will
            // return null which is correct.
            partitionIds.add(Long.hashCode(vector.getId()) % numPartitions);
        }
        return new ArrayList<>(partitionIds);
    }

    private List<Integer> getQueryPartition(int[] hash, HashFamilyRange range) {

        // 2. get hash in z-order space
        long[] hashInZOrderSpace = range.toZOrderSpace(hash);

        // 3. get z-order value
        BigInteger zOrderValue = range.zOrderValue(hashInZOrderSpace);

        // 4. get partition
        return Collections.singletonList(range.partition(zOrderValue, numPartitions));
    }

    private boolean isFresh(HashFamilyRange range, FloatVector query) {
        return range.updateTime + updateInterval >= query.getEventTime() - query.getTTL();
    }
}
