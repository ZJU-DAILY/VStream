package cn.edu.zju.daily.util;

import cn.edu.zju.daily.function.ChromaDBKeyedProcessFunction;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * Parameters regarding Flink vector search job.
 *
 * @author auroflow
 */
public class Parameters implements Serializable {

    public static Parameters load(String path, boolean isResource) {
        try {
            Yaml yaml = new Yaml(new Constructor(Parameters.class, new LoaderOptions()));
            String url = null;
            if (isResource) {
                if (path.startsWith("/")) {
                    url = path;
                } else {
                    url = "/" + path;
                }
                System.out.println("Reading params from resource " + url);
                return yaml.load(Parameters.class.getResourceAsStream(url));
            } else {
                System.out.println("Reading params from file " + path);
                return yaml.load(Files.newInputStream(Paths.get(path)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String flinkJobManagerHost;
    private int flinkJobManagerPort;
    private String hdfsAddress;
    private String hdfsUser;
    private String sourcePath;
    private String queryPath;
    private int vectorDim;
    private List<Long> insertThrottleThresholds;
    private List<Long> insertRates;
    private List<Long> observedInsertRates;
    private List<Long> queryThrottleThresholds;
    private List<Long> queryRates;
    private int queryExecuteLoop;
    private int parallelism;
    private int reduceParallelism;
    private int numCopies;
    private long maxTTL;
    private String metricType;
    private int lshNumHilbertBits; // for lsh+hilbert
    private long lshPartitionUpdateInterval; // for lsh+hilbert
    private int lshHilbertMaxRetainedElements; // for lsh+hilbert
    private int lshNumFamilies; // for lsh+hilbert
    private int lshNumHashes;
    private float lshBucketWidth;
    private int hnswM;
    private int hnswEfConstruction;
    private int hnswEfSearch;
    private int k;
    private int rocksDBMaxElementsPerHnswTable;
    private long rocksDBSSTableSize;
    private long rocksDBBlockSize;
    private long rocksDBBlockCacheSize;
    private float rocksDBTerminationWeight;
    private float rocksDBTerminationFactor;
    private float rocksDBTerminationThreshold;
    private float rocksDBTerminationLowerBound;
    private String groundTruthPath;
    private String fileSinkPath;
    private long sortInterval;
    private String partitioner;
    private int proximity;
    private int rocksDBFlushThreshold;
    private int rocksDBMaxWriteBufferNumber;
    private int rocksDBMaxBackgroundJobs;
    private int rocksDBBlockRestartInterval;
    private String milvusCollectionName;
    private String milvusHost;
    private int milvusPort;
    private int milvusNumShards;
    private int milvusInsertBufferCapacity;
    private int milvusQueryBufferCapacity;
    private String chromaCollectionName;
    private String chromaAddressFile;
    private int chromaInsertBatchSize;
    private int chromaHnswBatchSize;
    private boolean chromaClearData;
    private int chromaQueryBatchSize;

    /** Flink job manager host. */
    public String getFlinkJobManagerHost() {
        return flinkJobManagerHost;
    }

    public void setFlinkJobManagerHost(String flinkJobManagerHost) {
        this.flinkJobManagerHost = flinkJobManagerHost;
    }

    /** Flink job manager port. */
    public int getFlinkJobManagerPort() {
        return flinkJobManagerPort;
    }

    public void setFlinkJobManagerPort(int flinkJobManagerPort) {
        this.flinkJobManagerPort = flinkJobManagerPort;
    }

    /**
     * HDFS address.
     *
     * @return HDFS address
     */
    public String getHdfsAddress() {
        return hdfsAddress;
    }

    public void setHdfsAddress(String hdfsAddress) {
        this.hdfsAddress = hdfsAddress;
    }

    /**
     * HDFS user.
     *
     * @return HDFS user
     */
    public String getHdfsUser() {
        return hdfsUser;
    }

    public void setHdfsUser(String hdfsUser) {
        this.hdfsUser = hdfsUser;
    }

    /**
     * Source vector path on HDFS.
     *
     * @return source vector path
     */
    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    /**
     * Query vector path on HDFS.
     *
     * @return query vector path
     */
    public String getQueryPath() {
        return queryPath;
    }

    public void setQueryPath(String queryPath) {
        this.queryPath = queryPath;
    }

    /**
     * The vector dimension.
     *
     * @return the vector dimension
     */
    public int getVectorDim() {
        return vectorDim;
    }

    public void setVectorDim(int vectorDim) {
        this.vectorDim = vectorDim;
    }

    public List<Long> getInsertThrottleThresholds() {
        return insertThrottleThresholds;
    }

    public void setInsertThrottleThresholds(List<Long> insertThrottleThresholds) {
        if (!insertThrottleThresholds.get(0).equals(0L)) {
            throw new RuntimeException("The first threshold should be zero.");
        }
        this.insertThrottleThresholds = insertThrottleThresholds;
    }

    public List<Long> getInsertRates() {
        return insertRates;
    }

    public void setInsertRates(List<Long> insertRates) {
        this.insertRates = insertRates;
    }

    public List<Long> getObservedInsertRates() {
        return observedInsertRates;
    }

    public void setObservedInsertRates(List<Long> observedInsertRates) {
        this.observedInsertRates = observedInsertRates;
    }

    public List<Long> getQueryThrottleThresholds() {
        return queryThrottleThresholds;
    }

    public void setQueryThrottleThresholds(List<Long> queryThrottleThresholds) {
        if (!queryThrottleThresholds.get(0).equals(0L)) {
            throw new RuntimeException("The first threshold should be zero.");
        }
        this.queryThrottleThresholds = queryThrottleThresholds;
    }

    public List<Long> getQueryRates() {
        return queryRates;
    }

    public void setQueryRates(List<Long> queryRates) {
        this.queryRates = queryRates;
    }

    /**
     * How many times the queries are performed.
     *
     * @return query execute loop
     */
    public int getQueryExecuteLoop() {
        return queryExecuteLoop;
    }

    public void setQueryExecuteLoop(int queryExecuteLoop) {
        this.queryExecuteLoop = queryExecuteLoop;
    }

    /**
     * Flink's parallelism (i.e. number of RocksDB connections)
     *
     * @return parallelism
     */
    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    /** Reduce parallelism. */
    public int getReduceParallelism() {
        return reduceParallelism;
    }

    public void setReduceParallelism(int reduceParallelism) {
        this.reduceParallelism = reduceParallelism;
    }

    /**
     * Number of partitions that a vector or a query is sent to.
     *
     * @return number of copies
     */
    public int getNumCopies() {
        return numCopies;
    }

    public void setNumCopies(int numCopies) {
        this.numCopies = numCopies;
    }

    public long getMaxTTL() {
        return maxTTL;
    }

    public void setMaxTTL(long maxTTL) {
        this.maxTTL = maxTTL;
    }

    /**
     * Metric type.
     *
     * @return metric type
     */
    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public int getLshHilbertMaxRetainedElements() {
        return lshHilbertMaxRetainedElements;
    }

    public void setLshHilbertMaxRetainedElements(int lshHilbertMaxRetainedElements) {
        this.lshHilbertMaxRetainedElements = lshHilbertMaxRetainedElements;
    }

    public int getLshNumHilbertBits() {
        return lshNumHilbertBits;
    }

    public void setLshNumHilbertBits(int lshNumHilbertBits) {
        this.lshNumHilbertBits = lshNumHilbertBits;
    }

    public long getLshPartitionUpdateInterval() {
        return lshPartitionUpdateInterval;
    }

    public void setLshPartitionUpdateInterval(long lshPartitionUpdateInterval) {
        this.lshPartitionUpdateInterval = lshPartitionUpdateInterval;
    }

    public int getLshNumFamilies() {
        return lshNumFamilies;
    }

    public void setLshNumFamilies(int lshNumFamilies) {
        this.lshNumFamilies = lshNumFamilies;
    }

    /**
     * Number of hash functions in a hash family.
     *
     * @return number of hash functions
     */
    public int getLshNumHashes() {
        return lshNumHashes;
    }

    public void setLshNumHashes(int lshNumHashes) {
        this.lshNumHashes = lshNumHashes;
    }

    /**
     * LSH bucket width.
     *
     * @return bucket width
     */
    public float getLshBucketWidth() {
        return lshBucketWidth;
    }

    public void setLshBucketWidth(float lshBucketWidth) {
        this.lshBucketWidth = lshBucketWidth;
    }

    // HNSW related parameters

    public int getHnswM() {
        return hnswM;
    }

    public void setHnswM(int hnswM) {
        this.hnswM = hnswM;
    }

    public int getHnswEfConstruction() {
        return hnswEfConstruction;
    }

    public void setHnswEfConstruction(int hnswEfConstruction) {
        this.hnswEfConstruction = hnswEfConstruction;
    }

    public int getHnswEfSearch() {
        return hnswEfSearch;
    }

    public void setHnswEfSearch(int hnswEfSearch) {
        this.hnswEfSearch = hnswEfSearch;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    // RocksDB related parameters

    /**
     * Max elements per HNSW table. Currently, the memtable will be flushed when it reaches this
     * limit.
     *
     * @return max elements per HNSW table
     */
    public int getRocksDBMaxElementsPerHnswTable() {
        return rocksDBMaxElementsPerHnswTable;
    }

    public void setRocksDBMaxElementsPerHnswTable(int rocksDBMaxElementsPerHnswTable) {
        this.rocksDBMaxElementsPerHnswTable = rocksDBMaxElementsPerHnswTable;
    }

    /**
     * Max SSTable size, i.e. write buffer size.
     *
     * @return sstable size
     */
    public long getRocksDBSSTableSize() {
        return rocksDBSSTableSize;
    }

    public void setRocksDBSSTableSize(long rocksDBSSTableSize) {
        this.rocksDBSSTableSize = rocksDBSSTableSize;
    }

    /**
     * Data block size.
     *
     * @return data block size
     */
    public long getRocksDBBlockSize() {
        return rocksDBBlockSize;
    }

    public void setRocksDBBlockSize(long rocksDBBlockSize) {
        this.rocksDBBlockSize = rocksDBBlockSize;
    }

    /**
     * Termination weight.
     *
     * @return termination weight
     */
    public float getRocksDBTerminationWeight() {
        return rocksDBTerminationWeight;
    }

    public void setRocksDBTerminationWeight(float rocksDBTerminationWeight) {
        this.rocksDBTerminationWeight = rocksDBTerminationWeight;
    }

    /**
     * Termination factor.
     *
     * @return termination factor
     */
    public float getRocksDBTerminationFactor() {
        return rocksDBTerminationFactor;
    }

    public void setRocksDBTerminationFactor(float rocksDBTerminationFactor) {
        this.rocksDBTerminationFactor = rocksDBTerminationFactor;
    }

    /**
     * Termination threshold.
     *
     * @return termination threshold
     */
    public float getRocksDBTerminationThreshold() {
        return rocksDBTerminationThreshold;
    }

    public void setRocksDBTerminationThreshold(float rocksDBTerminationThreshold) {
        this.rocksDBTerminationThreshold = rocksDBTerminationThreshold;
    }

    /**
     * The minimum ratio of sstables that must be searched before termination.
     *
     * @return termination lower bound
     */
    public float getRocksDBTerminationLowerBound() {
        return rocksDBTerminationLowerBound;
    }

    public void setRocksDBTerminationLowerBound(float rocksDBTerminationLowerBound) {
        this.rocksDBTerminationLowerBound = rocksDBTerminationLowerBound;
    }

    /** Size of block cache in bytes, which caches data blocks and vectors. */
    public long getRocksDBBlockCacheSize() {
        return rocksDBBlockCacheSize;
    }

    public void setRocksDBBlockCacheSize(long rocksDBBlockCacheSize) {
        this.rocksDBBlockCacheSize = rocksDBBlockCacheSize;
    }

    /**
     * Local ground truth .ivecs file path. For testing only.
     *
     * @return ground truth path
     */
    public String getGroundTruthPath() {
        return groundTruthPath;
    }

    public void setGroundTruthPath(String groundTruthPath) {
        this.groundTruthPath = groundTruthPath;
    }

    /**
     * File sink directory path on HDFS.
     *
     * @return file sink path
     */
    public String getFileSinkPath() {
        return fileSinkPath;
    }

    public void setFileSinkPath(String fileSinkPath) {
        this.fileSinkPath = fileSinkPath;
    }

    /**
     * Trigger SSTable sort every sortInterval queries.
     *
     * @return sort interval
     */
    public long getSortInterval() {
        return sortInterval;
    }

    public void setSortInterval(long sortInterval) {
        this.sortInterval = sortInterval;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    /**
     * LSH proximity.
     *
     * @return LSH proximity
     */
    public int getProximity() {
        return proximity;
    }

    public void setProximity(int proximity) {
        this.proximity = proximity;
    }

    /**
     * Num of memtables before flush.
     *
     * @return flush threshold
     */
    public int getRocksDBFlushThreshold() {
        return rocksDBFlushThreshold;
    }

    public void setRocksDBFlushThreshold(int rocksDBFlushThreshold) {
        this.rocksDBFlushThreshold = rocksDBFlushThreshold;
    }

    /** Num of memtables before write stall. */
    public int getRocksDBMaxWriteBufferNumber() {
        return rocksDBMaxWriteBufferNumber;
    }

    public void setRocksDBMaxWriteBufferNumber(int rocksDBMaxWriteBufferNumber) {
        this.rocksDBMaxWriteBufferNumber = rocksDBMaxWriteBufferNumber;
    }

    /** Max number of background flush threads on a single task manager. */
    public int getRocksDBMaxBackgroundJobs() {
        return rocksDBMaxBackgroundJobs;
    }

    public void setRocksDBMaxBackgroundJobs(int rocksDBMaxBackgroundJobs) {
        this.rocksDBMaxBackgroundJobs = rocksDBMaxBackgroundJobs;
    }

    /**
     * Block restart interval, i.e. number of vectors between restart points for Gorilla compression
     * of vectors.
     */
    public int getRocksDBBlockRestartInterval() {
        return rocksDBBlockRestartInterval;
    }

    public void setRocksDBBlockRestartInterval(int rocksDBBlockRestartInterval) {
        this.rocksDBBlockRestartInterval = rocksDBBlockRestartInterval;
    }

    /** Milvus collection name. */
    public String getMilvusCollectionName() {
        return milvusCollectionName;
    }

    public void setMilvusCollectionName(String milvusCollectionName) {
        this.milvusCollectionName = milvusCollectionName;
    }

    /** Milvus host. */
    public String getMilvusHost() {
        return milvusHost;
    }

    public void setMilvusHost(String milvusHost) {
        this.milvusHost = milvusHost;
    }

    /** Milvus port. */
    public int getMilvusPort() {
        return milvusPort;
    }

    public void setMilvusPort(int milvusPort) {
        this.milvusPort = milvusPort;
    }

    public int getMilvusNumShards() {
        return milvusNumShards;
    }

    public void setMilvusNumShards(int milvusNumShards) {
        this.milvusNumShards = milvusNumShards;
    }

    public int getMilvusInsertBufferCapacity() {
        return milvusInsertBufferCapacity;
    }

    public void setMilvusInsertBufferCapacity(int milvusInsertBufferCapacity) {
        this.milvusInsertBufferCapacity = milvusInsertBufferCapacity;
    }

    public int getMilvusQueryBufferCapacity() {
        return this.milvusQueryBufferCapacity;
    }

    public void setMilvusQueryBufferCapacity(int milvusQueryBufferCapacity) {
        this.milvusQueryBufferCapacity = milvusQueryBufferCapacity;
    }

    // =========================
    // ChromaDB related parameters
    // =========================

    /**
     * A list of ChromaDB addresses such as "localhost:8000" for the {@link
     * ChromaDBKeyedProcessFunction} to choose from. The list size should be at least equal to the
     * combiner parallelism. Each subtask will choose an address sequentially from the list.
     */
    public void setChromaAddressFile(String chromaAddressFile) {
        this.chromaAddressFile = chromaAddressFile;
    }

    public String getChromaAddressFile() {
        return chromaAddressFile;
    }

    /** ChromaDB collection name. */
    public String getChromaCollectionName() {
        return chromaCollectionName;
    }

    public void setChromaCollectionName(String chromaCollectionName) {
        this.chromaCollectionName = chromaCollectionName;
    }

    /** ChromaDB insert batch size. */
    public int getChromaInsertBatchSize() {
        return chromaInsertBatchSize;
    }

    public void setChromaInsertBatchSize(int chromaInsertBatchSize) {
        this.chromaInsertBatchSize = chromaInsertBatchSize;
    }

    /**
     * ChromaDB HNSW batch size. This is the frequency Chroma Brute index is flushed to the
     * in-memory HNSW index.
     */
    public int getChromaHnswBatchSize() {
        return chromaHnswBatchSize;
    }

    public void setChromaHnswBatchSize(int chromaHnswBatchSize) {
        this.chromaHnswBatchSize = chromaHnswBatchSize;
    }

    /** Whether to clear data in ChromaDB before running the job. */
    public boolean isChromaClearData() {
        return chromaClearData;
    }

    public void setChromaClearData(boolean chromaClearData) {
        if (!chromaClearData) {
            throw new RuntimeException("ChromaDB data should be cleared before running the job.");
        }
        this.chromaClearData = true;
    }

    public int getChromaQueryBatchSize() {
        return chromaQueryBatchSize;
    }

    public void setChromaQueryBatchSize(int chromaQueryBatchSize) {
        this.chromaQueryBatchSize = chromaQueryBatchSize;
    }
}
