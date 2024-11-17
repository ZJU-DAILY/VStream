package cn.edu.zju.daily.util;

import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormat;
import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormatAdaptor;
import cn.edu.zju.daily.data.source.rate.StagedRateControllerBuilder;
import cn.edu.zju.daily.partitioner.LSHProximityPartitionFunction;
import cn.edu.zju.daily.partitioner.LSHWithSpaceFillingPartitionFunction;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import lombok.Data;
import org.rocksdb.InfoLogLevel;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * Parameters regarding Flink vector search job.
 *
 * @author auroflow
 */
public @Data class Parameters implements Serializable {

    // ===============================
    // FLINK CLUSTER INFORMATION
    // ===============================

    private String flinkJobManagerHost;
    private int flinkJobManagerPort;
    private int parallelism;
    private int reduceParallelism;

    // ===============================
    // DATASETS
    // ===============================

    private int vectorDim;

    // ----
    // HDFS
    // ----
    /** HDFS address. Format: {@code hdfs://host:port}. */
    private String hdfsAddress;

    private String hdfsUser;

    /** Source dataset path on HDFS. Supports .txt, .fvecs or .bvecs extensions. */
    private String sourcePath;

    /** Query dataset path on HDFS. Supports .txt, .fvecs or .bvecs extensions. */
    private String queryPath;

    /** Local ground truth .ivecs file path. For testing only. */
    private String groundTruthPath;

    /** File sink directory path on HDFS. */
    private String fileSinkPath;

    // -------
    // Inserts
    // -------
    /** Number of vectors to skip before each insert loop. */
    private int insertSkip;

    /** Number of vectors to insert per loop. */
    private int insertLimitPerLoop;

    /** Number of insert loops. */
    private int insertLoops;

    /**
     * Number of vectors to read in each bulk during insertion. Used in {@link
     * FloatVectorBinaryInputFormatAdaptor}.
     */
    private int insertReadBulkSize;

    /**
     * Starting vector count for each insert period. The first threshold should be zero. Used in
     * {@link StagedRateControllerBuilder}.
     */
    private List<Long> insertThrottleThresholds;

    /** Insert rates for each insert period. Used in {@link StagedRateControllerBuilder}. */
    private List<Long> insertRates;

    /**
     * The insert rates for each insert period observed by {@link
     * LSHWithSpaceFillingPartitionFunction}.
     */
    private List<Long> observedInsertRates;

    /**
     * Whether wait for index to build before switching to the next insert rate. Supports "none",
     * "milvus" and "qdrant".
     */
    private String waitingIndexStrategy;

    /**
     * If {@link Parameters#waitingIndexStrategy} is not "none", the ratios to wait for. Should be
     * of the same length as {@link Parameters#insertRates}.
     */
    private List<Float> indexWaitRatios;

    /**
     * Deletion ratio in the insert stream. Should be between [0, 1). Used in {@link
     * FloatVectorBinaryInputFormat}.
     */
    private double deleteRatio;

    // -------
    // Queries
    // -------
    /** Query throttle mode. Supported modes: {@code bind-insert} or {@code staged}. */
    private String queryThrottleMode;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code bind-insert}, this is the insert count when
     * the query throttler will switch from the old query rate to the new query rate.
     */
    private long queryThrottleInsertThreshold;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code bind-insert}, this is the initial query
     * rate.
     */
    private long initialQueryRate;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code bind-insert}, this is the new query rate.
     */
    private long newQueryRate;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code bind-insert}, this is the HDFS path where
     * the insert throttler writes the new query rate to and the query throttler polls from.
     */
    private String queryRatePollingPath;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code staged}, this is the starting vector count
     * for each query period. The first threshold should be zero. Used in {@link
     * StagedRateControllerBuilder}.
     */
    private List<Long> queryThrottleThresholds;

    /**
     * If {@link Parameters#queryThrottleMode} is {@code staged}, this is the initial query rate
     * before insert count reaches {@link Parameters#queryThrottleInsertThreshold}.
     */
    private List<Long> queryRates;

    /**
     * Number of queries to read in each bulk during query. Used in {@link
     * FloatVectorBinaryInputFormat}.
     */
    private int queryReadBulkSize;

    /** Number of times to loop the query dataset. */
    private int queryLoops;

    /** The maximum query TTL. */
    private long maxTTL;

    /** Metric type. Supports {@code L2} and {@code IP}. */
    private String metricType;

    // =================
    // Partitioners
    // =================

    /** Partitioner name. */
    private String partitioner;

    // ---
    // LSH
    // ---
    /** Number of bits to represent each dimension of the LSH space. */
    private int lshNumSpaceFillingBits; // for lsh+hilbert and lsh+zorder

    /** Interval to update the grid for LSH partitioning. */
    private long lshPartitionUpdateInterval; // for lsh+hilbert

    /**
     * Maximum number of elements retained in the Hilbert partitioner for calculating the new grid.
     */
    private int lshHilbertMaxRetainedElements; // for lsh+hilbert

    /** Number of LSH families. */
    private int lshNumFamilies; // for lsh+hilbert

    /** Number of hash functions in a hash family. */
    private int lshNumHashes;

    /** LSH bucket width. This is relevant to the dataset. */
    private float lshBucketWidth;

    /** LSH proximity for {@link LSHProximityPartitionFunction}. */
    private int proximity;

    // -------
    // Odyssey
    // -------
    /** Number of elements in the SAX representation. */
    private int odysseySaxPaaSize;

    /** The bit width of each element in the SAX representation. */
    private int odysseySaxWidth;

    /** Window size in milliseconds. */
    private long odysseyWindowSize;

    /**
     * Maximum allowed skew factor. Replication groups whose workload exceed this factor will have
     * their new data randomly assigned to other groups.
     */
    private float odysseySkewFactor;

    /**
     * Number of largest SAX bins which are divided across the replication groups. The rest are sent
     * to one replication group, if that group is not skewed.
     */
    private int odysseyLambda;

    /**
     * Number of parallelisms in each replication group. Each parallelism in the same group receives
     * the same data.
     */
    private int odysseyReplicationFactor;

    // ------
    // KMeans
    // ------
    /** KMeans window size in milliseconds. */
    private int kmeansWindowSize;

    /** KMeans replication factor (number of partitions each data and query is sent to). */
    private int kmeansReplicationFactor;

    /** KMeans max number of samples to keep for calculating the new centroids. */
    private int kmeansMaxHistorySize;

    private int kmeansMaxIter;

    // ====================
    // Indexing
    // ====================

    // ----
    // HNSW
    // ----

    /** HNSW M. */
    private int hnswM;

    /** HNSW efConstruction. */
    private int hnswEfConstruction;

    /** HNSW efSearch. */
    private int hnswEfSearch;

    /** Number of nearest neighbours to return. */
    private int k;

    // =================
    // Backends
    // =================

    // -----------------------
    // VStream (RocksDB-based)
    // -----------------------
    private String rocksDBStoragePrefix;

    /**
     * Max elements per HNSW table. Currently, the memtable will be flushed when it reaches this
     * limit.
     */
    private int rocksDBMaxElementsPerHnswTable;

    /** Max SSTable size, i.e. write buffer size, in bytes. */
    private long rocksDBSSTableSize;

    /** Data block size in bytes. */
    private long rocksDBBlockSize;

    /** Block cache size in bytes, which caches data blocks and vectors. */
    private long rocksDBBlockCacheSize;

    /** Termination weight. */
    private float rocksDBTerminationWeight;

    /** Termination factor. */
    private float rocksDBTerminationFactor;

    /** Termination threshold. */
    private float rocksDBTerminationThreshold;

    /** The minimum ratio of sstables that must be searched before termination. */
    private float rocksDBTerminationLowerBound;

    /** Trigger SSTable sort every sortInterval queries. */
    private long sortInterval;

    /** Num of memtables before flush. */
    private int rocksDBFlushThreshold;

    /** Num of memtables before write stall. */
    private int rocksDBMaxWriteBufferNumber;

    /** Max number of background flush threads on a single task manager. */
    private int rocksDBMaxBackgroundJobs;

    /**
     * Block restart interval, i.e. number of vectors between restart points for Gorilla compression
     * of vectors.
     */
    private int rocksDBBlockRestartInterval;

    /** Whether to skip sstables during search. */
    private boolean rocksDBSkipSST;

    private InfoLogLevel rocksDBInfoLogLevel;

    // ------
    // Milvus
    // ------
    private String milvusCollectionName;
    private String milvusHost;
    private int milvusPort;
    private int milvusNumShards;
    private int milvusInsertBufferCapacity;
    private int milvusQueryBufferCapacity;

    // --------
    // ChromaDB
    // --------
    /** ChromaDB collection name prefix. The full name is {@code prefix_<subtask index>}. */
    private String chromaCollectionNamePrefix;

    /**
     * ChromaDB addresses on all task managers. This is a multi-line file, each line in the format
     * of {@code host:port_low:port_high}, where {@code host} is a task manager. On each host,
     * {@code port_high - port_low + 1} ChromaDB instances are expected to be listening on ports
     * from {@code port_low} to {@code port_high}.
     */
    private String chromaAddressFile;

    /**
     * Whether to use multiple ChromaDB instances on each task manager. If true, all subtasks use
     * the first Chroma instances specified in {@link Parameters#chromaAddressFile} for that TM. If
     * false, each subtask uses one instance, in which case there should be enough ChromaDB
     * instances to cover the subtasks running on that TM.
     */
    private boolean chromaMultiInstancesOnTM;

    private int chromaInsertBatchSize;
    private int chromaQueryBatchSize;

    // ChromaDB HNSW parameters
    private int chromaHnswBatchSize;
    private int chromaHnswSyncThreshold;
    private double chromaHnswResizeFactor;
    private int chromaHnswNumThreads;

    /** Whether to remove the ChromaDB data before starting the job. */
    private boolean chromaClearData;

    // ------
    // Qdrant
    // ------
    private String qdrantHost;
    private int qdrantPort;
    private String qdrantCollectionName;

    private int qdrantInsertBatchSize;
    private int qdrantQueryBatchSize;

    // ===============================
    // CONSTRUCTORS WITH VALIDATIONS
    // ===============================

    public void setInsertThrottleThresholds(List<Long> insertThrottleThresholds) {
        if (!insertThrottleThresholds.get(0).equals(0L)) {
            throw new RuntimeException("The first insert threshold should be zero.");
        }
        this.insertThrottleThresholds = insertThrottleThresholds;
    }

    public void setQueryThrottleMode(String queryThrottleMode) {
        if (!"bind-insert".equals(queryThrottleMode) && !"staged".equals(queryThrottleMode)) {
            throw new RuntimeException(
                    "Supported query throttle modes: bind-insert or staged, given "
                            + queryThrottleMode);
        }
        this.queryThrottleMode = queryThrottleMode;
    }

    public void setQueryThrottleThresholds(List<Long> queryThrottleThresholds) {
        if (!queryThrottleThresholds.get(0).equals(0L)) {
            throw new RuntimeException("The first query threshold should be zero.");
        }
        this.queryThrottleThresholds = queryThrottleThresholds;
    }

    public void setChromaClearData(boolean chromaClearData) {
        if (!chromaClearData) {
            throw new RuntimeException("ChromaDB data should be cleared before running the job.");
        }
        this.chromaClearData = true;
    }

    // ===================
    // YAML LOADER
    // ===================

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
}
