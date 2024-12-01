package cn.edu.zju.daily.data.source;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.rate.VectorDataThrottler;
import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormat;
import cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormatAdaptor;
import cn.edu.zju.daily.data.source.format.FloatVectorInputFormat;
import cn.edu.zju.daily.data.source.rate.*;
import cn.edu.zju.daily.data.vector.HDFSVectorParser;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.partitioner.PartitionFunction;
import cn.edu.zju.daily.util.Parameters;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/** Create vector source with designated rate. */
public class HDFSVectorSourceBuilder {

    private static final long QUERY_POLLING_INTERVAL_MILLIS = 10000L;
    private static final String DEFAULT_MILVUS_INDEX_NAME = "embedding";

    private final StreamExecutionEnvironment env;
    private final Parameters params;
    private final HDFSVectorParser parser = new HDFSVectorParser();

    public HDFSVectorSourceBuilder(StreamExecutionEnvironment env, Parameters params) {
        this.env = env;
        this.params = params;
    }

    public SingleOutputStreamOperator<VectorData> getSourceStream(boolean throttle) {
        List<Long> thresholds =
                throttle ? params.getInsertThrottleThresholds() : Collections.singletonList(0L);
        List<Long> rates = throttle ? params.getInsertRates() : Collections.singletonList(0L);

        if (params.getSourcePath().endsWith(".txt")) {
            if ("bind-insert".equals(params.getQueryThrottleMode())) {
                throw new RuntimeException("Text source does not support binding rate.");
            }
            if (Objects.nonNull(params.getWaitingIndexStrategy())
                    && !"none".equalsIgnoreCase(params.getWaitingIndexStrategy())) {
                throw new RuntimeException("Text source does not support waiting for index build.");
            }
            if (params.getDeleteRatio() != 0D) {
                throw new RuntimeException("Text source does not support delete ratio != 0.");
            }

            return getTextSource(
                    params.getHdfsAddress(),
                    params.getSourcePath(),
                    thresholds,
                    rates,
                    "source",
                    params.getInsertLoops());
        } else if (params.getSourcePath().endsWith(".fvecs")
                || params.getSourcePath().endsWith(".bvecs")) {

            String waitingIndexStrategy = params.getWaitingIndexStrategy();

            RateControllerBuilder rateControllerBuilder;
            if (Objects.isNull(waitingIndexStrategy)
                    || "none".equalsIgnoreCase(waitingIndexStrategy)) {
                rateControllerBuilder =
                        new StagedRateControllerBuilder(thresholds, ratesToIntervals(rates));
            } else if ("milvus".equalsIgnoreCase(waitingIndexStrategy)) {
                rateControllerBuilder =
                        new WaitingIndexBuildStagedRateControllerBuilder(
                                thresholds,
                                ratesToIntervals(rates),
                                params.getIndexWaitRatios(),
                                new MilvusWaitingIndexBuildStrategy(
                                        params.getMilvusHost(),
                                        params.getMilvusPort(),
                                        params.getMilvusCollectionName(),
                                        DEFAULT_MILVUS_INDEX_NAME));
            } else if ("qdrant".equalsIgnoreCase(waitingIndexStrategy)) {
                rateControllerBuilder =
                        new WaitingIndexBuildStagedRateControllerBuilder(
                                thresholds,
                                ratesToIntervals(rates),
                                params.getIndexWaitRatios(),
                                new QdrantWaitingIndexBuildStrategy(
                                        params.getQdrantHost(),
                                        params.getQdrantPort(),
                                        params.getQdrantCollectionName()));
            } else {
                throw new RuntimeException("Unknown waiting index strategy.");
            }

            if ("bind-insert".equals(params.getQueryThrottleMode())) {
                rateControllerBuilder =
                        new BindingRateControllerBuilder(
                                rateControllerBuilder,
                                params.getHdfsAddress(),
                                params.getHdfsUser(),
                                params.getQueryRatePollingPath(),
                                rateToInterval(params.getInitialQueryRate()),
                                rateToInterval(params.getNewQueryRate()),
                                params.getQueryThrottleInsertThreshold());
            }

            return getBinarySource(
                    params.getHdfsAddress(),
                    params.getSourcePath(),
                    "source",
                    params.getVectorDim(),
                    params.getInsertSkip(),
                    params.getInsertLimitPerLoop(),
                    params.getInsertLoops(),
                    params.getInsertReadBulkSize(),
                    params.getDeleteRatio(),
                    rateControllerBuilder);
        } else {
            throw new RuntimeException("Unknown file type.");
        }
    }

    public SingleOutputStreamOperator<VectorData> getQueryStream(boolean throttle) {
        List<Long> thresholds =
                throttle ? params.getQueryThrottleThresholds() : Collections.singletonList(0L);
        List<Long> rates = throttle ? params.getQueryRates() : Collections.singletonList(0L);

        if (params.getQueryPath().endsWith(".txt")) {
            if ("bind-query".equals(params.getQueryThrottleMode())) {
                throw new RuntimeException("Text source does not support binding rate.");
            }

            return getTextSource(
                    params.getHdfsAddress(),
                    params.getQueryPath(),
                    thresholds,
                    rates,
                    "query",
                    params.getQueryLoops());
        } else if (params.getQueryPath().endsWith(".fvecs")
                || params.getQueryPath().endsWith(".bvecs")) {

            RateControllerBuilder rateControllerBuilder;
            if ("staged".equals(params.getQueryThrottleMode())) {
                rateControllerBuilder =
                        new StagedRateControllerBuilder(thresholds, ratesToIntervals(rates));
            } else {
                rateControllerBuilder =
                        new PollingRateControllerBuilder(
                                params.getHdfsAddress(),
                                params.getHdfsUser(),
                                params.getQueryRatePollingPath(),
                                QUERY_POLLING_INTERVAL_MILLIS,
                                rateToInterval(params.getInitialQueryRate()));
            }

            return getBinarySource(
                    params.getHdfsAddress(),
                    params.getQueryPath(),
                    "query",
                    params.getVectorDim(),
                    0,
                    Integer.MAX_VALUE,
                    params.getQueryLoops(),
                    params.getQueryReadBulkSize(),
                    0D,
                    rateControllerBuilder);
        } else {
            throw new RuntimeException("Unknown file type.");
        }
    }

    public SingleOutputStreamOperator<PartitionedElement> getHybridStream(boolean throttle) {
        if (throttle) {
            throw new RuntimeException("Hybrid stream does not support throttling.");
        } else {
            FileSource<String> fileSource =
                    FileSource.forRecordStreamFormat(
                                    new TextLineInputFormat(),
                                    new Path(params.getHdfsAddress() + params.getSourcePath()))
                            .build();
            return env.fromSource(
                            fileSource, WatermarkStrategy.noWatermarks(), "hdfs-vector-source")
                    .map(parser::parsePartitionedData)
                    .setParallelism(1)
                    .name("hybrid input")
                    .returns(PartitionedElement.class)
                    .map(
                            data -> {
                                data.getData().setEventTime(System.currentTimeMillis());
                                return data;
                            })
                    .setParallelism(1)
                    .returns(PartitionedElement.class);
        }
    }

    /**
     * Get a stream of vectors, then queries.
     *
     * <p>ONLY FOR TESTING -- CANNOT HANDLE LARGE DATA.
     *
     * @return
     * @throws Exception
     */
    public SingleOutputStreamOperator<PartitionedElement> getPartitionedSourceAndQueryStream()
            throws Exception {
        // Collect source and query data to master. For now we only support 1M source and 1M query.
        List<VectorData> source =
                getSourceStream(false).executeAndCollect("fetch source", 1_000_000);
        List<VectorData> queries =
                getQueryStream(false).executeAndCollect("fetch queries", 1_000_000);

        Random random = new Random(2345678L);

        PartitionFunction partitioner = PartitionFunction.getPartitionFunction(params, random);

        List<PartitionedElement> data = new ArrayList<>();

        Collector<PartitionedElement> collector =
                new Collector<PartitionedElement>() {
                    @Override
                    public void collect(PartitionedElement record) {
                        data.add(record);
                    }

                    @Override
                    public void close() {}
                };

        for (VectorData vector : source) {
            partitioner.flatMap1(vector, collector);
        }
        for (VectorData query : queries) {
            partitioner.flatMap2(query, collector);
        }

        // Count elements in each partition
        Map<Integer, Integer> count = new HashMap<>();
        for (PartitionedElement record : data) {
            count.put(record.getPartitionId(), count.getOrDefault(record.getPartitionId(), 0) + 1);
        }
        System.out.println(count);

        return env.fromCollection(data, TypeInformation.of(PartitionedElement.class));
    }

    private SingleOutputStreamOperator<VectorData> getTextSource(
            String hdfsAddress,
            String hdfsPath,
            List<Long> thresholds,
            List<Long> rates,
            String name,
            int numLoops) { // (Deprecated) txt files

        FileSource<VectorData> fileSource =
                FileSource.forRecordStreamFormat(
                                new FloatVectorInputFormat(params.getMaxTTL()),
                                new Path(hdfsAddress + hdfsPath))
                        .setFileEnumerator(
                                new LoopingNonSplittingRecursiveEnumerator.Provider(numLoops))
                        .build();

        VectorDataThrottler throttler =
                new VectorDataThrottler(thresholds, ratesToIntervals(rates), numLoops > 1);

        return env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "hdfs-vector-source")
                .setParallelism(1)
                .name(name + " input")
                .returns(VectorData.class)
                .disableChaining()
                .map(throttler)
                .setParallelism(1)
                .name(name + " throttle")
                .returns(VectorData.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VectorData>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (data, timestamp) -> {
                                            long current = System.currentTimeMillis();
                                            data.setEventTime(current);
                                            return current; // currently, use system time as
                                            // event
                                            // time
                                        }))
                .name(name + " timestamps");
    }

    private SingleOutputStreamOperator<VectorData> getBinarySource(
            String hdfsAddress,
            String hdfsPath,
            String name,
            int vectorDim,
            int skip,
            int limitPerLoop,
            int numLoops,
            int bulkSize,
            double deleteRatio,
            RateControllerBuilder rateControllerBuilder) {

        // Binary files
        FloatVectorBinaryInputFormat.FileType fileType;
        if (hdfsPath.endsWith(".fvecs")) {
            fileType = FloatVectorBinaryInputFormat.FileType.F_VEC;
        } else {
            // hdfsPath.endsWith(".bvecs")
            fileType = FloatVectorBinaryInputFormat.FileType.B_VEC;
        }

        FileSource<VectorData> fileSource =
                FileSource.forBulkFileFormat(
                                new FloatVectorBinaryInputFormatAdaptor(
                                        new FloatVectorBinaryInputFormat(
                                                name,
                                                params.getMaxTTL(),
                                                fileType,
                                                skip,
                                                limitPerLoop,
                                                vectorDim,
                                                numLoops,
                                                deleteRatio,
                                                rateControllerBuilder),
                                        bulkSize,
                                        vectorDim),
                                new Path(hdfsAddress + hdfsPath))
                        .setFileEnumerator(new LoopingNonSplittingRecursiveEnumerator.Provider(1))
                        .build();

        return env.fromSource(
                        fileSource,
                        WatermarkStrategy.<VectorData>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (vector, timestamp) -> vector.getEventTime()),
                        "hdfs-vector-source")
                .setParallelism(1)
                .setMaxParallelism(1)
                .name(name + " input")
                .returns(VectorData.class)
                .disableChaining();
    }

    // rate: 负数，表示实际的 interval，即每隔多少秒插一个；0 表示不加限制；正数表示每秒插多少个
    private long rateToInterval(long rate) {
        if (rate < 0L) return (-rate) * 1_000_000_000L;
        if (rate == 0L) return 0L; // 0 means no speed limit
        return 1_000_000_000L / rate;
    }

    private List<Long> ratesToIntervals(List<Long> rates) {
        return rates.stream().map(this::rateToInterval).collect(toList());
    }
}
