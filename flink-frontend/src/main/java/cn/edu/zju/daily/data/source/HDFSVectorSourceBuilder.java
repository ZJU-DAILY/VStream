package cn.edu.zju.daily.data.source;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.data.PartitionedData;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.HDFSVectorParser;
import cn.edu.zju.daily.function.partitioner.PartitionFunction;
import cn.edu.zju.daily.rate.FloatVectorThrottler;
import cn.edu.zju.daily.util.Parameters;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/** Create vector source with designated rate. */
public class HDFSVectorSourceBuilder {

    private final StreamExecutionEnvironment env;
    private final Parameters params;
    private final HDFSVectorParser parser = new HDFSVectorParser();

    public HDFSVectorSourceBuilder(StreamExecutionEnvironment env, Parameters params) {
        this.env = env;
        this.params = params;
    }

    public SingleOutputStreamOperator<FloatVector> getSourceStream(boolean throttle) {
        if (throttle) {
            return get(
                    params.getHdfsAddress(),
                    params.getSourcePath(),
                    params.getInsertThrottleThresholds(),
                    params.getInsertRates(),
                    "source",
                    5,
                    1);
        } else {
            return get(
                    params.getHdfsAddress(),
                    params.getSourcePath(),
                    Collections.singletonList(0L),
                    Collections.singletonList(0L),
                    "source",
                    5,
                    1);
        }
    }

    public SingleOutputStreamOperator<FloatVector> getQueryStream(boolean throttle) {
        if (throttle) {
            return get(
                    params.getHdfsAddress(),
                    params.getQueryPath(),
                    params.getQueryThrottleThresholds(),
                    params.getQueryRates(),
                    "query",
                    1,
                    params.getQueryLoops());
        } else {
            return get(
                    params.getHdfsAddress(),
                    params.getQueryPath(),
                    Collections.singletonList(0L),
                    Collections.singletonList(0L),
                    "query",
                    1,
                    params.getQueryLoops());
        }
    }

    public SingleOutputStreamOperator<PartitionedData> getHybridStream(boolean throttle) {
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
                    .returns(PartitionedData.class)
                    .map(
                            data -> {
                                data.getVector().setEventTime(System.currentTimeMillis());
                                return data;
                            })
                    .setParallelism(1)
                    .returns(PartitionedData.class);
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
    public SingleOutputStreamOperator<PartitionedData> getPartitionedSourceAndQueryStream()
            throws Exception {
        // Collect source and query data to master. For now we only support 1M source and 1M query.
        List<FloatVector> source =
                getSourceStream(false).executeAndCollect("fetch source", 1_000_000);
        List<FloatVector> queries =
                getQueryStream(false).executeAndCollect("fetch queries", 1_000_000);

        Random random = new Random(2345678L);

        PartitionFunction partitioner = PartitionFunction.getPartitionFunction(params, random);

        List<PartitionedData> data = new ArrayList<>();

        Collector<PartitionedData> collector =
                new Collector<PartitionedData>() {
                    @Override
                    public void collect(PartitionedData record) {
                        data.add(record);
                    }

                    @Override
                    public void close() {}
                };

        for (FloatVector vector : source) {
            partitioner.flatMap1(vector, collector);
        }
        for (FloatVector query : queries) {
            partitioner.flatMap2(query, collector);
        }

        // Count elements in each partition
        Map<Integer, Integer> count = new HashMap<>();
        for (PartitionedData record : data) {
            count.put(record.getPartitionId(), count.getOrDefault(record.getPartitionId(), 0) + 1);
        }
        System.out.println(count);

        return env.fromCollection(data, TypeInformation.of(PartitionedData.class));
    }

    private static class MaxTTLSetter implements MapFunction<String, FloatVector> {

        long maxTTL;
        HDFSVectorParser parser;

        MaxTTLSetter(long maxTTL, HDFSVectorParser parser) {
            this.maxTTL = maxTTL;
            this.parser = parser;
        }

        @Override
        public FloatVector map(String value) throws Exception {
            FloatVector vector = parser.parseVector(value);
            vector.setTTL(maxTTL); // currently set to max TTL
            return vector;
        }
    }

    private SingleOutputStreamOperator<FloatVector> get(
            String hdfsAddress,
            String hdfsPath,
            List<Long> thresholds,
            List<Long> rates,
            String name,
            int sourceParallelism,
            int numLoops) {
        FileSource<FloatVector> fileSource;
        if (numLoops > 1) {
            // Continuous source not supported yet
            fileSource =
                    FileSource.forRecordStreamFormat(
                                    new FloatVectorInputFormat(params.getMaxTTL()),
                                    new Path(hdfsAddress + hdfsPath))
                            .setFileEnumerator(
                                    new LoopingNonSplittingRecursiveEnumerator.Provider(numLoops))
                            .build();
        } else {
            fileSource =
                    FileSource.forRecordStreamFormat(
                                    new FloatVectorInputFormat(params.getMaxTTL()),
                                    new Path(hdfsAddress + hdfsPath))
                            .build();
        }

        FloatVectorThrottler throttler =
                new FloatVectorThrottler(thresholds, ratesToIntervals(rates), numLoops > 1);
        return env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "hdfs-vector-source")
                .setParallelism(1)
                .name(name + " input")
                .returns(FloatVector.class)
                .disableChaining()
                .map(throttler)
                .setParallelism(1)
                .name(name + " throttle")
                .returns(FloatVector.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<FloatVector>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (vector, timestamp) -> {
                                            long current = System.currentTimeMillis();
                                            vector.setEventTime(current);
                                            return current; // currently, use system time as event
                                            // time
                                        }))
                .name(name + " timestamps");
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
