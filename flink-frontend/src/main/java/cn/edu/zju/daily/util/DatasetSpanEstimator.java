package cn.edu.zju.daily.util;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import cn.edu.zju.daily.data.source.HDFSVectorSourceBuilder;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.function.partitioner.LSHPartitionFunction;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DatasetSpanEstimator {

    private final Parameters params;
    private final StreamExecutionEnvironment env;
    private final HDFSVectorSourceBuilder source;

    public DatasetSpanEstimator(Parameters params) {
        this.params = params;
        env = StreamExecutionEnvironment.createLocalEnvironment(1);
        source = new HDFSVectorSourceBuilder(env, params);
    }

    public float estimate() throws Exception {

        List<Tuple2<Float, Float>> tuples =
                source.getSourceStream(false)
                        .map(
                                vector -> {
                                    float min = Float.MAX_VALUE;
                                    float max = Float.MIN_VALUE;
                                    for (float v : vector.getValue()) {
                                        if (v < min) {
                                            min = v;
                                        }
                                        if (v > max) {
                                            max = v;
                                        }
                                    }
                                    return new Tuple2<Float, Float>(min, max);
                                })
                        .returns(TypeInformation.of(new TypeHint<Tuple2<Float, Float>>() {}))
                        .executeAndCollect(10000);
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (Tuple2<Float, Float> tuple : tuples) {
            if (tuple.f0 < min) {
                min = tuple.f0;
            }
            if (tuple.f1 > max) {
                max = tuple.f1;
            }
        }

        return max - min;
    }

    private void tryPartition(float width) throws Exception {
        LSHPartitionFunction partitioner =
                new LSHPartitionFunction(
                        params.getVectorDim(),
                        params.getNumCopies(),
                        params.getNumCopies(),
                        params.getParallelism(),
                        width);

        List<VectorData> vectors = source.getSourceStream(false).executeAndCollect(10000);
        Map<Integer, Long> count =
                vectors.stream()
                        .flatMap(v -> partitioner.getNodeIds(v).stream())
                        .collect(groupingBy(Function.identity(), counting()));
        System.out.println(count);
    }

    public static void main(String[] args) {
        DatasetSpanEstimator estimator =
                new DatasetSpanEstimator(
                        Parameters.load(
                                "/home/auroflow/code/vector-search/rocksdb-stream/src/main/resources/params.yaml",
                                false));
        try {
            float estimate = estimator.estimate();
            System.out.println(estimate + " " + estimate * 8 / 200);
            estimator.tryPartition(estimate * 8 / 200);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
