package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.PartitionedElement;
import cn.edu.zju.daily.data.result.SearchResult;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.util.Parameters;
import cn.edu.zju.daily.util.SearchResultTranslator;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HnswLibKeyedProcessFunction
        extends KeyedProcessFunction<Integer, PartitionedElement, SearchResult> {

    private static final int BATCH_SIZE = 100;
    private int randomInit;

    private final Parameters params;

    private HnswIndex<Long, float[], FloatVector, Float> index;

    private static final Logger logger = LoggerFactory.getLogger(HnswLibKeyedProcessFunction.class);

    private List<FloatVector> insertBuffer;

    public HnswLibKeyedProcessFunction(Parameters params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        index =
                HnswIndex.newBuilder(
                                params.getVectorDim(),
                                DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE,
                                10_000_001)
                        .withM(params.getHnswM())
                        .withEfConstruction(params.getHnswEfConstruction())
                        .build();
        insertBuffer = new ArrayList<>(BATCH_SIZE);
        randomInit = ThreadLocalRandom.current().nextInt(BATCH_SIZE);
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

        if (data.getDataType() == PartitionedElement.DataType.QUERY) {
            long start = 0;
            if (data.getData().asVector().getId() % 1 == 0) {
                // XXX: bigger output interval
                start = System.currentTimeMillis();
            }
            SearchResult result =
                    SearchResultTranslator.translate(
                            index.findNearest(data.getData().asVector().getValue(), params.getK()),
                            data.getData().asVector().id());
            if (data.getData().asVector().getId() % 1 == 0) {
                logger.info(
                        "Searching query #{} in {} ms",
                        data.getData().getId(),
                        (System.currentTimeMillis() - start));
            }
            collector.collect(result);
        } else if (data.getDataType() == PartitionedElement.DataType.INSERT_OR_DELETE) {

            insertBuffer.add(data.getData().asVector());
            if (insertBuffer.size() + randomInit >= BATCH_SIZE) {
                long start = 0;
                if (data.getData().getId() % 10000 < 100) {
                    start = System.currentTimeMillis();
                }
                index.addAll(insertBuffer);
                insertBuffer.clear();
                if (data.getData().getId() % 10000 < 100) {
                    logger.info(
                            "Inserting vector #{} in {} ms",
                            data.getData().getId(),
                            (System.currentTimeMillis() - start));
                }
                randomInit = 0;
            }
        } else {
            throw new Exception("Unknown data type: " + data.getDataType());
        }
    }
}
