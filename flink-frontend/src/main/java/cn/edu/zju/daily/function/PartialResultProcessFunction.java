package cn.edu.zju.daily.function;

import cn.edu.zju.daily.data.result.SearchResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Combine partial search results. The stream should be keyed by query ID and have a count window whose size equals {@code numPartitions}.
 */
public class PartialResultProcessFunction extends KeyedProcessFunction<Long, SearchResult, SearchResult> {

  private final int k;

  private ValueState<SearchResult> partialResult;

  /**
   * Combines partial search results.
   *
   * @param k number of nearest neighbors to return
   */
  public PartialResultProcessFunction(int k) {
    this.k = k;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    partialResult = getRuntimeContext().getState(new ValueStateDescriptor<>("partialResult", SearchResult.class));
  }


  @Override
  public void processElement(SearchResult value, KeyedProcessFunction<Long, SearchResult, SearchResult>.Context ctx, Collector<SearchResult> out) throws Exception {
    SearchResult current = partialResult.value();

    if (current == null) {
      current = value;
    } else {
      current = SearchResult.combine(current, value, k);
    }

    if (current.isComplete()) {
      out.collect(current);
      partialResult.clear();
    } else {
      partialResult.update(current);
    }
  }
}