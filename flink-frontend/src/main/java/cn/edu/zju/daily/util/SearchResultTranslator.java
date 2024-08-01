package cn.edu.zju.daily.util;

import com.github.jelmerk.knn.SearchResult;

import java.util.List;

public class SearchResultTranslator {

    public static cn.edu.zju.daily.data.result.SearchResult translate(
            List<com.github.jelmerk.knn.SearchResult<cn.edu.zju.daily.data.vector.FloatVector, Float>> searchResult,
            long queryId) {
        return new cn.edu.zju.daily.data.result.SearchResult(
                queryId,
                searchResult.stream().map(item -> item.item().id()).collect(java.util.stream.Collectors.toList()),
                searchResult.stream().map(SearchResult::distance).collect(java.util.stream.Collectors.toList()));
    }
}
