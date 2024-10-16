package cn.edu.zju.daily.util;

import java.util.List;
import tech.amikos.chromadb.EmbeddingFunction;

public class CustomEmptyChromaEmbeddingFunction implements EmbeddingFunction {

    private static final CustomEmptyChromaEmbeddingFunction INSTANCE =
            new CustomEmptyChromaEmbeddingFunction();

    private CustomEmptyChromaEmbeddingFunction() {}

    @Override
    public List<List<Float>> createEmbedding(List<String> documents) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<List<Float>> createEmbedding(List<String> documents, String model) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public static CustomEmptyChromaEmbeddingFunction getInstance() {
        return INSTANCE;
    }
}
