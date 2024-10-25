package cn.edu.zju.daily.util.chromadb;

import java.util.List;
import tech.amikos.chromadb.EFException;
import tech.amikos.chromadb.Embedding;
import tech.amikos.chromadb.embeddings.EmbeddingFunction;

public class EmptyChromaEmbeddingFunction implements EmbeddingFunction {

    private static final EmptyChromaEmbeddingFunction INSTANCE = new EmptyChromaEmbeddingFunction();

    private EmptyChromaEmbeddingFunction() {}

    public static EmptyChromaEmbeddingFunction getInstance() {
        return INSTANCE;
    }

    @Override
    public Embedding embedQuery(String query) throws EFException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Embedding> embedDocuments(List<String> documents) throws EFException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Embedding> embedDocuments(String[] documents) throws EFException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
