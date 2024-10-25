package cn.edu.zju.daily.util.chromadb;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.LinkedTreeMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import tech.amikos.chromadb.ChromaException;
import tech.amikos.chromadb.Embedding;
import tech.amikos.chromadb.embeddings.EmbeddingFunction;
import tech.amikos.chromadb.handler.ApiException;
import tech.amikos.chromadb.handler.DefaultApi;
import tech.amikos.chromadb.model.*;

public class ChromaCollection {
    static Gson gson = new Gson();
    DefaultApi api;
    String collectionName;

    String collectionId;

    LinkedTreeMap<String, Object> metadata = new LinkedTreeMap<>();

    private final EmbeddingFunction embeddingFunction;

    public ChromaCollection(
            DefaultApi api, String collectionName, EmbeddingFunction embeddingFunction) {
        this.api = api;
        this.collectionName = collectionName;
        this.embeddingFunction = embeddingFunction;
    }

    public String getName() {
        return collectionName;
    }

    public String getId() {
        return collectionId;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @SuppressWarnings("unchecked")
    public ChromaCollection fetch() throws ApiException {
        try {
            LinkedTreeMap<String, ?> resp =
                    (LinkedTreeMap<String, ?>) api.getCollection(collectionName);
            this.collectionName = resp.get("name").toString();
            this.collectionId = resp.get("id").toString();
            this.metadata = (LinkedTreeMap<String, Object>) resp.get("metadata");
            return this;
        } catch (ApiException e) {
            throw e;
        }
    }

    public static ChromaCollection getInstance(DefaultApi api, String collectionName)
            throws ApiException {
        return new ChromaCollection(api, collectionName, null);
    }

    @Override
    public String toString() {
        return "ChromaCollection{"
                + "collectionName='"
                + collectionName
                + '\''
                + ", collectionId='"
                + collectionId
                + '\''
                + ", metadata="
                + metadata
                + '}';
    }

    public Object delete() throws ApiException {
        return this.delete(null, null, null);
    }

    @SuppressWarnings("unchecked")
    public Object add(
            List<float[]> embeddings,
            List<Map<String, String>> metadatas,
            List<String> documents,
            List<String> ids)
            throws ChromaException {
        AddEmbedding req = new AddEmbedding();
        List<float[]> _embeddings = embeddings;
        if (_embeddings == null) {
            _embeddings =
                    this.embeddingFunction.embedDocuments(documents).stream()
                            .map(Embedding::asArray)
                            .collect(Collectors.toList());
        }
        req.setEmbeddings((List<Object>) (Object) _embeddings);
        req.setMetadatas((List<Map<String, Object>>) (Object) metadatas);
        req.setDocuments(documents);
        req.incrementIndex(true);
        req.setIds(ids);
        try {
            return api.add(req, this.collectionId);
        } catch (ApiException e) {
            throw new ChromaException(e);
        }
    }

    public Integer count() throws ApiException {
        return api.count(this.collectionId);
    }

    public Object delete(
            List<String> ids, Map<String, Object> where, Map<String, Object> whereDocument)
            throws ApiException {
        DeleteEmbedding req = new DeleteEmbedding();
        req.setIds(ids);
        if (where != null) {
            req.where(
                    where.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        if (whereDocument != null) {
            req.whereDocument(
                    whereDocument.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        return api.delete(req, this.collectionId);
    }

    public Object deleteWithIds(List<String> ids) throws ApiException {
        return delete(ids, null, null);
    }

    public QueryResponse query(
            List<String> queryTexts,
            Integer nResults,
            Map<String, Object> where,
            Map<String, Object> whereDocument,
            List<QueryEmbedding.IncludeEnum> include)
            throws ChromaException {
        List<float[]> embeddings =
                this.embeddingFunction.embedDocuments(queryTexts).stream()
                        .map(Embedding::asArray)
                        .collect(Collectors.toList());
        return queryEmbeddings(embeddings, nResults, where, whereDocument, include);
    }

    @SuppressWarnings("unchecked")
    public QueryResponse queryEmbeddings(
            List<float[]> embeddings,
            Integer nResults,
            Map<String, Object> where,
            Map<String, Object> whereDocument,
            List<QueryEmbedding.IncludeEnum> include)
            throws ChromaException {
        QueryEmbedding body = new QueryEmbedding();
        body.queryEmbeddings((List<Object>) (Object) embeddings);
        body.nResults(nResults);
        body.include(include);
        if (where != null) {
            body.where(
                    where.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        if (whereDocument != null) {
            body.whereDocument(
                    whereDocument.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        try {
            Gson gson = new Gson();
            String json = gson.toJson(api.getNearestNeighbors(body, this.collectionId));
            return new Gson().fromJson(json, QueryResponse.class);
        } catch (ApiException e) {
            throw new ChromaException(e);
        }
    }

    public static class QueryResponse {
        @SerializedName("documents")
        private List<List<String>> documents;

        @SerializedName("embeddings")
        private List<List<Float>> embeddings;

        @SerializedName("ids")
        private List<List<String>> ids;

        @SerializedName("metadatas")
        private List<List<Map<String, Object>>> metadatas;

        @SerializedName("distances")
        private List<List<Float>> distances;

        public List<List<String>> getDocuments() {
            return documents;
        }

        public List<List<Float>> getEmbeddings() {
            return embeddings;
        }

        public List<List<String>> getIds() {
            return ids;
        }

        public List<List<Map<String, Object>>> getMetadatas() {
            return metadatas;
        }

        public List<List<Float>> getDistances() {
            return distances;
        }

        @Override
        public String toString() {
            return new Gson().toJson(this);
        }
    }

    public static class GetResult {
        @SerializedName("documents")
        private List<String> documents;

        @SerializedName("embeddings")
        private List<Float> embeddings;

        @SerializedName("ids")
        private List<String> ids;

        @SerializedName("metadatas")
        private List<Map<String, Object>> metadatas;

        public List<String> getDocuments() {
            return documents;
        }

        public List<Float> getEmbeddings() {
            return embeddings;
        }

        public List<String> getIds() {
            return ids;
        }

        public List<Map<String, Object>> getMetadatas() {
            return metadatas;
        }

        @Override
        public String toString() {
            return new Gson().toJson(this);
        }
    }
}
