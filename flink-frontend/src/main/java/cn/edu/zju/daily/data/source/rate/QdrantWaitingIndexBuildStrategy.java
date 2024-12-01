package cn.edu.zju.daily.data.source.rate;

import cn.edu.zju.daily.function.qdrant.QdrantUtil;
import io.qdrant.client.grpc.Collections.CollectionInfo;

public class QdrantWaitingIndexBuildStrategy implements WaitingIndexBuildStrategy {

    private final String host;
    private final int port;
    private final String collectionName;

    public QdrantWaitingIndexBuildStrategy(String host, int port, String collectionName) {
        this.host = host;
        this.port = port;
        this.collectionName = collectionName;
    }

    @Override
    public void waitIndexBuild(float waitRatio) {
        try (QdrantUtil util = new QdrantUtil(host, port)) {
            while (true) {
                CollectionInfo info = util.getCollectionInfo(collectionName);
                float ratio = (float) info.getIndexedVectorsCount() / info.getPointsCount();
                if (ratio >= waitRatio) {
                    break;
                }
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to wait for Qdrant index build", e);
        }
    }
}
