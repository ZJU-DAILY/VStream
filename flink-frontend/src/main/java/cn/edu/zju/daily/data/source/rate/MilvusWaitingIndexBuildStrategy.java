package cn.edu.zju.daily.data.source.rate;

import cn.edu.zju.daily.function.milvus.MilvusUtil;
import io.milvus.grpc.IndexDescription;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusWaitingIndexBuildStrategy implements WaitingIndexBuildStrategy {

    private static final long SLEEP_INTERVAL = 10000;

    private final String milvusHost;
    private final int milvusPort;
    private final String milvusCollectionName;
    private final String milvusIndexName;

    public MilvusWaitingIndexBuildStrategy(
            String milvusHost,
            int milvusPort,
            String milvusCollectionName,
            String milvusIndexName) {
        this.milvusHost = milvusHost;
        this.milvusPort = milvusPort;
        this.milvusCollectionName = milvusCollectionName;
        this.milvusIndexName = milvusIndexName;
    }

    @Override
    public void waitIndexBuild(float waitRatio) {

        MilvusUtil util = new MilvusUtil();
        util.connect(milvusHost, milvusPort);

        while (true) {
            IndexDescription desc;
            try {
                desc = util.getIndexDescription(milvusCollectionName, milvusIndexName);
            } catch (Exception e) {
                LOG.error("Failed to get index description. Index waiting is SKIPPED!", e);
                return;
            }
            float ratio;
            if (desc.getTotalRows() == 0) {
                ratio = 1;
            } else {
                ratio = (float) desc.getIndexedRows() / desc.getTotalRows();
            }
            if (ratio >= waitRatio) {
                LOG.info(
                        "Total rows: {}, indexed rows: {}, pending index rows: {}, ratio ({}) above required {}",
                        desc.getTotalRows(),
                        desc.getIndexedRows(),
                        desc.getPendingIndexRows(),
                        ratio,
                        waitRatio);
                break;
            } else {
                LOG.info(
                        "Total rows: {}, indexed rows: {}, pending index rows: {}, ratio ({}) still below required {}",
                        desc.getTotalRows(),
                        desc.getIndexedRows(),
                        desc.getPendingIndexRows(),
                        ratio,
                        waitRatio);
            }
            try {
                Thread.sleep(SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for index build", e);
            }
        }
    }
}
