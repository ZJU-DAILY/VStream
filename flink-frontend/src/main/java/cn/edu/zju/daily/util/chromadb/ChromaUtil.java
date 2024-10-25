package cn.edu.zju.daily.util.chromadb;

import cn.edu.zju.daily.util.JobInfo;
import cn.edu.zju.daily.util.Parameters;
import com.google.gson.internal.LinkedTreeMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;

public class ChromaUtil {
    public static String chooseAddressToUse(
            String address, JobInfo jobInfo, RuntimeContext runtimeContext) {
        String[] parts = address.split(":");
        String host = parts[0];
        int low = Integer.parseInt(parts[1]);
        int high = Integer.parseInt(parts[2]);
        String taskName = runtimeContext.getTaskName();

        int index = runtimeContext.getIndexOfThisSubtask();
        String thisTMId = jobInfo.getTaskManagerId(taskName, index);
        int indexOnThisTM = 0;
        for (int i = 0; i < index; i++) {
            if (jobInfo.getTaskManagerId(taskName, i).equals(thisTMId)) {
                indexOnThisTM++;
            }
        }

        int port = low + indexOnThisTM;
        if (port > high) {
            throw new RuntimeException("Port number " + port + " exceeds the upper bound " + high);
        }
        return "http://" + host + ":" + port;
    }

    public static List<String> readAddresses(String addressFile) throws IOException {
        return Files.readAllLines(Paths.get(addressFile));
    }

    public static LinkedTreeMap<String, Object> getHnswParams(Parameters params) {
        LinkedTreeMap<String, Object> metadata = new LinkedTreeMap<>();
        metadata.put("hnsw:batch_size", params.getChromaInsertBatchSize());
        metadata.put("hnsw:sync_threshold", params.getChromaHnswSyncThreshold());
        metadata.put("hnsw:resize_factor", params.getChromaHnswResizeFactor());
        metadata.put("hnsw:num_threads", params.getChromaHnswNumThreads());
        metadata.put("hnsw:M", params.getHnswM());
        metadata.put("hnsw:search_ef", params.getHnswEfSearch());
        metadata.put("hnsw:construction_ef", params.getHnswEfConstruction());
        metadata.put("hnsw:space", params.getMetricType().toLowerCase());
        return metadata;
    }
}
