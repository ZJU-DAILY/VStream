package cn.edu.zju.daily.function.chroma.util;

import cn.edu.zju.daily.util.JobInfo;
import cn.edu.zju.daily.util.Parameters;
import com.google.gson.internal.LinkedTreeMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;

public class ChromaUtil {

    /**
     * Get the address of the Chroma server to use for the current subtask.
     *
     * @param params Parameters
     * @param runtimeContext RuntimeContext
     * @return Chroma server address
     * @throws IOException if the address file cannot be read
     */
    public static String getChromaAddress(Parameters params, RuntimeContext runtimeContext)
            throws IOException {
        String addressFile = params.getChromaAddressFile();
        int subtaskIndex = runtimeContext.getIndexOfThisSubtask();

        List<String> addressGroups = readAddressGroups(addressFile); // host:port_low:port_high

        // Find the address group for this TM
        String addressGroup = null;
        // Get the hostname of the current task executor
        JobInfo jobInfo =
                new JobInfo(params.getFlinkJobManagerHost(), params.getFlinkJobManagerPort());
        String hostName = jobInfo.getHost(runtimeContext.getTaskName(), subtaskIndex).toLowerCase();

        for (String a : addressGroups) {
            String host = a.split(":")[0].toLowerCase();
            if (host.equals(hostName)) {
                addressGroup = a;
                break;
            }
        }
        if (addressGroup == null) {
            throw new RuntimeException("No Chroma server address found for " + hostName);
        }

        // Choose the address for this subtask
        if (params.isChromaMultiInstancesOnTM()) {
            // Each subtask uses a dedicated Chroma instance
            return chooseAddressToUse(addressGroup, jobInfo, runtimeContext);
        } else {
            // Always use the first address in the group
            String[] splits = addressGroup.split(":");
            return "http://" + splits[0] + ":" + splits[1];
        }
    }

    /**
     * Choose a Chroma instance to use for the current subtask on this TM.
     *
     * @param address
     * @param jobInfo
     * @param runtimeContext
     * @return
     */
    private static String chooseAddressToUse(
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

    public static List<String> readAddressGroups(String addressFile) throws IOException {
        return Files.readAllLines(Paths.get(addressFile));
    }

    /**
     * Get Chroma collection metadata for creating a new collection.
     *
     * @param params
     * @return
     */
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
