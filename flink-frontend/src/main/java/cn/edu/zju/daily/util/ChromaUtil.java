package cn.edu.zju.daily.util;

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
}
