package cn.edu.zju.daily.data.source.rate;

import cn.edu.zju.daily.util.HadoopFileHelper;
import java.io.*;

/**
 * Writes the polling rate to HDFS.
 *
 * <p>This class is not thread-safe. It is dangerous to have two {@code PollingRateWriter}s
 * operating the same HDFS file.
 */
public class DelayPusher implements Serializable {

    private final String hdfsAddress;
    private final String hdfsUser;
    private final String hdfsPath;

    public DelayPusher(String hdfsAddress, String hdfsUser, String hdfsPath) throws IOException {
        this.hdfsAddress = hdfsAddress;
        this.hdfsUser = hdfsUser;
        this.hdfsPath = hdfsPath;
    }

    public void push(long delayNanos) {
        try {
            write(Long.toString(delayNanos));
        } catch (IOException e) {
            throw new RuntimeException("Could not write rate to HDFS", e);
        }
    }

    private void write(String content) throws IOException {
        HadoopFileHelper hdfs = new HadoopFileHelper(hdfsAddress, hdfsUser);
        try (OutputStream stream = hdfs.getOutputStream(hdfsPath);
                OutputStreamWriter writer = new OutputStreamWriter(stream);
                BufferedWriter out = new BufferedWriter(writer)) {
            out.write(content + "\n");
        }
    }
}
