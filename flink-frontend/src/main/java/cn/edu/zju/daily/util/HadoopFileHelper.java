package cn.edu.zju.daily.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** @author shenghao */
public class HadoopFileHelper {

    private final FileSystem fs;

    public HadoopFileHelper(String hdfsAddress, String hdfsUserName) throws IOException {
        System.setProperty("HADOOP_USER_NAME", hdfsUserName);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsAddress);
        fs = FileSystem.get(conf);
    }

    public boolean exists(final String remotePath) throws IOException {
        return fs.exists(new Path(remotePath));
    }

    public long getLength(final String remotePath) throws IOException {
        return fs.getFileStatus(new Path(remotePath)).getLen();
    }

    public void delete(final String remotePath) throws IOException {
        fs.delete(new Path(remotePath), false);
    }

    /**
     * Get the output stream of the remote file. Files are overwritten if they already exist.
     *
     * @param remotePath The path of the remote file.
     * @return The output stream of the remote file.
     * @throws IOException If an I/O error occurs.
     */
    public OutputStream getOutputStream(final String remotePath) throws IOException {
        return fs.create(new Path(remotePath));
    }

    /**
     * Get the input stream of the remote file.
     *
     * @param remotePath The path of the remote file.
     * @return The input stream of the remote file.
     * @throws IOException If an I/O error occurs.
     */
    public InputStream getInputStream(final String remotePath) throws IOException {
        return fs.open(new Path(remotePath));
    }

    public List<String> readAllLines(final String remotePath) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(getInputStream(remotePath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
