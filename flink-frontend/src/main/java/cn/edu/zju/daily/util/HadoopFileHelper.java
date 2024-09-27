package cn.edu.zju.daily.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** @author shenghao */
public class HadoopFileHelper {

    private final String remotePath;

    private final FileSystem fs;

    public HadoopFileHelper(String hdfsAddress, String remotePath, String hdfsUserName)
            throws IOException {
        this.remotePath = remotePath;
        System.setProperty("HADOOP_USER_NAME", hdfsUserName);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsAddress);
        fs = FileSystem.get(conf);
    }

    public boolean remoteFileExists() throws IOException {
        return fs.exists(new Path(remotePath));
    }

    public long getLength() throws IOException {
        return fs.getFileStatus(new Path(remotePath)).getLen();
    }

    public void deleteRemoteFile() throws IOException {
        fs.delete(new Path(remotePath), false);
    }

    public Pair<OutputStream, BufferedWriter> beginWrite() throws IOException {
        if (remoteFileExists()) {
            deleteRemoteFile();
        }
        // 指定要写入的文件路径
        Path filePath = new Path(remotePath);

        // 打开输出流
        OutputStream outputStream = fs.create(filePath);
        return Pair.of(outputStream, new BufferedWriter(new OutputStreamWriter(outputStream)));
    }

    public void endWrite(BufferedWriter bufferedWriter, OutputStream outputStream)
            throws IOException {
        // 关闭流
        bufferedWriter.close();
        outputStream.close();
    }

    public void writeString(BufferedWriter bufferedWriter, String string) throws IOException {
        bufferedWriter.write(string);
        bufferedWriter.newLine();
    }
}
