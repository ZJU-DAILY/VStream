package cn.edu.zju.daily.data.source.format;

import static org.apache.flink.runtime.fs.hdfs.HadoopDataInputStream.MIN_SKIP_BYTES;

import java.io.BufferedInputStream;
import java.io.IOException;
import org.apache.flink.core.fs.FSDataInputStream;

class BufferedFSDataInputStream extends FSDataInputStream {

    private final int bufferSize;
    private BufferedInputStream bufferedInputStream;
    private final FSDataInputStream in;

    public BufferedFSDataInputStream(FSDataInputStream inputStream, int bufferSize) {
        this.bufferSize = bufferSize;
        this.in = inputStream;
        this.bufferedInputStream = new BufferedInputStream(inputStream, bufferSize);
    }

    @Override
    public void seek(long seekPos) throws IOException {
        long delta = seekPos - getPos();
        if (delta >= 0L && delta <= MIN_SKIP_BYTES) {
            skipFully(delta);
        } else {
            in.seek(seekPos);
            bufferedInputStream = new BufferedInputStream(in, bufferSize);
        }
    }

    private void skipFully(long bytes) throws IOException {
        while (bytes > 0) {
            bytes -= bufferedInputStream.skip(bytes);
        }
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public int read() throws IOException {
        return bufferedInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return bufferedInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return bufferedInputStream.read(b, off, len);
    }
}
