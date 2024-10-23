package cn.edu.zju.daily.data.source.format;

import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.core.fs.FSDataInputStream;

class BufferedFSDataInputStream extends FSDataInputStream {

    private volatile byte[] buf;
    private int pos; // next byte to read
    private int count; // last valid byte + 1
    private FSDataInputStream in;

    public BufferedFSDataInputStream(FSDataInputStream inputStream, int bufferSize) {
        this.buf = new byte[bufferSize];
        this.in = inputStream;
    }

    @Override
    public void seek(long seekPos) throws IOException {
        in.seek(seekPos);
        pos = 0;
        count = 0;
    }

    @Override
    public long getPos() throws IOException {
        throw new UnsupportedOperationException("Do not use this function. Not reliable.");
    }

    @Override
    public int read() throws IOException {
        if (pos >= buf.length) {
            pos = 0;
            count = 0;
        }
        if (pos >= count) {
            fill();
            if (pos >= count) {
                return -1;
            }
        }
        return getBufIfOpen()[pos++] & 0xff;
    }

    public void fill() throws IOException {
        int bytesToFill = getBufIfOpen().length - count;
        while (bytesToFill > 0) {
            int bytesRead = getInIfOpen().read(getBufIfOpen(), count, bytesToFill);
            if (bytesRead == -1) {
                return;
            }
            count += bytesRead;
            bytesToFill -= bytesRead;
        }
    }

    private void fill(byte[] a) throws IOException {
        int c = 0;
        int bytesToFill = a.length - c;
        while (bytesToFill > 0) {
            int bytesRead = getInIfOpen().read(a, c, bytesToFill);
            if (bytesRead == -1) {
                return;
            }
            c += bytesRead;
            bytesToFill -= bytesRead;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public synchronized int readAux(byte[] b, long pos) throws IOException {
        long oldPos = in.getPos();
        getInIfOpen().seek(pos);
        fill(b);
        getInIfOpen().seek(oldPos);
        return b.length;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (; ; ) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0) return (n == 0) ? nread : n;
            n += nread;
            if (n >= len) return n;
            // if not closed but no bytes available, return
            InputStream input = in;
            if (input != null && input.available() <= 0) return n;
        }
    }

    private int read1(byte[] b, int off, int len) throws IOException {
        if (pos >= buf.length) {
            pos = 0;
            count = 0;
        }
        int avail = count - pos;
        if (avail <= 0) {
            fill();
            avail = count - pos;
            if (avail <= 0) return -1;
        }
        int cnt = Math.min(avail, len);
        System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
        pos += cnt;
        return cnt;
    }

    private FSDataInputStream getInIfOpen() {
        FSDataInputStream input = in;
        if (input == null) throw new NullPointerException("Stream closed");
        return input;
    }

    private byte[] getBufIfOpen() {
        byte[] buffer = buf;
        if (buffer == null) throw new NullPointerException("Stream closed");
        return buffer;
    }

    @Override
    public void close() throws IOException {
        super.close();
        in.close();
        in = null;
        buf = null;
    }
}
