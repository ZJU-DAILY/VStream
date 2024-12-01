package cn.edu.zju.daily.data.result;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;

/**
 * This class reads an integer vector file in SIFT format.
 *
 * @see <a href="ttp://corpus-texmex.irisa.fr">Datasets for approximate nearest neighbor search</a>
 */
@Slf4j
public class IvecIterator implements Iterator<int[]> {

    private final RandomAccessFile file;
    private final long length; // file length in bytes
    private final ByteBuffer intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    private ByteBuffer buffer;

    public static IvecIterator fromFile(String filename) throws IOException {
        return new IvecIterator(new RandomAccessFile(filename, "r"));
    }

    /**
     * 创建向量读取器。
     *
     * @param file 向量文件。假设所有向量的维度相同。
     * @param numLoops 循环多少次
     * @param skip 每个循环从第skip个向量开始
     * @param limit 每个循环读取limit个向量
     * @param inputType 读取float (4-bit) 或unsigned char (1-bit)向量
     * @throws IOException 读取文件失败
     */
    public IvecIterator(RandomAccessFile file) throws IOException {
        this.file = file;
        this.length = file.length();
        this.file.seek(0);
    }

    private boolean isEOF() throws IOException {
        return file.getFilePointer() == length;
    }

    @Override
    public boolean hasNext() {
        try {
            return !isEOF();
        } catch (IOException e) {
            LOG.error("Failed to get file pointer.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public int[] next() {
        try {
            int dimension = readIntLittleEndian(file);
            ensureBuffer(dimension * 4);
            file.read(buffer.array(), 0, dimension * 4);
            int[] vector = new int[dimension];
            buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(vector, 0, dimension);
            return vector;
        } catch (IOException e) {
            LOG.error("Failed to read file.");
            throw new RuntimeException(e);
        }
    }

    private int readIntLittleEndian(RandomAccessFile file) throws IOException {
        file.read(intBuffer.array(), 0, 4);
        return intBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(0);
    }

    private void ensureBuffer(int length) {
        if (buffer == null || buffer.capacity() < length) {
            buffer = ByteBuffer.allocate(length);
        }
        ((Buffer) buffer).position(0);
    }
}
