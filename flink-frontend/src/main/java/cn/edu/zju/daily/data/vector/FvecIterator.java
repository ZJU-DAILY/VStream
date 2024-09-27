package cn.edu.zju.daily.data.vector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author auroflow */
public class FvecIterator implements Iterator<float[]> {

    public enum InputType {
        F_VEC,
        B_VEC
    }

    private static final Logger logger = LoggerFactory.getLogger(FvecIterator.class);

    private final RandomAccessFile file;
    private final int numLoops;
    private int loop = 0;
    private final long length; // file length in bytes
    private final ByteBuffer intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    private ByteBuffer buffer;
    private final InputType inputType;
    private final long startPosition;
    private final long limit;
    private long count = 0;

    public static FvecIterator fromFile(String filename) throws IOException {
        return fromFile(filename, 1);
    }

    public static FvecIterator fromFile(String filename, int numLoops) throws IOException {
        if (filename.endsWith(".fvecs")) {
            return new FvecIterator(
                    new RandomAccessFile(filename, "r"),
                    numLoops,
                    0,
                    Long.MAX_VALUE,
                    InputType.F_VEC);
        } else if (filename.endsWith(".bvecs")) {
            return new FvecIterator(
                    new RandomAccessFile(filename, "r"),
                    numLoops,
                    0,
                    Long.MAX_VALUE,
                    InputType.B_VEC);
        } else {
            throw new RuntimeException("Unknown file type.");
        }
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
    public FvecIterator(
            RandomAccessFile file, int numLoops, long skip, long limit, InputType inputType)
            throws IOException {
        this.file = file;
        this.numLoops = numLoops;
        this.limit = limit;
        this.length = file.length();
        this.inputType = inputType;

        int dimension = readIntLittleEndian(file);
        int vectorWidth = 4 + dimension * (inputType == InputType.F_VEC ? 4 : 1);
        this.startPosition = vectorWidth * skip;
        this.file.seek(startPosition);
    }

    private boolean isEOF() throws IOException {
        return (count != 0 && count % limit == 0) || file.getFilePointer() == length;
    }

    @Override
    public boolean hasNext() {
        try {
            // There are no more vectors if the fp is at EOF, and we're at the last loop
            return !(isEOF() && loop == numLoops - 1);
        } catch (IOException e) {
            logger.error("Failed to get file pointer.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public float[] next() {
        try {
            if (isEOF()) {
                if (loop < numLoops) {
                    logger.debug("File reached limit, seek to start pos");
                    file.seek(startPosition);
                    loop += 1;
                } else {
                    throw new RuntimeException("No more vector.");
                }
            }

            count += 1; // we're reading a new vector
            int dimension = readIntLittleEndian(file);

            if (inputType == InputType.F_VEC) {
                ensureBuffer(dimension * 4);
                file.read(buffer.array(), 0, dimension * 4);
                float[] vector = new float[dimension];
                buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(vector, 0, dimension);
                return vector;
            } else if (inputType == InputType.B_VEC) {
                ensureBuffer(dimension);
                file.read(buffer.array(), 0, dimension);
                byte[] vector = new byte[dimension];
                buffer.get(vector, 0, dimension);
                float[] floatBuffer = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    floatBuffer[i] = vector[i];
                }
                return floatBuffer;
            } else {
                throw new RuntimeException("Impossible branch.");
            }
        } catch (IOException e) {
            logger.error("Failed to read file.");
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
