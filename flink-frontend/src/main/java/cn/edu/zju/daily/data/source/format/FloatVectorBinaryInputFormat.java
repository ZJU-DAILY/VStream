package cn.edu.zju.daily.data.source.format;

import cn.edu.zju.daily.data.source.rate.RateControllerBuilder;
import cn.edu.zju.daily.data.source.rate.UnlimitedRateControllerBuilder;
import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.VectorData;
import cn.edu.zju.daily.data.vector.VectorDeletion;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.time.Duration;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.MathUtils;

@Slf4j
public class FloatVectorBinaryInputFormat extends SimpleStreamFormat<VectorData> {

    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /**
     * If the current clock is ahead of allowedEmitTs by this amount of time, there is severe
     * backpressure and the allowedEmitTs is reset to the current time.
     */
    private static final Duration DELAY_REFRESH_THRESHOLD = Duration.ofSeconds(10);

    private static final int DELETE_BUFFER_SIZE = 1000;

    /**
     * Create a new FloatVectorBinaryInputFormat.
     *
     * @param maxTTL the maximum time-to-live of the vectors
     * @param fileType the type of the file (fvec or bvec)
     * @param skip the number of vectors to skip before each loop
     * @param limitPerLoop the number of vectors to read for each loop; read until EOF if 0
     * @param dim vector dim
     * @param deleteRatio the ratio of {@link VectorDeletion}s in the generated vectors
     * @param numLoops the number of times to loop the part of the file
     */
    public FloatVectorBinaryInputFormat(
            String name,
            long maxTTL,
            FileType fileType,
            int skip,
            int limitPerLoop,
            int dim,
            int numLoops,
            double deleteRatio,
            RateControllerBuilder rateControllerBuilder) {
        this.name = name;
        this.maxTTL = maxTTL;
        this.fileType = fileType;
        this.skip = skip;
        this.limitPerLoop = limitPerLoop == 0 ? Integer.MAX_VALUE : limitPerLoop;
        this.dim = dim;
        this.numLoops = numLoops;
        this.deleteRatio = deleteRatio;
        if (rateControllerBuilder != null) {
            this.rateControllerBuilder = rateControllerBuilder;
        } else {
            this.rateControllerBuilder = new UnlimitedRateControllerBuilder();
        }
    }

    public enum FileType {
        F_VEC(Float.BYTES),
        B_VEC(Byte.BYTES);

        public final int width;

        FileType(int width) {
            this.width = width;
        }
    }

    static final int DIM_BYTES = Integer.BYTES;

    private final String name;
    private final long maxTTL;
    private final FileType fileType;
    private final int skip;
    private final int limitPerLoop;
    private final int dim;
    private final int numLoops;
    private final double deleteRatio;
    private final RateControllerBuilder rateControllerBuilder;

    FileType getFileType() {
        return fileType;
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        RateControllerBuilder.RateController rateController = rateControllerBuilder.build();
        int fetchSize =
                Math.min(
                        MAX_BUFFER_SIZE,
                        MathUtils.checkedDownCast(
                                config.get(StreamFormat.FETCH_IO_SIZE).getBytes()));
        // Otherwise there will be error (should look into it).
        int width = DIM_BYTES + dim * fileType.width;
        fetchSize += width - fetchSize % width;
        if (fetchSize <= 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The fetch size (%s) must be > 0, but is %d",
                            StreamFormat.FETCH_IO_SIZE.key(), fetchSize));
        }
        return new Reader(
                stream,
                name,
                fileType,
                maxTTL,
                skip,
                limitPerLoop,
                dim,
                numLoops,
                deleteRatio,
                rateController,
                fetchSize);
    }

    @Override
    public TypeInformation<VectorData> getProducedType() {
        return TypeInformation.of(VectorData.class);
    }

    public static final class Reader implements StreamFormat.Reader<VectorData> {

        private final BufferedFSDataInputStream stream;
        private final FileType fileType;
        private final long maxTTL;
        private final int vectorBytes;
        private final long startPos; // read from this offset
        private final int limitPerLoop; // number of vectors to read
        private final int dim; // dimension of each vector
        private final int numLoops; // number of times to loop the part of the file
        private final double deleteRatio;
        private final long maxTotal;
        private int deleteBufferIndex;
        private int loop; // current loop

        /** How many data vectors in the file are parsed in this loop. */
        private int count;

        /** Total number of calls to read(), including data vectors and deletions. */
        private long total;

        /** ID of the next inserted vector. */
        private long vectorId;

        private final RateControllerBuilder.RateController rateController;
        private final String name;
        private final Random random;

        private final ByteBuffer intBuffer;
        private ByteBuffer buffer;
        private final ByteBuffer deleteMarkBuffer;
        private long allowedEmitNanos;
        private long deleteIdStart;

        Reader(
                FSDataInputStream stream,
                String name,
                FileType fileType,
                long maxTTL,
                int skip,
                int limitPerLoop,
                int dim,
                int numLoops,
                double deleteRatio,
                RateControllerBuilder.RateController rateController,
                int fetchSize)
                throws IOException {

            if (deleteRatio > 0D && numLoops != 1) {
                throw new IllegalArgumentException("Delete ratio must be 0 when numLoops > 1.");
            }

            this.stream = new BufferedFSDataInputStream(stream, fetchSize);
            this.name = name;
            this.fileType = fileType;
            this.maxTTL = maxTTL;
            this.vectorBytes = DIM_BYTES + dim * fileType.width;
            this.startPos = (long) this.vectorBytes * skip;
            this.limitPerLoop = limitPerLoop;
            this.dim = dim;
            this.numLoops = numLoops;
            this.maxTotal = (long) numLoops * limitPerLoop;
            this.deleteRatio = deleteRatio;
            this.rateController = rateController;
            this.loop = 0;
            this.count = 0;
            this.intBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            this.deleteMarkBuffer =
                    ByteBuffer.allocate(DELETE_BUFFER_SIZE * vectorBytes)
                            .order(ByteOrder.LITTLE_ENDIAN);
            this.deleteIdStart = -1;
            this.allowedEmitNanos = 0;
            this.random = new Random(66);

            try {
                this.stream.seek(startPos);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to seek to the start position " + startPos + ".", e);
            }
            LOG.info(
                    "FloatVectorInputFormat.Reader created with maxTTL = {}, estimated stream size: {}.",
                    maxTTL,
                    stream.available());
        }

        @Override
        public VectorData read() throws IOException {
            // Here count and loop are set by the previous call to be the correct value
            if (this.loop == this.numLoops || this.total == this.maxTotal) {
                return null;
            }

            VectorData data;

            boolean delete = false;
            if (deleteRatio > 0D && count > DELETE_BUFFER_SIZE) {
                double r = random.nextDouble();
                delete = r < deleteRatio;
            }
            if (!delete) {
                float[] floatArray = readNextArray(true);
                if (floatArray == null) {
                    return read();
                }
                data = new FloatVector(vectorId++, floatArray, 0L, maxTTL);
                LOG.trace("Read vector: {}", data);
                // Set the count and loop for the next call
                this.count++;
                if (this.count == this.limitPerLoop) {
                    this.count = 0;
                    this.loop++;
                    this.stream.seek(startPos);
                }
            } else {
                data = getNextDeleteMarker();
                LOG.trace("Read deletion marker: {}", data);
            }

            // Rate control
            long currentNanos = System.nanoTime();

            if (rateController != null
                    && !(rateController instanceof UnlimitedRateControllerBuilder.Controller)) {
                long delay = rateController.getDelayNanos(total);
                long delayRefreshThresholdNanos =
                        Math.max(DELAY_REFRESH_THRESHOLD.toNanos(), delay);
                if (allowedEmitNanos == 0
                        || currentNanos - allowedEmitNanos >= delayRefreshThresholdNanos) {
                    LOG.info(
                            "FloatVectorBinaryInputFormat.Reader ({}): Resetting allowedEmitNanos to current time.",
                            name);
                    allowedEmitNanos = currentNanos;
                }
                while (currentNanos < allowedEmitNanos) {
                    currentNanos = System.nanoTime();
                }
                allowedEmitNanos += delay;
            }
            data.setEventTime(System.currentTimeMillis());
            total++;
            return data;
        }

        /**
         * Returns the next vector from the stream. Returns null if the end of the stream is
         * reached.
         */
        private float[] readNextArray(boolean rewindWhenEOF) throws IOException {
            // Read the vector
            // Skip the dimension as we know it is dim
            int bytesSkippedTotal = 0;
            while (bytesSkippedTotal < DIM_BYTES) {
                int bytesSkipped =
                        stream.read(
                                intBuffer.array(),
                                bytesSkippedTotal,
                                DIM_BYTES - bytesSkippedTotal);
                if (bytesSkipped == -1) {
                    // EOF reached, start next loop
                    LOG.info("EOF reached before dim is read.");
                    if (rewindWhenEOF) {
                        this.count = 0;
                        this.loop++;
                        this.stream.seek(startPos);
                    }
                    return null;
                } else if (bytesSkipped == 0) {
                    break;
                }
                bytesSkippedTotal += bytesSkipped;
            }
            if (bytesSkippedTotal != DIM_BYTES) {
                throw new IOException(
                        "Failed to skip " + DIM_BYTES + " bytes of vector dimension.");
            }

            // Check vector dim
            int dimRead = intBuffer.asIntBuffer().get(0);
            if (dimRead != dim) {
                throw new IOException(
                        "Failed to read the correct dimension of the vector, expected "
                                + dim
                                + ", actually read "
                                + dimRead
                                + ".");
            }

            // Read the data until the vector is complete
            ensureBuffer(dim * fileType.width);
            int bytesReadTotal = 0;
            while (bytesReadTotal < dim * fileType.width) {
                int bytesRead =
                        stream.read(
                                buffer.array(),
                                bytesReadTotal,
                                dim * fileType.width - bytesReadTotal);
                if (bytesRead == -1) {
                    // EOF reached, start next loop
                    throw new IOException("EOF reached halfway through reading the vector.");
                } else if (bytesRead == 0) {
                    break;
                }
                bytesReadTotal += bytesRead;
            }
            if (bytesReadTotal != dim * fileType.width) {
                throw new IOException(
                        "Failed to read "
                                + dim * fileType.width
                                + " bytes of the vector, actually read "
                                + bytesReadTotal
                                + " bytes.");
            }

            float[] floatArray = new float[dim];
            if (fileType == FileType.F_VEC) {
                buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(floatArray, 0, dim);
            } else if (fileType == FileType.B_VEC) {
                byte[] array = new byte[dim];
                buffer.get(array, 0, dim);
                for (int i = 0; i < dim; i++) {
                    floatArray[i] = array[i];
                }
            } else {
                throw new RuntimeException("Unsupported file type: " + fileType);
            }
            return floatArray;
        }

        private VectorDeletion getNextDeleteMarker() {
            if (deleteBufferIndex == DELETE_BUFFER_SIZE || deleteIdStart == -1) {
                deleteBufferIndex = 0;
                deleteIdStart = random.nextInt(count - DELETE_BUFFER_SIZE);
                long deletePos = startPos + deleteIdStart * vectorBytes;
                try {
                    int read = stream.readAux(deleteMarkBuffer.array(), deletePos);
                    if (read != DELETE_BUFFER_SIZE * vectorBytes) {
                        throw new IOException(
                                "Failed to read the deletion markers at position "
                                        + deletePos
                                        + ", expected "
                                        + dim * fileType.width
                                        + " bytes, actually read "
                                        + read
                                        + " bytes.");
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read the deletion marker.", e);
                }
            }

            // read the next delete marker
            float[] floatArray = new float[dim];
            if (fileType == FileType.F_VEC) {
                ((FloatBuffer)
                                deleteMarkBuffer
                                        .asFloatBuffer()
                                        .position(deleteBufferIndex * (1 + dim) + 1))
                        .get(floatArray, 0, dim);
            } else if (fileType == FileType.B_VEC) {
                byte[] array = new byte[dim];
                ((ByteBuffer) deleteMarkBuffer.position(deleteBufferIndex * (4 + dim) + 4))
                        .get(array, 0, dim);
                for (int i = 0; i < dim; i++) {
                    floatArray[i] = array[i];
                }
            } else {
                throw new RuntimeException("Unsupported file type: " + fileType);
            }

            VectorDeletion marker =
                    new VectorDeletion(deleteIdStart + deleteBufferIndex, floatArray, 0L, maxTTL);
            deleteBufferIndex++;
            return marker;
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }

        private void ensureBuffer(int length) {
            if (buffer == null || buffer.capacity() < length) {
                buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
            }
            ((Buffer) buffer).position(0);
        }
    }
}
