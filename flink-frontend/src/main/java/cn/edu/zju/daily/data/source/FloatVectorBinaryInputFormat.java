package cn.edu.zju.daily.data.source;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FloatVectorBinaryInputFormat extends SimpleStreamFormat<FloatVector> {

    private static final Logger LOG = LoggerFactory.getLogger(FloatVectorBinaryInputFormat.class);

    /**
     * If the current clock is ahead of allowedEmitTs by this amount of time, there is severe
     * backpressure and the allowedEmitTs is reset to the current time.
     */
    private static final Duration DELAY_REFRESH_THRESHOLD = Duration.ofSeconds(10);

    /**
     * Create a new FloatVectorBinaryInputFormat.
     *
     * @param maxTTL the maximum time-to-live of the vectors
     * @param fileType the type of the file (fvec or bvec)
     * @param skip the number of vectors to skip before each loop
     * @param limitPerLoop the number of vectors to read for each loop; read until EOF if 0
     * @param dim vector dim
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
            RateController rateController) {
        this.name = name;
        this.maxTTL = maxTTL;
        this.fileType = fileType;
        this.skip = skip;
        this.limitPerLoop = limitPerLoop == 0 ? Integer.MAX_VALUE : limitPerLoop;
        this.dim = dim;
        this.numLoops = numLoops;
        this.rateController = rateController;
    }

    public enum FileType {
        F_VEC(Float.BYTES),
        B_VEC(Byte.BYTES);

        public final int width;

        FileType(int width) {
            this.width = width;
        }
    }

    private static final int DIM_BYTES = Integer.BYTES;

    private final String name;
    private final long maxTTL;
    private final FileType fileType;
    private final int skip;
    private final int limitPerLoop;
    private final int dim;
    private final int numLoops;
    private final RateController rateController;

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        return new Reader(
                stream, name, fileType, maxTTL, skip, limitPerLoop, dim, numLoops, rateController);
    }

    @Override
    public TypeInformation<FloatVector> getProducedType() {
        return TypeInformation.of(FloatVector.class);
    }

    public static final class Reader implements StreamFormat.Reader<FloatVector> {

        private final FSDataInputStream stream;
        private final FileType fileType;
        private final long maxTTL;
        private final long startPos; // read from this offset
        private final int limitPerLoop; // number of vectors to read
        private final int dim; // dimension of each vector
        private final int numLoops; // number of times to loop the part of the file
        private int loop; // current loop
        private int count; // current count in this loop
        private long vectorId;
        private final RateController rateController;
        private final String name;

        private final ByteBuffer intBuffer;
        private ByteBuffer buffer;
        private long allowedEmitNanos;

        Reader(
                FSDataInputStream stream,
                String name,
                FileType fileType,
                long maxTTL,
                int skip,
                int limitPerLoop,
                int dim,
                int numLoops,
                RateController rateController)
                throws IOException {
            this.stream = stream;
            this.name = name;
            this.fileType = fileType;
            this.maxTTL = maxTTL;
            int width = DIM_BYTES + dim * fileType.width;
            this.startPos = (long) width * skip;
            this.limitPerLoop = limitPerLoop;
            this.dim = dim;
            this.numLoops = numLoops;
            this.rateController = rateController;
            this.loop = 0;
            this.count = 0;
            this.intBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            this.allowedEmitNanos = 0;

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
        public FloatVector read() throws IOException {
            // Here count and loop are set by the previous call to be the correct value
            if (this.loop == this.numLoops) {
                return null;
            }

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
                    LOG.info("EOF reached before dim is read, starting next loop.");
                    this.count = 0;
                    this.loop++;
                    this.stream.seek(startPos);
                    return read();
                } else if (bytesSkipped == 0) {
                    break;
                }
                bytesSkippedTotal += bytesSkipped;
            }
            if (bytesSkippedTotal != DIM_BYTES) {
                throw new IOException(
                        "Failed to skip " + DIM_BYTES + " bytes of vector dimension.");
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
                    LOG.info("EOF reached before user-provided data size, starting next loop.");
                    this.count = 0;
                    this.loop++;
                    this.stream.seek(startPos);
                    return read();
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

            FloatVector vector;
            if (fileType == FileType.F_VEC) {
                float[] array = new float[dim];
                buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(array, 0, dim);
                vector = new FloatVector(vectorId++, array, 0L, maxTTL);
            } else if (fileType == FileType.B_VEC) {
                byte[] array = new byte[dim];
                buffer.get(array, 0, dim);
                float[] floatArray = new float[dim];
                for (int i = 0; i < dim; i++) {
                    floatArray[i] = array[i];
                }
                vector = new FloatVector(vectorId++, floatArray, 0L, maxTTL);
                LOG.trace("Read vector: {}", vector);
            } else {
                throw new RuntimeException("Unsupported file type: " + fileType);
            }

            // Set the count and loop for the next call
            this.count++;
            if (this.count == this.limitPerLoop) {
                this.count = 0;
                this.loop++;
                this.stream.seek(startPos);
            }

            // Rate control
            long currentNanos = System.nanoTime();

            if (rateController != null) {
                long delay = rateController.getDelayNanos(vectorId);
                long delayRefreshThresholdNanos =
                        Math.max(DELAY_REFRESH_THRESHOLD.toNanos(), delay);
                if (allowedEmitNanos == 0
                        || currentNanos - allowedEmitNanos >= delayRefreshThresholdNanos) {
                    LOG.info(
                            "FloatVectorBinaryInputFormat.Reader({}): Resetting allowedEmitNanos to current time.",
                            name);
                    allowedEmitNanos = currentNanos;
                }
                while (currentNanos < allowedEmitNanos) {
                    currentNanos = System.nanoTime();
                }
                allowedEmitNanos += delay;
            }
            vector.setEventTime(System.currentTimeMillis());
            return vector;
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
