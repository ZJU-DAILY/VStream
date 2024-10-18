package cn.edu.zju.daily.data.source;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.HDFSVectorParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FloatVectorInputFormat extends SimpleStreamFormat<FloatVector> {

    private static final Logger LOG = LoggerFactory.getLogger(FloatVectorInputFormat.class);
    public static final String DEFAULT_CHARSET_NAME = "UTF-8";
    private final String charsetName;
    private final long maxTTL;

    public FloatVectorInputFormat(long maxTTL) {
        this(DEFAULT_CHARSET_NAME, maxTTL);
    }

    public FloatVectorInputFormat(String charsetName, long maxTTL) {
        this.charsetName = charsetName;
        this.maxTTL = maxTTL;
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, charsetName));
        return new Reader(reader, maxTTL);
    }

    @Override
    public TypeInformation<FloatVector> getProducedType() {
        return TypeInformation.of(FloatVector.class);
    }

    public static final class Reader implements StreamFormat.Reader<FloatVector> {

        private final BufferedReader reader;
        private final HDFSVectorParser parser;
        private final long maxTTL;

        Reader(BufferedReader reader, long maxTTL) {
            this.reader = reader;
            this.parser = new HDFSVectorParser();
            this.maxTTL = maxTTL;
            LOG.info("FloatVectorInputFormat.Reader created with maxTTL = {}.", maxTTL);
        }

        @Override
        public FloatVector read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            FloatVector vector = parser.parseVector(line);
            vector.setTTL(maxTTL); // currently set to max TTL
            LOG.trace("Read vector: {}", vector);
            return vector;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
