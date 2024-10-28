package cn.edu.zju.daily.data.source.format;

import cn.edu.zju.daily.data.vector.HDFSVectorParser;
import cn.edu.zju.daily.data.vector.VectorData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

@Slf4j
public class FloatVectorInputFormat extends SimpleStreamFormat<VectorData> {

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";
    private final String charsetName;
    private final long maxTTL;

    public FloatVectorInputFormat(long maxTTL) {
        this(DEFAULT_CHARSET_NAME, maxTTL);
    }

    FloatVectorInputFormat(String charsetName, long maxTTL) {
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
    public TypeInformation<VectorData> getProducedType() {
        return TypeInformation.of(VectorData.class);
    }

    public static final class Reader implements StreamFormat.Reader<VectorData> {

        private final BufferedReader reader;
        private final HDFSVectorParser parser;
        private final long maxTTL;

        Reader(BufferedReader reader, long maxTTL) {
            this.reader = reader;
            this.parser = new HDFSVectorParser();
            this.maxTTL = maxTTL;
            LOG.info("VectorDataInputFormat.Reader created with maxTTL = {}.", maxTTL);
        }

        @Override
        public VectorData read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            VectorData data = parser.parseVector(line);
            data.setTTL(maxTTL); // currently set to max TTL
            LOG.trace("Read vector: {}", data);
            return data;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
