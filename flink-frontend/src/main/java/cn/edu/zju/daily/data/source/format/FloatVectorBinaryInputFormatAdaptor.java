package cn.edu.zju.daily.data.source.format;

import static cn.edu.zju.daily.data.source.format.FloatVectorBinaryInputFormat.DIM_BYTES;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;

/**
 * This adaptor converts FloatVectorBinaryInputFormat to BulkFormat, while supporting adjusting the
 * bulk size.
 */
public class FloatVectorBinaryInputFormatAdaptor
        implements BulkFormat<FloatVector, FileSourceSplit> {

    private final long bulkSizeInBytes;
    private final StreamFormatAdapter<FloatVector> wrapee;

    /**
     * Constructor of FloatVectorBinaryInputFormatAdaptor.
     *
     * @param streamFormat FloatVectorBinaryInputFormat
     * @param bulkSize number of vectors to read in a batch
     * @param dim vector dimension
     */
    public FloatVectorBinaryInputFormatAdaptor(
            FloatVectorBinaryInputFormat streamFormat, int bulkSize, int dim) {
        this.wrapee = new StreamFormatAdapter<>(streamFormat);
        this.bulkSizeInBytes =
                bulkSize * (DIM_BYTES + dim * (long) streamFormat.getFileType().width);
    }

    @Override
    public Reader<FloatVector> createReader(Configuration config, FileSourceSplit split)
            throws IOException {
        Configuration clone = config.clone();
        clone.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(bulkSizeInBytes));
        return wrapee.createReader(clone, split);
    }

    @Override
    public Reader<FloatVector> restoreReader(Configuration config, FileSourceSplit split)
            throws IOException {
        Configuration clone = config.clone();
        clone.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(bulkSizeInBytes));
        return wrapee.restoreReader(clone, split);
    }

    @Override
    public boolean isSplittable() {
        return wrapee.isSplittable();
    }

    @Override
    public TypeInformation<FloatVector> getProducedType() {
        return wrapee.getProducedType();
    }
}
