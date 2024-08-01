package cn.edu.zju.daily;

import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

public class TableFactoryHandleTest {

    @Test
    void test() {
        RocksDB.loadLibrary();

        final double bitsPerKey = 8;
        final boolean blockBasedMode = true;
        BloomFilter bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
        BlockBasedTableConfig config = new BlockBasedTableConfig();
        config.setFilterPolicy(bloomFilter);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        options.setTableFormatConfig(config);
    }
}
