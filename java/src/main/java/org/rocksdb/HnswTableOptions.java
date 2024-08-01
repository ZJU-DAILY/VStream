package org.rocksdb;

public class HnswTableOptions extends BlockBasedTableConfig {
    private long dim;
    private SpaceType spaceType;
    private long M;

    public HnswTableOptions() {
        super();
    }

    @Override
    public HnswTableOptions setBlockSize(final long blockSize) {
        super.setBlockSize(blockSize);
        return this;
    }

    public long dim() {
        return dim;
    }

    public HnswTableOptions setDim(final long dim) {
        this.dim = dim;
        return this;
    }

    public SpaceType space() {
        return spaceType;
    }

    public HnswTableOptions setSpace(final SpaceType spaceType) {
        this.spaceType = spaceType;
        return this;
    }

    public long M() {
        return M;
    }

    public HnswTableOptions setM(final long M) {
        this.M = M;
        return this;
    }

    @Override
    protected long newTableFactoryHandle() {
        final long filterPolicyHandle;
        if (filterPolicy() != null) {
            filterPolicyHandle = filterPolicy().nativeHandle_;
        } else {
            filterPolicyHandle = 0;
        }

        final long blockCacheHandle;
        if (blockCache != null) {
            blockCacheHandle = blockCache.nativeHandle_;
        } else {
            blockCacheHandle = 0;
        }

        final long persistentCacheHandle;
        if (persistentCache != null) {
            persistentCacheHandle = persistentCache.nativeHandle_;
        } else {
            persistentCacheHandle = 0;
        }
        return newTableFactoryHandle(cacheIndexAndFilterBlocks(), cacheIndexAndFilterBlocksWithHighPriority(),
                                     pinL0FilterAndIndexBlocksInCache(), pinTopLevelIndexAndFilter(),
                                     indexType().getValue(), dataBlockIndexType().getValue(),
                                     dataBlockHashTableUtilRatio(), checksumType().getValue(), noBlockCache(),
                                     blockCacheHandle, persistentCacheHandle, blockSize(), blockSizeDeviation(),
                                     blockRestartInterval(), indexBlockRestartInterval(), metadataBlockSize(),
                                     partitionFilters(), optimizeFiltersForMemory(), useDeltaEncoding(),
                                     filterPolicyHandle, wholeKeyFiltering(), verifyCompression(), readAmpBytesPerBit(),
                                     formatVersion(), enableIndexCompression(), blockAlign(),
                                     indexShortening().getValue(), blockCacheSize(), cacheNumShardBits(), dim,
                                     SpaceType.toByte(spaceType), M);
    }

    private native long newTableFactoryHandle(final boolean cacheIndexAndFilterBlocks,
                                              final boolean cacheIndexAndFilterBlocksWithHighPriority,
                                              final boolean pinL0FilterAndIndexBlocksInCache,
                                              final boolean pinTopLevelIndexAndFilter, final byte indexTypeValue,
                                              final byte dataBlockIndexTypeValue,
                                              final double dataBlockHashTableUtilRatio, final byte checksumTypeValue,
                                              final boolean noBlockCache, final long blockCacheHandle,
                                              final long persistentCacheHandle, final long blockSize,
                                              final int blockSizeDeviation, final int blockRestartInterval,
                                              final int indexBlockRestartInterval, final long metadataBlockSize,
                                              final boolean partitionFilters, final boolean optimizeFiltersForMemory,
                                              final boolean useDeltaEncoding, final long filterPolicyHandle,
                                              final boolean wholeKeyFiltering, final boolean verifyCompression,
                                              final int readAmpBytesPerBit, final int formatVersion,
                                              final boolean enableIndexCompression, final boolean blockAlign,
                                              final byte indexShortening, @Deprecated final long blockCacheSize,
                                              @Deprecated final int blockCacheNumShardBits, long dim,
                                              byte spaceType, long M);
}
