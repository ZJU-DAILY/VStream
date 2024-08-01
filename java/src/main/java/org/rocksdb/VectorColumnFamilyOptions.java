// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Paths;
import java.util.*;

/**
 * VectorColumnFamilyOptions to control the behavior of a vectorbackend.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 * <p>
 * As a descendant of {@link AbstractNativeReference}, this class is {@link AutoCloseable}
 * and will be automatically released if opened in the preamble of a try with resources block.
 */
public class VectorColumnFamilyOptions extends RocksObject implements
        VectorColumnFamilyOptionsInterface<VectorColumnFamilyOptions>,
        MutableVectorColumnFamilyOptionsInterface<VectorColumnFamilyOptions> {
    static {
        RocksDB.loadLibrary();
    }

    public VectorColumnFamilyOptions() {
        super(newVectorColumnFamilyOptions());
    }

    public VectorColumnFamilyOptions(final ColumnFamilyOptions other) {
        super(copyColumnFamilyOptions(other.nativeHandle_));
    }

    public VectorColumnFamilyOptions(final VectorColumnFamilyOptions other) {
        super(copyVectorColumnFamilyOptions(other.nativeHandle_));
        this.memTableConfig_ = other.memTableConfig_;
        this.tableFormatConfig_ = other.tableFormatConfig_;
        this.comparator_ = other.comparator_;
        this.compactionFilter_ = other.compactionFilter_;
        this.compactionFilterFactory_ = other.compactionFilterFactory_;
        this.compactionOptionsUniversal_ = other.compactionOptionsUniversal_;
        this.compactionOptionsFIFO_ = other.compactionOptionsFIFO_;
        this.bottommostCompressionOptions_ = other.bottommostCompressionOptions_;
        this.compressionOptions_ = other.compressionOptions_;
        this.compactionThreadLimiter_ = other.compactionThreadLimiter_;
        this.sstPartitionerFactory_ = other.sstPartitionerFactory_;
    }

    public VectorColumnFamilyOptions(final VectorOptions options) {
        super(newVectorColumnFamilyOptionsFromOptions(options.nativeHandle_));
    }

    VectorColumnFamilyOptions(final long handle) {
        super(handle);
    }

    private static native long getVectorColumnFamilyOptionsFromProps(final long cfgHandle, String optString);

    private static native long getVectorColumnFamilyOptionsFromProps(final String optString);

    private static native long newVectorColumnFamilyOptions();

    private static native long copyColumnFamilyOptions(final long handle);

    private static native long copyVectorColumnFamilyOptions(final long handle);

    private static native long newVectorColumnFamilyOptionsFromOptions(final long optionsHandle);

    /**
     * <p>Method to get a options instance by using pre-configured
     * property values. If one or many values are undefined in
     * the context of RocksDB the method will return a null
     * value.</p>
     *
     * <p><strong>Note</strong>: Property keys can be derived from
     * getter methods within the options class. Example: the method
     * {@code writeBufferSize()} has a property key:
     * {@code write_buffer_size}.</p>
     *
     * @param properties {@link java.util.Properties} instance.
     *
     * @return {@link org.rocksdb.VectorColumnFamilyOptions instance}
     *     or null.
     *
     * @throws java.lang.IllegalArgumentException if null or empty
     *     {@link Properties} instance is passed to the method call.
     */
    public static VectorColumnFamilyOptions getVectorColumnFamilyOptionsFromProps(
            final Properties properties) {
        VectorColumnFamilyOptions vectorColumnFamilyOptions = null;
        final long handle =
                getVectorColumnFamilyOptionsFromProps(VectorOptions.getOptionStringFromProps(properties));
        if (handle != 0) {
            vectorColumnFamilyOptions = new VectorColumnFamilyOptions(handle);
        }
        return vectorColumnFamilyOptions;
    }

    /**
     * <p>Method to get a options instance by using pre-configured
     * property values. If one or many values are undefined in
     * the context of RocksDB the method will return a null
     * value.</p>
     *
     * <p><strong>Note</strong>: Property keys can be derived from
     * getter methods within the options class. Example: the method
     * {@code writeBufferSize()} has a property key:
     * {@code write_buffer_size}.</p>
     *
     * @param cfgOpts  ConfigOptions controlling how the properties are parsed.
     * @param properties {@link java.util.Properties} instance.
     *
     * @return {@link org.rocksdb.VectorColumnFamilyOptions instance}
     *     or null.
     *
     * @throws java.lang.IllegalArgumentException if null or empty
     *     {@link Properties} instance is passed to the method call.
     */
    public static VectorColumnFamilyOptions getVectorColumnFamilyOptionsFromProps(
            final ConfigOptions cfgOpts, final Properties properties) {
        VectorColumnFamilyOptions vectorColumnFamilyOptions = null;
        final long handle = getVectorColumnFamilyOptionsFromProps(
                cfgOpts.nativeHandle_, VectorOptions.getOptionStringFromProps(properties));
        if (handle != 0){
            vectorColumnFamilyOptions = new VectorColumnFamilyOptions(handle);
        }
        return vectorColumnFamilyOptions;
    }

    @Override
    public VectorColumnFamilyOptions oldDefaults(final int majorVersion, final int minorVersion) {
        oldDefaults(nativeHandle_, majorVersion, minorVersion);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeForSmallDb() {
        optimizeForSmallDb(nativeHandle_);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeForSmallDb(final Cache cache) {
        optimizeForSmallDb(nativeHandle_, cache.getNativeHandle());
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeForPointLookup(
            final long blockCacheSizeMb) {
        optimizeForPointLookup(nativeHandle_,
                               blockCacheSizeMb);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeLevelStyleCompaction() {
        optimizeLevelStyleCompaction(nativeHandle_,
                                     DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeLevelStyleCompaction(
            final long memtableMemoryBudget) {
        optimizeLevelStyleCompaction(nativeHandle_,
                                     memtableMemoryBudget);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeUniversalStyleCompaction() {
        optimizeUniversalStyleCompaction(nativeHandle_,
                                         DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions optimizeUniversalStyleCompaction(
            final long memtableMemoryBudget) {
        optimizeUniversalStyleCompaction(nativeHandle_,
                                         memtableMemoryBudget);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setComparator(
            final BuiltinComparator builtinComparator) {
        assert(isOwningHandle());
        setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setComparator(
            final AbstractComparator comparator) {
        assert (isOwningHandle());
        setComparatorHandle(nativeHandle_, comparator.nativeHandle_,
                            comparator.getComparatorType().getValue());
        comparator_ = comparator;
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setMergeOperatorName(final String name) {
        assert (isOwningHandle());
        if (name == null) {
            throw new IllegalArgumentException(
                    "Merge operator name must not be null.");
        }
        setMergeOperatorName(nativeHandle_, name);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setMergeOperator(
            final MergeOperator mergeOperator) {
        setMergeOperator(nativeHandle_, mergeOperator.nativeHandle_);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setCompactionFilter(
            final AbstractCompactionFilter<? extends AbstractSlice<?>>
                    compactionFilter) {
        setCompactionFilterHandle(nativeHandle_, compactionFilter.nativeHandle_);
        compactionFilter_ = compactionFilter;
        return this;
    }

    @Override
    public AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter() {
        assert (isOwningHandle());
        return compactionFilter_;
    }

    @Override
    public VectorColumnFamilyOptions setCompactionFilterFactory(final AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory) {
        assert (isOwningHandle());
        setCompactionFilterFactoryHandle(nativeHandle_, compactionFilterFactory.nativeHandle_);
        compactionFilterFactory_ = compactionFilterFactory;
        return this;
    }

    @Override
    public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory() {
        assert (isOwningHandle());
        return compactionFilterFactory_;
    }

    @Override
    public VectorColumnFamilyOptions setWriteBufferSize(final long writeBufferSize) {
        assert(isOwningHandle());
        setWriteBufferSize(nativeHandle_, writeBufferSize);
        return this;
    }

    @Override
    public long writeBufferSize()  {
        assert(isOwningHandle());
        return writeBufferSize(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxWriteBufferNumber(
            final int maxWriteBufferNumber) {
        assert(isOwningHandle());
        setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
        return this;
    }

    @Override
    public int maxWriteBufferNumber() {
        assert(isOwningHandle());
        return maxWriteBufferNumber(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMinWriteBufferNumberToMerge(
            final int minWriteBufferNumberToMerge) {
        setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
        return this;
    }

    @Override
    public int minWriteBufferNumberToMerge() {
        return minWriteBufferNumberToMerge(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions useFixedLengthPrefixExtractor(final int n) {
        assert(isOwningHandle());
        useFixedLengthPrefixExtractor(nativeHandle_, n);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions useCappedPrefixExtractor(final int n) {
        assert(isOwningHandle());
        useCappedPrefixExtractor(nativeHandle_, n);
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setCompressionType(
            final CompressionType compressionType) {
        setCompressionType(nativeHandle_, compressionType.getValue());
        return this;
    }

    @Override
    public CompressionType compressionType() {
        return CompressionType.getCompressionType(compressionType(nativeHandle_));
    }

    @Override
    public int flushThreshold() {
        return flushThreshold(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setFlushThreshold(int flushThreshold) {
        setFlushThreshold(nativeHandle_, flushThreshold);
        return this;
    }


    @Override
    public VectorColumnFamilyOptions setCompressionPerLevel(
            final List<CompressionType> compressionLevels) {
        final byte[] byteCompressionTypes = new byte[
                compressionLevels.size()];
        for (int i = 0; i < compressionLevels.size(); i++) {
            byteCompressionTypes[i] = compressionLevels.get(i).getValue();
        }
        setCompressionPerLevel(nativeHandle_, byteCompressionTypes);
        return this;
    }

    @Override
    public List<CompressionType> compressionPerLevel() {
        final byte[] byteCompressionTypes =
                compressionPerLevel(nativeHandle_);
        final List<CompressionType> compressionLevels = new ArrayList<>();
        for (final byte byteCompressionType : byteCompressionTypes) {
            compressionLevels.add(CompressionType.getCompressionType(
                    byteCompressionType));
        }
        return compressionLevels;
    }

    @Override
    public VectorColumnFamilyOptions setBottommostCompressionType(
            final CompressionType bottommostCompressionType) {
        setBottommostCompressionType(nativeHandle_,
                                     bottommostCompressionType.getValue());
        return this;
    }

    @Override
    public CompressionType bottommostCompressionType() {
        return CompressionType.getCompressionType(
                bottommostCompressionType(nativeHandle_));
    }

    @Override
    public VectorColumnFamilyOptions setBottommostCompressionOptions(
            final CompressionOptions bottommostCompressionOptions) {
        setBottommostCompressionOptions(nativeHandle_,
                                        bottommostCompressionOptions.nativeHandle_);
        this.bottommostCompressionOptions_ = bottommostCompressionOptions;
        return this;
    }

    @Override
    public CompressionOptions bottommostCompressionOptions() {
        return this.bottommostCompressionOptions_;
    }

    @Override
    public VectorColumnFamilyOptions setCompressionOptions(
            final CompressionOptions compressionOptions) {
        setCompressionOptions(nativeHandle_, compressionOptions.nativeHandle_);
        this.compressionOptions_ = compressionOptions;
        return this;
    }

    @Override
    public CompressionOptions compressionOptions() {
        return this.compressionOptions_;
    }

    @Override
    public VectorColumnFamilyOptions setNumLevels(final int numLevels) {
        setNumLevels(nativeHandle_, numLevels);
        return this;
    }

    @Override
    public int numLevels() {
        return numLevels(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevelZeroFileNumCompactionTrigger(
            final int numFiles) {
        setLevelZeroFileNumCompactionTrigger(
                nativeHandle_, numFiles);
        return this;
    }

    @Override
    public int levelZeroFileNumCompactionTrigger() {
        return levelZeroFileNumCompactionTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevelZeroSlowdownWritesTrigger(
            final int numFiles) {
        setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
        return this;
    }

    @Override
    public int levelZeroSlowdownWritesTrigger() {
        return levelZeroSlowdownWritesTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevelZeroStopWritesTrigger(final int numFiles) {
        setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
        return this;
    }

    @Override
    public int levelZeroStopWritesTrigger() {
        return levelZeroStopWritesTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTargetFileSizeBase(
            final long targetFileSizeBase) {
        setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
        return this;
    }

    @Override
    public long targetFileSizeBase() {
        return targetFileSizeBase(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTargetFileSizeMultiplier(
            final int multiplier) {
        setTargetFileSizeMultiplier(nativeHandle_, multiplier);
        return this;
    }

    @Override
    public int targetFileSizeMultiplier() {
        return targetFileSizeMultiplier(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxBytesForLevelBase(
            final long maxBytesForLevelBase) {
        setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
        return this;
    }

    @Override
    public long maxBytesForLevelBase() {
        return maxBytesForLevelBase(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevelCompactionDynamicLevelBytes(
            final boolean enableLevelCompactionDynamicLevelBytes) {
        setLevelCompactionDynamicLevelBytes(nativeHandle_,
                                            enableLevelCompactionDynamicLevelBytes);
        return this;
    }

    @Override
    public boolean levelCompactionDynamicLevelBytes() {
        return levelCompactionDynamicLevelBytes(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxBytesForLevelMultiplier(final double multiplier) {
        setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
        return this;
    }

    @Override
    public double maxBytesForLevelMultiplier() {
        return maxBytesForLevelMultiplier(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxCompactionBytes(final long maxCompactionBytes) {
        setMaxCompactionBytes(nativeHandle_, maxCompactionBytes);
        return this;
    }

    @Override
    public long maxCompactionBytes() {
        return maxCompactionBytes(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setArenaBlockSize(
            final long arenaBlockSize) {
        setArenaBlockSize(nativeHandle_, arenaBlockSize);
        return this;
    }

    @Override
    public long arenaBlockSize() {
        return arenaBlockSize(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setDisableAutoCompactions(
            final boolean disableAutoCompactions) {
        setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
        return this;
    }

    @Override
    public boolean disableAutoCompactions() {
        return disableAutoCompactions(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setCompactionStyle(
            final CompactionStyle compactionStyle) {
        setCompactionStyle(nativeHandle_, compactionStyle.getValue());
        return this;
    }

    @Override
    public CompactionStyle compactionStyle() {
        return CompactionStyle.fromValue(compactionStyle(nativeHandle_));
    }

    @Override
    public VectorColumnFamilyOptions setMaxTableFilesSizeFIFO(
            final long maxTableFilesSize) {
        assert(maxTableFilesSize > 0); // unsigned native type
        assert(isOwningHandle());
        setMaxTableFilesSizeFIFO(nativeHandle_, maxTableFilesSize);
        return this;
    }

    @Override
    public long maxTableFilesSizeFIFO() {
        return maxTableFilesSizeFIFO(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxSequentialSkipInIterations(
            final long maxSequentialSkipInIterations) {
        setMaxSequentialSkipInIterations(nativeHandle_,
                                         maxSequentialSkipInIterations);
        return this;
    }

    @Override
    public long maxSequentialSkipInIterations() {
        return maxSequentialSkipInIterations(nativeHandle_);
    }

    @Override
    public MemTableConfig memTableConfig() {
        return this.memTableConfig_;
    }

    @Override
    public VectorColumnFamilyOptions setMemTableConfig(
            final MemTableConfig memTableConfig) {
        setMemTableFactory(
                nativeHandle_, memTableConfig.newMemTableFactoryHandle());
        this.memTableConfig_ = memTableConfig;
        return this;
    }

    @Override
    public String memTableFactoryName() {
        assert(isOwningHandle());
        return memTableFactoryName(nativeHandle_);
    }

    @Override
    public TableFormatConfig tableFormatConfig() {
        return this.tableFormatConfig_;
    }

    @Override
    public VectorColumnFamilyOptions setTableFormatConfig(
            final TableFormatConfig tableFormatConfig) {
        setTableFactory(nativeHandle_, tableFormatConfig.newTableFactoryHandle());
        this.tableFormatConfig_ = tableFormatConfig;
        return this;
    }

    @Override
    public String tableFactoryName() {
        assert(isOwningHandle());
        return tableFactoryName(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setCfPaths(final Collection<DbPath> cfPaths) {
        assert (isOwningHandle());

        final int len = cfPaths.size();
        final String[] paths = new String[len];
        final long[] targetSizes = new long[len];

        int i = 0;
        for (final DbPath dbPath : cfPaths) {
            paths[i] = dbPath.path.toString();
            targetSizes[i] = dbPath.targetSize;
            i++;
        }
        setCfPaths(nativeHandle_, paths, targetSizes);
        return this;
    }

    @Override
    public List<DbPath> cfPaths() {
        final int len = (int) cfPathsLen(nativeHandle_);

        if (len == 0) {
            return Collections.emptyList();
        }

        final String[] paths = new String[len];
        final long[] targetSizes = new long[len];

        cfPaths(nativeHandle_, paths, targetSizes);

        final List<DbPath> cfPaths = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            cfPaths.add(new DbPath(Paths.get(paths[i]), targetSizes[i]));
        }

        return cfPaths;
    }

    @Override
    public VectorColumnFamilyOptions setInplaceUpdateSupport(
            final boolean inplaceUpdateSupport) {
        setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
        return this;
    }

    @Override
    public boolean inplaceUpdateSupport() {
        return inplaceUpdateSupport(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setInplaceUpdateNumLocks(
            final long inplaceUpdateNumLocks) {
        setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
        return this;
    }

    @Override
    public long inplaceUpdateNumLocks() {
        return inplaceUpdateNumLocks(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMemtablePrefixBloomSizeRatio(
            final double memtablePrefixBloomSizeRatio) {
        setMemtablePrefixBloomSizeRatio(nativeHandle_, memtablePrefixBloomSizeRatio);
        return this;
    }

    @Override
    public double memtablePrefixBloomSizeRatio() {
        return memtablePrefixBloomSizeRatio(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setExperimentalMempurgeThreshold(
            final double experimentalMempurgeThreshold) {
        setExperimentalMempurgeThreshold(nativeHandle_, experimentalMempurgeThreshold);
        return this;
    }

    @Override
    public double experimentalMempurgeThreshold() {
        return experimentalMempurgeThreshold(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMemtableWholeKeyFiltering(final boolean memtableWholeKeyFiltering) {
        setMemtableWholeKeyFiltering(nativeHandle_, memtableWholeKeyFiltering);
        return this;
    }

    @Override
    public boolean memtableWholeKeyFiltering() {
        return memtableWholeKeyFiltering(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setBloomLocality(final int bloomLocality) {
        setBloomLocality(nativeHandle_, bloomLocality);
        return this;
    }

    @Override
    public int bloomLocality() {
        return bloomLocality(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxSuccessiveMerges(
            final long maxSuccessiveMerges) {
        setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
        return this;
    }

    @Override
    public long maxSuccessiveMerges() {
        return maxSuccessiveMerges(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setOptimizeFiltersForHits(
            final boolean optimizeFiltersForHits) {
        setOptimizeFiltersForHits(nativeHandle_, optimizeFiltersForHits);
        return this;
    }

    @Override
    public boolean optimizeFiltersForHits() {
        return optimizeFiltersForHits(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMemtableHugePageSize(final long memtableHugePageSize) {
        setMemtableHugePageSize(nativeHandle_,
                                memtableHugePageSize);
        return this;
    }

    @Override
    public long memtableHugePageSize() {
        return memtableHugePageSize(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setSoftPendingCompactionBytesLimit(
            final long softPendingCompactionBytesLimit) {
        setSoftPendingCompactionBytesLimit(nativeHandle_,
                                           softPendingCompactionBytesLimit);
        return this;
    }

    @Override
    public long softPendingCompactionBytesLimit() {
        return softPendingCompactionBytesLimit(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setHardPendingCompactionBytesLimit(
            final long hardPendingCompactionBytesLimit) {
        setHardPendingCompactionBytesLimit(nativeHandle_, hardPendingCompactionBytesLimit);
        return this;
    }

    @Override
    public long hardPendingCompactionBytesLimit() {
        return hardPendingCompactionBytesLimit(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevel0FileNumCompactionTrigger(
            final int level0FileNumCompactionTrigger) {
        setLevel0FileNumCompactionTrigger(nativeHandle_, level0FileNumCompactionTrigger);
        return this;
    }

    @Override
    public int level0FileNumCompactionTrigger() {
        return level0FileNumCompactionTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevel0SlowdownWritesTrigger(final int level0SlowdownWritesTrigger) {
        setLevel0SlowdownWritesTrigger(nativeHandle_, level0SlowdownWritesTrigger);
        return this;
    }

    @Override
    public int level0SlowdownWritesTrigger() {
        return level0SlowdownWritesTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setLevel0StopWritesTrigger(final int level0StopWritesTrigger) {
        setLevel0StopWritesTrigger(nativeHandle_, level0StopWritesTrigger);
        return this;
    }

    @Override
    public int level0StopWritesTrigger() {
        return level0StopWritesTrigger(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxBytesForLevelMultiplierAdditional(
            final int[] maxBytesForLevelMultiplierAdditional) {
        setMaxBytesForLevelMultiplierAdditional(nativeHandle_, maxBytesForLevelMultiplierAdditional);
        return this;
    }

    @Override
    public int[] maxBytesForLevelMultiplierAdditional() {
        return maxBytesForLevelMultiplierAdditional(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setParanoidFileChecks(final boolean paranoidFileChecks) {
        setParanoidFileChecks(nativeHandle_, paranoidFileChecks);
        return this;
    }

    @Override
    public boolean paranoidFileChecks() {
        return paranoidFileChecks(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setMaxWriteBufferNumberToMaintain(
            final int maxWriteBufferNumberToMaintain) {
        setMaxWriteBufferNumberToMaintain(
                nativeHandle_, maxWriteBufferNumberToMaintain);
        return this;
    }

    @Override
    public int maxWriteBufferNumberToMaintain() {
        return maxWriteBufferNumberToMaintain(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setCompactionPriority(
            final CompactionPriority compactionPriority) {
        setCompactionPriority(nativeHandle_, compactionPriority.getValue());
        return this;
    }

    @Override
    public CompactionPriority compactionPriority() {
        return CompactionPriority.getCompactionPriority(
                compactionPriority(nativeHandle_));
    }

    @Override
    public VectorColumnFamilyOptions setReportBgIoStats(final boolean reportBgIoStats) {
        setReportBgIoStats(nativeHandle_, reportBgIoStats);
        return this;
    }

    @Override
    public boolean reportBgIoStats() {
        return reportBgIoStats(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTtl(final long ttl) {
        setTtl(nativeHandle_, ttl);
        return this;
    }

    @Override
    public long ttl() {
        return ttl(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setPeriodicCompactionSeconds(final long periodicCompactionSeconds) {
        setPeriodicCompactionSeconds(nativeHandle_, periodicCompactionSeconds);
        return this;
    }

    @Override
    public long periodicCompactionSeconds() {
        return periodicCompactionSeconds(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setCompactionOptionsUniversal(
            final CompactionOptionsUniversal compactionOptionsUniversal) {
        setCompactionOptionsUniversal(nativeHandle_,
                                      compactionOptionsUniversal.nativeHandle_);
        this.compactionOptionsUniversal_ = compactionOptionsUniversal;
        return this;
    }

    @Override
    public CompactionOptionsUniversal compactionOptionsUniversal() {
        return this.compactionOptionsUniversal_;
    }

    @Override
    public VectorColumnFamilyOptions setCompactionOptionsFIFO(final CompactionOptionsFIFO compactionOptionsFIFO) {
        setCompactionOptionsFIFO(nativeHandle_,
                                 compactionOptionsFIFO.nativeHandle_);
        this.compactionOptionsFIFO_ = compactionOptionsFIFO;
        return this;
    }

    @Override
    public CompactionOptionsFIFO compactionOptionsFIFO() {
        return this.compactionOptionsFIFO_;
    }

    @Override
    public VectorColumnFamilyOptions setForceConsistencyChecks(final boolean forceConsistencyChecks) {
        setForceConsistencyChecks(nativeHandle_, forceConsistencyChecks);
        return this;
    }

    @Override
    public boolean forceConsistencyChecks() {
        return forceConsistencyChecks(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setSstPartitionerFactory(
            final SstPartitionerFactory sstPartitionerFactory) {
        setSstPartitionerFactory(nativeHandle_, sstPartitionerFactory.nativeHandle_);
        this.sstPartitionerFactory_ = sstPartitionerFactory;
        return this;
    }

    @Override
    public VectorColumnFamilyOptions setCompactionThreadLimiter(
            final ConcurrentTaskLimiter compactionThreadLimiter) {
        setCompactionThreadLimiter(nativeHandle_, compactionThreadLimiter.nativeHandle_);
        this.compactionThreadLimiter_ = compactionThreadLimiter;
        return this;
    }

    @Override
    public ConcurrentTaskLimiter compactionThreadLimiter() {
        assert (isOwningHandle());
        return this.compactionThreadLimiter_;
    }

    @Override
    public SstPartitionerFactory sstPartitionerFactory() {
        return sstPartitionerFactory_;
    }

    //
    // BEGIN options for blobs (integrated BlobDB)
    //

    /**
     * When set, large values (blobs) are written to separate blob files, and only
     * pointers to them are stored in SST files. This can reduce write amplification
     * for large-value use cases at the cost of introducing a level of indirection
     * for reads. See also the options min_blob_size, blob_file_size,
     * blob_compression_type, enable_blob_garbage_collection, and
     * blob_garbage_collection_age_cutoff below.
     * <p>
     * Default: false
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param enableBlobFiles true iff blob files should be enabled
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setEnableBlobFiles(final boolean enableBlobFiles) {
        setEnableBlobFiles(nativeHandle_, enableBlobFiles);
        return this;
    }

    /**
     * When set, large values (blobs) are written to separate blob files, and only
     * pointers to them are stored in SST files. This can reduce write amplification
     * for large-value use cases at the cost of introducing a level of indirection
     * for reads. See also the options min_blob_size, blob_file_size,
     * blob_compression_type, enable_blob_garbage_collection, and
     * blob_garbage_collection_age_cutoff below.
     * <p>
     * Default: false
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @return true iff blob files are currently enabled
     */
    public boolean enableBlobFiles() {
        return enableBlobFiles(nativeHandle_);
    }

    /**
     * Set the size of the smallest value to be stored separately in a blob file. Values
     * which have an uncompressed size smaller than this threshold are stored
     * alongside the keys in SST files in the usual fashion. A value of zero for
     * this option means that all values are stored in blob files. Note that
     * enable_blob_files has to be set in order for this option to have any effect.
     * <p>
     * Default: 0
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param minBlobSize the size of the smallest value to be stored separately in a blob file
     * @return these options, updated with the supplied minimum blob size value
     */
    @Override
    public VectorColumnFamilyOptions setMinBlobSize(final long minBlobSize) {
        setMinBlobSize(nativeHandle_, minBlobSize);
        return this;
    }

    /**
     * Get the size of the smallest value to be stored separately in a blob file. Values
     * which have an uncompressed size smaller than this threshold are stored
     * alongside the keys in SST files in the usual fashion. A value of zero for
     * this option means that all values are stored in blob files. Note that
     * enable_blob_files has to be set in order for this option to have any effect.
     * <p>
     * Default: 0
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @return the current minimum blob size
     */
    @Override
    public long minBlobSize() {
        return minBlobSize(nativeHandle_);
    }

    /**
     * Set the size limit for blob files. When writing blob files, a new file is opened
     * once this limit is reached. Note that enable_blob_files has to be set in
     * order for this option to have any effect.
     * <p>
     * Default: 256 MB
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param blobFileSize the new size limit for blob files
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setBlobFileSize(final long blobFileSize) {
        setBlobFileSize(nativeHandle_, blobFileSize);
        return this;
    }

    /**
     * Get the size limit for blob files. When writing blob files, a new file is opened
     * once this limit is reached. Note that enable_blob_files has to be set in
     * order for this option to have any effect.
     * <p>
     * Default: 256 MB
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @return the size limit for blob files
     */
    @Override
    public long blobFileSize() {
        return blobFileSize(nativeHandle_);
    }

    /**
     * Set the compression algorithm to use for large values stored in blob files. Note
     * that enable_blob_files has to be set in order for this option to have any
     * effect.
     * <p>
     * Default: no compression
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param compressionType the compression algorithm to use
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setBlobCompressionType(final CompressionType compressionType) {
        setBlobCompressionType(nativeHandle_, compressionType.getValue());
        return this;
    }

    /**
     * Get the compression algorithm to use for large values stored in blob files. Note
     * that enable_blob_files has to be set in order for this option to have any
     * effect.
     * <p>
     * Default: no compression
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @return the compression algorithm currently in use for blobs
     */
    @Override
    public CompressionType blobCompressionType() {
        return CompressionType.values()[blobCompressionType(nativeHandle_)];
    }

    /**
     * Enable/disable garbage collection of blobs. Blob GC is performed as part of
     * compaction. Valid blobs residing in blob files older than a cutoff get
     * relocated to new files as they are encountered during compaction, which makes
     * it possible to clean up blob files once they contain nothing but
     * obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff below.
     * <p>
     * Default: false
     *
     * @param enableBlobGarbageCollection true iff blob garbage collection is to be enabled
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setEnableBlobGarbageCollection(
            final boolean enableBlobGarbageCollection) {
        setEnableBlobGarbageCollection(nativeHandle_, enableBlobGarbageCollection);
        return this;
    }

    /**
     * Get enabled/disables state for garbage collection of blobs. Blob GC is performed as part of
     * compaction. Valid blobs residing in blob files older than a cutoff get
     * relocated to new files as they are encountered during compaction, which makes
     * it possible to clean up blob files once they contain nothing but
     * obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff below.
     * <p>
     * Default: false
     *
     * @return true iff blob garbage collection is currently enabled
     */
    @Override
    public boolean enableBlobGarbageCollection() {
        return enableBlobGarbageCollection(nativeHandle_);
    }

    /**
     * Set the cutoff in terms of blob file age for garbage collection. Blobs in the
     * oldest N blob files will be relocated when encountered during compaction,
     * where N = garbage_collection_cutoff * number_of_blob_files. Note that
     * enable_blob_garbage_collection has to be set in order for this option to have
     * any effect.
     * <p>
     * Default: 0.25
     *
     * @param blobGarbageCollectionAgeCutoff the new blob garbage collection age cutoff
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setBlobGarbageCollectionAgeCutoff(
            final double blobGarbageCollectionAgeCutoff) {
        setBlobGarbageCollectionAgeCutoff(nativeHandle_, blobGarbageCollectionAgeCutoff);
        return this;
    }

    /**
     * Get the cutoff in terms of blob file age for garbage collection. Blobs in the
     * oldest N blob files will be relocated when encountered during compaction,
     * where N = garbage_collection_cutoff * number_of_blob_files. Note that
     * enable_blob_garbage_collection has to be set in order for this option to have
     * any effect.
     * <p>
     * Default: 0.25
     *
     * @return the current blob garbage collection age cutoff
     */
    @Override
    public double blobGarbageCollectionAgeCutoff() {
        return blobGarbageCollectionAgeCutoff(nativeHandle_);
    }

    /**
     *  If the ratio of garbage in the oldest blob files exceeds this threshold,
     *  targeted compactions are scheduled in order to force garbage collecting
     *  the blob files in question, assuming they are all eligible based on the
     *  value of {@link #blobGarbageCollectionAgeCutoff} above. This option is
     *  currently only supported with leveled compactions.
     * <p>
     *  Note that {@link #enableBlobGarbageCollection} has to be set in order for this
     *  option to have any effect.
     * <p>
     *  Default: 1.0
     * <p>
     * Dynamically changeable through the SetOptions() API
     *
     * @param blobGarbageCollectionForceThreshold new value for the threshold
     * @return the reference to the current options
     */
    @Override
    public VectorColumnFamilyOptions setBlobGarbageCollectionForceThreshold(
            final double blobGarbageCollectionForceThreshold) {
        setBlobGarbageCollectionForceThreshold(nativeHandle_, blobGarbageCollectionForceThreshold);
        return this;
    }

    /**
     * Get the current value for the {@link #blobGarbageCollectionForceThreshold}
     * @return the current threshold at which garbage collection of blobs is forced
     */
    @Override
    public double blobGarbageCollectionForceThreshold() {
        return blobGarbageCollectionForceThreshold(nativeHandle_);
    }

    /**
     * Set compaction readahead for blob files.
     * <p>
     * Default: 0
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param blobCompactionReadaheadSize the compaction readahead for blob files
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setBlobCompactionReadaheadSize(
            final long blobCompactionReadaheadSize) {
        setBlobCompactionReadaheadSize(nativeHandle_, blobCompactionReadaheadSize);
        return this;
    }

    /**
     * Get compaction readahead for blob files.
     *
     * @return the current compaction readahead for blob files
     */
    @Override
    public long blobCompactionReadaheadSize() {
        return blobCompactionReadaheadSize(nativeHandle_);
    }

    /**
     * Set a certain LSM tree level to enable blob files.
     * <p>
     * Default: 0
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param blobFileStartingLevel the starting level to enable blob files
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setBlobFileStartingLevel(final int blobFileStartingLevel) {
        setBlobFileStartingLevel(nativeHandle_, blobFileStartingLevel);
        return this;
    }

    /**
     * Get the starting LSM tree level to enable blob files.
     * <p>
     * Default: 0
     *
     * @return the current LSM tree level to enable blob files.
     */
    @Override
    public int blobFileStartingLevel() {
        return blobFileStartingLevel(nativeHandle_);
    }

    /**
     * Set a certain prepopulate blob cache option.
     * <p>
     * Default: 0
     * <p>
     * Dynamically changeable through
     * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
     *
     * @param prepopulateBlobCache prepopulate the blob cache option
     *
     * @return the reference to the current options.
     */
    @Override
    public VectorColumnFamilyOptions setPrepopulateBlobCache(
            final PrepopulateBlobCache prepopulateBlobCache) {
        setPrepopulateBlobCache(nativeHandle_, prepopulateBlobCache.getValue());
        return this;
    }

    /**
     * Get the prepopulate blob cache option.
     * <p>
     * Default: 0
     *
     * @return the current prepopulate blob cache option.
     */
    @Override
    public PrepopulateBlobCache prepopulateBlobCache() {
        return PrepopulateBlobCache.getPrepopulateBlobCache(prepopulateBlobCache(nativeHandle_));
    }

    //
    // END options for blobs (integrated BlobDB)
    //

    @Override
    public VectorColumnFamilyOptions setMaxElements(long maxElements) {
        setMaxElements(nativeHandle_, maxElements);
        return this;
    }

    @Override
    public long maxElements() {
        return maxElements(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setM(long M) {
        setM(nativeHandle_, M);
        return this;
    }

    @Override
    public long M() {
        return M(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setEfConstruction(long efConstruction) {
        setEfConstruction(nativeHandle_, efConstruction);
        return this;
    }

    @Override
    public long efConstruction() {
        return efConstruction(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setRandomSeed(long randomSeed) {
        setRandomSeed(nativeHandle_, randomSeed);
        return this;
    }

    @Override
    public long randomSeed() {
        return randomSeed(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setVisitListPoolSize(long visitListPoolSize) {
        setVisitListPoolSize(nativeHandle_, visitListPoolSize);
        return this;
    }

    @Override
    public long visitListPoolSize() {
        return visitListPoolSize(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTerminationThreshold(float terminationThreshold) {
        setTerminationThreshold(nativeHandle_, terminationThreshold);
        return this;
    }

    @Override
    public float terminationThreshold() {
        return terminationThreshold(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTerminationWeight(float terminationWeight) {
        setTerminationWeight(nativeHandle_, terminationWeight);
        return this;
    }

    @Override
    public float terminationWeight() {
        return terminationWeight(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setTerminationLowerBound(float terminationLowerBound) {
        setTerminationLowerBound(nativeHandle_, terminationLowerBound);
        return this;
    }

    @Override
    public float terminationLowerBound() {
        return terminationLowerBound(nativeHandle_);
    }


    @Override
    public VectorColumnFamilyOptions setDim(long dim) {
        setDim(nativeHandle_, dim);
        return this;
    }

    @Override
    public long dim() {
        return dim(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setSpace(SpaceType spaceType) {
        setSpace(nativeHandle_, SpaceType.toByte(spaceType));
        return this;
    }

    @Override
    public byte space() {
        return space(nativeHandle_);
    }

    @Override
    public VectorColumnFamilyOptions setAllowReplaceDeleted(boolean allowReplaceDeleted) {
        setAllowReplaceDeleted(nativeHandle_, allowReplaceDeleted);
        return this;
    }

    @Override
    public boolean allowReplaceDeleted() {
        return allowReplaceDeleted(nativeHandle_);
    }

    @Override
    protected final native void disposeInternal(final long handle);

    private native void setMaxElements(long handle, long maxElements);

    private native long maxElements(long handle);

    private native void setM(long handle, long M);

    private native long M(long handle);

    private native void setEfConstruction(long handle, long efConstruction);

    private native long efConstruction(long handle);

    private native void setRandomSeed(long handle, long randomSeed);

    private native long randomSeed(long handle);

    private native void setVisitListPoolSize(long handle, long visitListPoolSize);

    private native long visitListPoolSize(long handle);


    private native void setTerminationThreshold(long handle, float terminationThreshold);

    private native float terminationThreshold(long handle);

    private native void setTerminationWeight(long handle, float terminationWeight);

    private native float terminationWeight(long handle);

    private native void setTerminationLowerBound(long handle, float terminationLowerBound);

    private native float terminationLowerBound(long handle);

    private native void setDim(long handle, long dim);

    private native long dim(long handle);

    private native void setSpace(long handle, byte spaceType);

    private native byte space(long handle);

    private native void setAllowReplaceDeleted(long handle, boolean allowReplaceDeleted);

    private native boolean allowReplaceDeleted(long handle);

    private static native void oldDefaults(
            final long handle, final int majorVersion, final int minorVersion);
    private native void optimizeForSmallDb(final long handle);
    private static native void optimizeForSmallDb(final long handle, final long cacheHandle);
    private native void optimizeForPointLookup(long handle,
                                               long blockCacheSizeMb);
    private native void optimizeLevelStyleCompaction(long handle,
                                                     long memtableMemoryBudget);
    private native void optimizeUniversalStyleCompaction(long handle,
                                                         long memtableMemoryBudget);
    private native void setComparatorHandle(long handle, int builtinComparator);
    private native void setComparatorHandle(long optHandle,
                                            long comparatorHandle, byte comparatorType);
    private native void setMergeOperatorName(long handle, String name);
    private native void setMergeOperator(long handle, long mergeOperatorHandle);
    private native void setCompactionFilterHandle(long handle,
                                                  long compactionFilterHandle);
    private native void setCompactionFilterFactoryHandle(long handle,
                                                         long compactionFilterFactoryHandle);
    private native void setWriteBufferSize(long handle, long writeBufferSize)
            throws IllegalArgumentException;
    private native long writeBufferSize(long handle);
    private native void setMaxWriteBufferNumber(
            long handle, int maxWriteBufferNumber);
    private native int maxWriteBufferNumber(long handle);
    private native void setMinWriteBufferNumberToMerge(
            long handle, int minWriteBufferNumberToMerge);
    private native int minWriteBufferNumberToMerge(long handle);
    private native void setCompressionType(long handle, byte compressionType);
    private native byte compressionType(long handle);
    private native void setCompressionPerLevel(long handle,
                                               byte[] compressionLevels);
    private native byte[] compressionPerLevel(long handle);
    private native void setBottommostCompressionType(long handle,
                                                     byte bottommostCompressionType);
    private native byte bottommostCompressionType(long handle);
    private native void setBottommostCompressionOptions(final long handle,
                                                        final long bottommostCompressionOptionsHandle);
    private native void setCompressionOptions(long handle,
                                              long compressionOptionsHandle);
    private native void useFixedLengthPrefixExtractor(
            long handle, int prefixLength);
    private native void useCappedPrefixExtractor(
            long handle, int prefixLength);
    private native void setNumLevels(
            long handle, int numLevels);
    private native int numLevels(long handle);
    private native void setLevelZeroFileNumCompactionTrigger(
            long handle, int numFiles);
    private native int levelZeroFileNumCompactionTrigger(long handle);
    private native void setLevelZeroSlowdownWritesTrigger(
            long handle, int numFiles);
    private native int levelZeroSlowdownWritesTrigger(long handle);
    private native void setLevelZeroStopWritesTrigger(
            long handle, int numFiles);
    private native int levelZeroStopWritesTrigger(long handle);
    private native void setTargetFileSizeBase(
            long handle, long targetFileSizeBase);
    private native long targetFileSizeBase(long handle);
    private native void setTargetFileSizeMultiplier(
            long handle, int multiplier);
    private native int targetFileSizeMultiplier(long handle);
    private native void setMaxBytesForLevelBase(
            long handle, long maxBytesForLevelBase);
    private native long maxBytesForLevelBase(long handle);
    private native void setLevelCompactionDynamicLevelBytes(
            long handle, boolean enableLevelCompactionDynamicLevelBytes);
    private native boolean levelCompactionDynamicLevelBytes(
            long handle);
    private native void setMaxBytesForLevelMultiplier(long handle, double multiplier);
    private native double maxBytesForLevelMultiplier(long handle);
    private native void setMaxCompactionBytes(long handle, long maxCompactionBytes);
    private native long maxCompactionBytes(long handle);
    private native void setArenaBlockSize(
            long handle, long arenaBlockSize)
            throws IllegalArgumentException;
    private native long arenaBlockSize(long handle);
    private native void setDisableAutoCompactions(
            long handle, boolean disableAutoCompactions);
    private native boolean disableAutoCompactions(long handle);
    private native void setCompactionStyle(long handle, byte compactionStyle);
    private native byte compactionStyle(long handle);
    private native void setMaxTableFilesSizeFIFO(
            long handle, long max_table_files_size);
    private native long maxTableFilesSizeFIFO(long handle);
    private native void setMaxSequentialSkipInIterations(
            long handle, long maxSequentialSkipInIterations);
    private native long maxSequentialSkipInIterations(long handle);
    private native void setMemTableFactory(long handle, long factoryHandle);
    private native String memTableFactoryName(long handle);
    private native void setTableFactory(long handle, long factoryHandle);
    private native String tableFactoryName(long handle);
    private static native void setCfPaths(
            final long handle, final String[] paths, final long[] targetSizes);
    private static native long cfPathsLen(final long handle);
    private static native void cfPaths(
            final long handle, final String[] paths, final long[] targetSizes);
    private native void setInplaceUpdateSupport(
            long handle, boolean inplaceUpdateSupport);
    private native boolean inplaceUpdateSupport(long handle);
    private native void setInplaceUpdateNumLocks(
            long handle, long inplaceUpdateNumLocks)
            throws IllegalArgumentException;
    private native long inplaceUpdateNumLocks(long handle);
    private native void setMemtablePrefixBloomSizeRatio(
            long handle, double memtablePrefixBloomSizeRatio);
    private native double memtablePrefixBloomSizeRatio(long handle);
    private native void setExperimentalMempurgeThreshold(
            long handle, double experimentalMempurgeThreshold);
    private native double experimentalMempurgeThreshold(long handle);
    private native void setMemtableWholeKeyFiltering(long handle, boolean memtableWholeKeyFiltering);
    private native boolean memtableWholeKeyFiltering(long handle);
    private native void setBloomLocality(
            long handle, int bloomLocality);
    private native int bloomLocality(long handle);
    private native void setMaxSuccessiveMerges(
            long handle, long maxSuccessiveMerges)
            throws IllegalArgumentException;
    private native long maxSuccessiveMerges(long handle);
    private native void setOptimizeFiltersForHits(long handle,
                                                  boolean optimizeFiltersForHits);
    private native boolean optimizeFiltersForHits(long handle);
    private native void setMemtableHugePageSize(long handle,
                                                long memtableHugePageSize);
    private native long memtableHugePageSize(long handle);
    private native void setSoftPendingCompactionBytesLimit(long handle,
                                                           long softPendingCompactionBytesLimit);
    private native long softPendingCompactionBytesLimit(long handle);
    private native void setHardPendingCompactionBytesLimit(long handle,
                                                           long hardPendingCompactionBytesLimit);
    private native long hardPendingCompactionBytesLimit(long handle);
    private native void setLevel0FileNumCompactionTrigger(long handle,
                                                          int level0FileNumCompactionTrigger);
    private native int level0FileNumCompactionTrigger(long handle);
    private native void setLevel0SlowdownWritesTrigger(long handle,
                                                       int level0SlowdownWritesTrigger);
    private native int level0SlowdownWritesTrigger(long handle);
    private native void setLevel0StopWritesTrigger(long handle,
                                                   int level0StopWritesTrigger);
    private native int level0StopWritesTrigger(long handle);
    private native void setMaxBytesForLevelMultiplierAdditional(long handle,
                                                                int[] maxBytesForLevelMultiplierAdditional);
    private native int[] maxBytesForLevelMultiplierAdditional(long handle);
    private native void setParanoidFileChecks(long handle,
                                              boolean paranoidFileChecks);
    private native boolean paranoidFileChecks(long handle);
    private native void setMaxWriteBufferNumberToMaintain(final long handle,
                                                          final int maxWriteBufferNumberToMaintain);
    private native int maxWriteBufferNumberToMaintain(final long handle);
    private native void setCompactionPriority(final long handle,
                                              final byte compactionPriority);
    private native byte compactionPriority(final long handle);
    private native void setReportBgIoStats(final long handle,
                                           final boolean reportBgIoStats);
    private native boolean reportBgIoStats(final long handle);
    private native void setTtl(final long handle, final long ttl);
    private native long ttl(final long handle);
    private native void setPeriodicCompactionSeconds(
            final long handle, final long periodicCompactionSeconds);
    private native long periodicCompactionSeconds(final long handle);
    private native void setCompactionOptionsUniversal(final long handle,
                                                      final long compactionOptionsUniversalHandle);
    private native void setCompactionOptionsFIFO(final long handle,
                                                 final long compactionOptionsFIFOHandle);
    private native void setForceConsistencyChecks(final long handle,
                                                  final boolean forceConsistencyChecks);
    private native boolean forceConsistencyChecks(final long handle);
    private native void setSstPartitionerFactory(long nativeHandle_, long newFactoryHandle);
    private static native void setCompactionThreadLimiter(
            final long nativeHandle_, final long compactionThreadLimiterHandle);

    private native void setEnableBlobFiles(final long nativeHandle_, final boolean enableBlobFiles);
    private native boolean enableBlobFiles(final long nativeHandle_);
    private native void setMinBlobSize(final long nativeHandle_, final long minBlobSize);
    private native long minBlobSize(final long nativeHandle_);
    private native void setBlobFileSize(final long nativeHandle_, final long blobFileSize);
    private native long blobFileSize(final long nativeHandle_);
    private native void setBlobCompressionType(final long nativeHandle_, final byte compressionType);
    private native byte blobCompressionType(final long nativeHandle_);
    private native void setEnableBlobGarbageCollection(
            final long nativeHandle_, final boolean enableBlobGarbageCollection);
    private native boolean enableBlobGarbageCollection(final long nativeHandle_);
    private native void setBlobGarbageCollectionAgeCutoff(
            final long nativeHandle_, final double blobGarbageCollectionAgeCutoff);
    private native double blobGarbageCollectionAgeCutoff(final long nativeHandle_);
    private native void setBlobGarbageCollectionForceThreshold(
            final long nativeHandle_, final double blobGarbageCollectionForceThreshold);
    private native double blobGarbageCollectionForceThreshold(final long nativeHandle_);
    private native void setBlobCompactionReadaheadSize(
            final long nativeHandle_, final long blobCompactionReadaheadSize);
    private native long blobCompactionReadaheadSize(final long nativeHandle_);
    private native void setBlobFileStartingLevel(
            final long nativeHandle_, final int blobFileStartingLevel);
    private native int blobFileStartingLevel(final long nativeHandle_);
    private native void setPrepopulateBlobCache(
            final long nativeHandle_, final byte prepopulateBlobCache);
    private native byte prepopulateBlobCache(final long nativeHandle_);
    private native void setFlushThreshold(final long nativeHandle_, final int flushThreshold);
    private native int flushThreshold(final long nativeHandle_);

    // instance variables
    // NOTE: If you add new member variables, please update the copy constructor above!
    private MemTableConfig memTableConfig_;
    private TableFormatConfig tableFormatConfig_;
    private AbstractComparator comparator_;
    private AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter_;
    private AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
            compactionFilterFactory_;
    private CompactionOptionsUniversal compactionOptionsUniversal_;
    private CompactionOptionsFIFO compactionOptionsFIFO_;
    private CompressionOptions bottommostCompressionOptions_;
    private CompressionOptions compressionOptions_;
    private SstPartitionerFactory sstPartitionerFactory_;
    private ConcurrentTaskLimiter compactionThreadLimiter_;
}