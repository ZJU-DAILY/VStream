// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.test.RemoveEmptyValueCompactionFilterFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class VectorColumnFamilyOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void copyConstructor() {
    final VectorColumnFamilyOptions origOpts = new VectorColumnFamilyOptions();
    origOpts.setNumLevels(1);
    origOpts.setMaxElements(100_0000);
    origOpts.setM(20);
    origOpts.setEfConstruction(20);
    origOpts.setRandomSeed(114514);
    origOpts.setVisitListPoolSize(2);
    origOpts.setTerminationThreshold(100.0f);
    origOpts.setTerminationWeight(0.2f);
    origOpts.setDim(256);
    origOpts.setSpace(SpaceType.L2);
    origOpts.setAllowReplaceDeleted(true);
    final VectorColumnFamilyOptions copyOpts = new VectorColumnFamilyOptions(origOpts);
    assertThat(origOpts.numLevels()).isEqualTo(copyOpts.numLevels());
    assertThat(origOpts.maxElements()).isEqualTo(copyOpts.maxElements());
    assertThat(origOpts.M()).isEqualTo(copyOpts.M());
    assertThat(origOpts.efConstruction()).isEqualTo(copyOpts.efConstruction());
    assertThat(origOpts.randomSeed()).isEqualTo(copyOpts.randomSeed());
    assertThat(origOpts.visitListPoolSize()).isEqualTo(copyOpts.visitListPoolSize());
    assertThat(origOpts.terminationThreshold()).isEqualTo(copyOpts.terminationThreshold());
    assertThat(origOpts.terminationWeight()).isEqualTo(copyOpts.terminationWeight());
    assertThat(origOpts.dim()).isEqualTo(copyOpts.dim());
    assertThat(origOpts.space()).isEqualTo(copyOpts.space());
    assertThat(origOpts.allowReplaceDeleted()).isEqualTo(copyOpts.allowReplaceDeleted());
  }

  @Test
  public void getVectorColumnFamilyOptionsFromProps() {
    final Properties properties = new Properties();
    properties.put("random_seed", "112");
    properties.put("M", "13");

    try (final VectorColumnFamilyOptions opt = VectorColumnFamilyOptions.
        getVectorColumnFamilyOptionsFromProps(properties)) {
      // setup sample properties
      assertThat(opt).isNotNull();
      assertThat(String.valueOf(opt.randomSeed())).
          isEqualTo(properties.get("random_seed"));
      assertThat(String.valueOf(opt.M())).
          isEqualTo(properties.get("M"));
    }
  }

  @Test
  public void getVectorColumnFamilyOptionsFromPropsWithIgnoreIllegalValue() {
    // setup sample properties
    final Properties properties = new Properties();
    properties.put("tomato", "1024");
    properties.put("burger", "2");
    properties.put("random_seed", "112");
    properties.put("M", "13");

    try (final ConfigOptions cfgOpts = new ConfigOptions().setIgnoreUnknownOptions(true);
         final VectorColumnFamilyOptions opt =
                 VectorColumnFamilyOptions.getVectorColumnFamilyOptionsFromProps(cfgOpts, properties)) {
      // setup sample properties
      assertThat(opt).isNotNull();
      assertThat(String.valueOf(opt.randomSeed()))
          .isEqualTo(properties.get("random_seed"));
      assertThat(String.valueOf(opt.M()))
          .isEqualTo(properties.get("M"));
    }
  }

  @Test
  public void failVectorColumnFamilyOptionsFromPropsWithIllegalValue() {
    // setup sample properties
    final Properties properties = new Properties();
    properties.put("tomato", "1024");
    properties.put("burger", "2");

    try (final VectorColumnFamilyOptions opt =
                 VectorColumnFamilyOptions.getVectorColumnFamilyOptionsFromProps(properties)) {
      assertThat(opt).isNull();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failVectorColumnFamilyOptionsFromPropsWithNullValue() {
    try (final VectorColumnFamilyOptions ignored =
             VectorColumnFamilyOptions.getVectorColumnFamilyOptionsFromProps(null)) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failVectorColumnFamilyOptionsFromPropsWithEmptyProps() {
    try (final VectorColumnFamilyOptions ignored =
             VectorColumnFamilyOptions.getVectorColumnFamilyOptionsFromProps(new Properties())) {
    }
  }

  @Test
  public void writeBufferSize() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setWriteBufferSize(longValue);
      assertThat(opt.writeBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBufferNumber() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxWriteBufferNumber(intValue);
      assertThat(opt.maxWriteBufferNumber()).isEqualTo(intValue);
    }
  }

  @Test
  public void minWriteBufferNumberToMerge() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setMinWriteBufferNumberToMerge(intValue);
      assertThat(opt.minWriteBufferNumberToMerge()).isEqualTo(intValue);
    }
  }

  @Test
  public void numLevels() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setNumLevels(intValue);
      assertThat(opt.numLevels()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroFileNumCompactionTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroFileNumCompactionTrigger(intValue);
      assertThat(opt.levelZeroFileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroSlowdownWritesTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroSlowdownWritesTrigger(intValue);
      assertThat(opt.levelZeroSlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroStopWritesTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroStopWritesTrigger(intValue);
      assertThat(opt.levelZeroStopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void targetFileSizeBase() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setTargetFileSizeBase(longValue);
      assertThat(opt.targetFileSizeBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void targetFileSizeMultiplier() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setTargetFileSizeMultiplier(intValue);
      assertThat(opt.targetFileSizeMultiplier()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxBytesForLevelBase() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxBytesForLevelBase(longValue);
      assertThat(opt.maxBytesForLevelBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void levelCompactionDynamicLevelBytes() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setLevelCompactionDynamicLevelBytes(boolValue);
      assertThat(opt.levelCompactionDynamicLevelBytes())
          .isEqualTo(boolValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplier() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setMaxBytesForLevelMultiplier(doubleValue);
      assertThat(opt.maxBytesForLevelMultiplier()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplierAdditional() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue1 = rand.nextInt();
      final int intValue2 = rand.nextInt();
      final int[] ints = new int[]{intValue1, intValue2};
      opt.setMaxBytesForLevelMultiplierAdditional(ints);
      assertThat(opt.maxBytesForLevelMultiplierAdditional()).isEqualTo(ints);
    }
  }

  @Test
  public void maxCompactionBytes() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxCompactionBytes(longValue);
      assertThat(opt.maxCompactionBytes()).isEqualTo(longValue);
    }
  }

  @Test
  public void softPendingCompactionBytesLimit() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setSoftPendingCompactionBytesLimit(longValue);
      assertThat(opt.softPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void hardPendingCompactionBytesLimit() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setHardPendingCompactionBytesLimit(longValue);
      assertThat(opt.hardPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void level0FileNumCompactionTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0FileNumCompactionTrigger(intValue);
      assertThat(opt.level0FileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0SlowdownWritesTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0SlowdownWritesTrigger(intValue);
      assertThat(opt.level0SlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0StopWritesTrigger() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0StopWritesTrigger(intValue);
      assertThat(opt.level0StopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void arenaBlockSize() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setArenaBlockSize(longValue);
      assertThat(opt.arenaBlockSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void disableAutoCompactions() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableAutoCompactions(boolValue);
      assertThat(opt.disableAutoCompactions()).isEqualTo(boolValue);
    }
  }

  @Test
  public void maxSequentialSkipInIterations() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxSequentialSkipInIterations(longValue);
      assertThat(opt.maxSequentialSkipInIterations()).isEqualTo(longValue);
    }
  }

  @Test
  public void inplaceUpdateSupport() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setInplaceUpdateSupport(boolValue);
      assertThat(opt.inplaceUpdateSupport()).isEqualTo(boolValue);
    }
  }

  @Test
  public void inplaceUpdateNumLocks() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setInplaceUpdateNumLocks(longValue);
      assertThat(opt.inplaceUpdateNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void memtablePrefixBloomSizeRatio() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setMemtablePrefixBloomSizeRatio(doubleValue);
      assertThat(opt.memtablePrefixBloomSizeRatio()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void experimentalMempurgeThreshold() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setExperimentalMempurgeThreshold(doubleValue);
      assertThat(opt.experimentalMempurgeThreshold()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void memtableWholeKeyFiltering() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean booleanValue = rand.nextBoolean();
      opt.setMemtableWholeKeyFiltering(booleanValue);
      assertThat(opt.memtableWholeKeyFiltering()).isEqualTo(booleanValue);
    }
  }

  @Test
  public void memtableHugePageSize() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMemtableHugePageSize(longValue);
      assertThat(opt.memtableHugePageSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void bloomLocality() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setBloomLocality(intValue);
      assertThat(opt.bloomLocality()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxSuccessiveMerges() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxSuccessiveMerges(longValue);
      assertThat(opt.maxSuccessiveMerges()).isEqualTo(longValue);
    }
  }

  @Test
  public void optimizeFiltersForHits() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean aBoolean = rand.nextBoolean();
      opt.setOptimizeFiltersForHits(aBoolean);
      assertThat(opt.optimizeFiltersForHits()).isEqualTo(aBoolean);
    }
  }

  @Test
  public void memTable() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      opt.setMemTableConfig(new HashLinkedListMemTableConfig());
      assertThat(opt.memTableFactoryName()).
          isEqualTo("HashLinkedListRepFactory");
    }
  }

  @Test
  public void comparator() throws RocksDBException {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      opt.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    }
  }

  @Test
  public void linkageOfPrepMethods() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.optimizeUniversalStyleCompaction();
      options.optimizeUniversalStyleCompaction(4000);
      options.optimizeLevelStyleCompaction();
      options.optimizeLevelStyleCompaction(3000);
      options.optimizeForPointLookup(10);
      options.optimizeForSmallDb();
    }
  }

  @Test
  public void shouldSetTestPrefixExtractor() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.useFixedLengthPrefixExtractor(100);
      options.useFixedLengthPrefixExtractor(10);
    }
  }

  @Test
  public void shouldSetTestCappedPrefixExtractor() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.useCappedPrefixExtractor(100);
      options.useCappedPrefixExtractor(10);
    }
  }

  @Test
  public void compressionTypes() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions()) {
      for (final CompressionType compressionType :
          CompressionType.values()) {
        VectorColumnFamilyOptions.setCompressionType(compressionType);
        assertThat(VectorColumnFamilyOptions.compressionType()).
            isEqualTo(compressionType);
        assertThat(CompressionType.valueOf("NO_COMPRESSION")).
            isEqualTo(CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void compressionPerLevel() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions()) {
      assertThat(VectorColumnFamilyOptions.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList = new ArrayList<>();
      for (int i = 0; i < VectorColumnFamilyOptions.numLevels(); i++) {
        compressionTypeList.add(CompressionType.NO_COMPRESSION);
      }
      VectorColumnFamilyOptions.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = VectorColumnFamilyOptions.compressionPerLevel();
      for (final CompressionType compressionType : compressionTypeList) {
        assertThat(compressionType).isEqualTo(
            CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void differentCompressionsPerLevel() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions()) {
      VectorColumnFamilyOptions.setNumLevels(3);

      assertThat(VectorColumnFamilyOptions.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList = new ArrayList<>();

      compressionTypeList.add(CompressionType.BZLIB2_COMPRESSION);
      compressionTypeList.add(CompressionType.SNAPPY_COMPRESSION);
      compressionTypeList.add(CompressionType.LZ4_COMPRESSION);

      VectorColumnFamilyOptions.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = VectorColumnFamilyOptions.compressionPerLevel();

      assertThat(compressionTypeList.size()).isEqualTo(3);
      assertThat(compressionTypeList).
          containsExactly(
              CompressionType.BZLIB2_COMPRESSION,
              CompressionType.SNAPPY_COMPRESSION,
              CompressionType.LZ4_COMPRESSION);

    }
  }

  @Test
  public void bottommostCompressionType() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions()) {
      assertThat(VectorColumnFamilyOptions.bottommostCompressionType())
          .isEqualTo(CompressionType.DISABLE_COMPRESSION_OPTION);

      for (final CompressionType compressionType : CompressionType.values()) {
        VectorColumnFamilyOptions.setBottommostCompressionType(compressionType);
        assertThat(VectorColumnFamilyOptions.bottommostCompressionType())
            .isEqualTo(compressionType);
      }
    }
  }

  @Test
  public void bottommostCompressionOptions() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions =
             new VectorColumnFamilyOptions();
         final CompressionOptions bottommostCompressionOptions =
             new CompressionOptions()
                 .setMaxDictBytes(123)) {

      VectorColumnFamilyOptions.setBottommostCompressionOptions(
          bottommostCompressionOptions);
      assertThat(VectorColumnFamilyOptions.bottommostCompressionOptions())
          .isEqualTo(bottommostCompressionOptions);
      assertThat(VectorColumnFamilyOptions.bottommostCompressionOptions()
          .maxDictBytes()).isEqualTo(123);
    }
  }

  @Test
  public void compressionOptions() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions();
        final CompressionOptions compressionOptions = new CompressionOptions()
          .setMaxDictBytes(123)) {

      VectorColumnFamilyOptions.setCompressionOptions(compressionOptions);
      assertThat(VectorColumnFamilyOptions.compressionOptions())
          .isEqualTo(compressionOptions);
      assertThat(VectorColumnFamilyOptions.compressionOptions().maxDictBytes())
          .isEqualTo(123);
    }
  }

  @Test
  public void compactionStyles() {
    try (final VectorColumnFamilyOptions VectorColumnFamilyOptions
             = new VectorColumnFamilyOptions()) {
      for (final CompactionStyle compactionStyle :
          CompactionStyle.values()) {
        VectorColumnFamilyOptions.setCompactionStyle(compactionStyle);
        assertThat(VectorColumnFamilyOptions.compactionStyle()).
            isEqualTo(compactionStyle);
        assertThat(CompactionStyle.valueOf("FIFO")).
            isEqualTo(CompactionStyle.FIFO);
      }
    }
  }

  @Test
  public void maxTableFilesSizeFIFO() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      long longValue = rand.nextLong();
      // Size has to be positive
      longValue = (longValue < 0) ? -longValue : longValue;
      longValue = (longValue == 0) ? longValue + 1 : longValue;
      opt.setMaxTableFilesSizeFIFO(longValue);
      assertThat(opt.maxTableFilesSizeFIFO()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBufferNumberToMaintain() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      int intValue = rand.nextInt();
      // Size has to be positive
      intValue = (intValue < 0) ? -intValue : intValue;
      intValue = (intValue == 0) ? intValue + 1 : intValue;
      opt.setMaxWriteBufferNumberToMaintain(intValue);
      assertThat(opt.maxWriteBufferNumberToMaintain()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void compactionPriorities() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      for (final CompactionPriority compactionPriority :
          CompactionPriority.values()) {
        opt.setCompactionPriority(compactionPriority);
        assertThat(opt.compactionPriority()).
            isEqualTo(compactionPriority);
      }
    }
  }

  @Test
  public void reportBgIoStats() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean booleanValue = true;
      opt.setReportBgIoStats(booleanValue);
      assertThat(opt.reportBgIoStats()).
          isEqualTo(booleanValue);
    }
  }

  @Test
  public void ttl() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setTtl(1000 * 60);
      assertThat(options.ttl()).
          isEqualTo(1000 * 60);
    }
  }

  @Test
  public void periodicCompactionSeconds() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setPeriodicCompactionSeconds(1000 * 60);
      assertThat(options.periodicCompactionSeconds()).isEqualTo(1000 * 60);
    }
  }

  @Test
  public void compactionOptionsUniversal() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions();
        final CompactionOptionsUniversal optUni = new CompactionOptionsUniversal()
          .setCompressionSizePercent(7)) {
      opt.setCompactionOptionsUniversal(optUni);
      assertThat(opt.compactionOptionsUniversal()).
          isEqualTo(optUni);
      assertThat(opt.compactionOptionsUniversal().compressionSizePercent())
          .isEqualTo(7);
    }
  }

  @Test
  public void compactionOptionsFIFO() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions();
         final CompactionOptionsFIFO optFifo = new CompactionOptionsFIFO()
             .setMaxTableFilesSize(2000)) {
      opt.setCompactionOptionsFIFO(optFifo);
      assertThat(opt.compactionOptionsFIFO()).
          isEqualTo(optFifo);
      assertThat(opt.compactionOptionsFIFO().maxTableFilesSize())
          .isEqualTo(2000);
    }
  }

  @Test
  public void forceConsistencyChecks() {
    try (final VectorColumnFamilyOptions opt = new VectorColumnFamilyOptions()) {
      final boolean booleanValue = true;
      opt.setForceConsistencyChecks(booleanValue);
      assertThat(opt.forceConsistencyChecks()).
          isEqualTo(booleanValue);
    }
  }

  @Test
  public void compactionFilter() {
    try(final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions();
        final RemoveEmptyValueCompactionFilter cf = new RemoveEmptyValueCompactionFilter()) {
      options.setCompactionFilter(cf);
      assertThat(options.compactionFilter()).isEqualTo(cf);
    }
  }

  @Test
  public void compactionFilterFactory() {
    try(final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions();
        final RemoveEmptyValueCompactionFilterFactory cff = new RemoveEmptyValueCompactionFilterFactory()) {
      options.setCompactionFilterFactory(cff);
      assertThat(options.compactionFilterFactory()).isEqualTo(cff);
    }
  }

  @Test
  public void compactionThreadLimiter() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions();
         final ConcurrentTaskLimiter compactionThreadLimiter =
             new ConcurrentTaskLimiterImpl("name", 3)) {
      options.setCompactionThreadLimiter(compactionThreadLimiter);
      assertThat(options.compactionThreadLimiter()).isEqualTo(compactionThreadLimiter);
    }
  }

  @Test
  public void oldDefaults() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.oldDefaults(4, 6);
      assertEquals(4 << 20, options.writeBufferSize());
      assertThat(options.compactionPriority()).isEqualTo(CompactionPriority.ByCompensatedSize);
      assertThat(options.targetFileSizeBase()).isEqualTo(2 * 1048576);
      assertThat(options.maxBytesForLevelBase()).isEqualTo(10 * 1048576);
      assertThat(options.softPendingCompactionBytesLimit()).isEqualTo(0);
      assertThat(options.hardPendingCompactionBytesLimit()).isEqualTo(0);
      assertThat(options.level0StopWritesTrigger()).isEqualTo(24);
    }
  }

  @Test
  public void optimizeForSmallDbWithCache() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions();
         final Cache cache = new LRUCache(1024)) {
      assertThat(options.optimizeForSmallDb(cache)).isEqualTo(options);
    }
  }

  @Test
  public void cfPaths() throws IOException {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      final List<DbPath> paths = Arrays.asList(
          new DbPath(Paths.get("test1"), 2 << 25), new DbPath(Paths.get("/test2/path"), 2 << 25));
      assertThat(options.cfPaths()).isEqualTo(Collections.emptyList());
      assertThat(options.setCfPaths(paths)).isEqualTo(options);
      assertThat(options.cfPaths()).isEqualTo(paths);
    }
  }

  @Test
  public void m() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setM(10);
      assertThat(options.M()).isEqualTo(10);
    }
  }

  @Test
  public void maxElements() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setMaxElements(10);
      assertThat(options.maxElements()).isEqualTo(10);
    }
  }

  @Test
  public void efConstruction() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setEfConstruction(10);
      assertThat(options.efConstruction()).isEqualTo(10);
    }
  }

  @Test
  public void randomSeed() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setRandomSeed(10);
      assertThat(options.randomSeed()).isEqualTo(10);
    }
  }

  @Test
  public void visitListPoolSize() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setVisitListPoolSize(10);
      assertThat(options.visitListPoolSize()).isEqualTo(10);
    }
  }

  @Test
  public void terminationThreshold() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setTerminationThreshold(11.0f);
      assertThat(options.terminationThreshold()).isEqualTo(11.0f);
    }
  }

  @Test
  public void terminationWeight() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setTerminationWeight(12.0f);
      assertThat(options.terminationWeight()).isEqualTo(12.0f);
    }
  }

  @Test
  public void terminationLowerBound() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setTerminationLowerBound(0.8f);
      assertThat(options.terminationWeight()).isEqualTo(0.8f);
    }
  }

  @Test
  public void dim() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setDim(10);
      assertThat(options.dim()).isEqualTo(10);
    }
  }

  @Test
  public void space() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setSpace(SpaceType.IP);
      assertThat(options.space()).isEqualTo(SpaceType.toByte(SpaceType.IP));
    }
  }

  @Test
  public void allowReplaceDeleted() {
    try (final VectorColumnFamilyOptions options = new VectorColumnFamilyOptions()) {
      options.setAllowReplaceDeleted(true);
      assertThat(options.allowReplaceDeleted()).isTrue();
    }
  }
}
