// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.Assert.*;

public class VectorColumnFamilyTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private final long dim = 2;

  private final VectorColumnFamilyOptions vcf_opts = new VectorColumnFamilyOptions()
          .setDim(dim)
          .setSpace(SpaceType.L2)
          .setM(10)
          .setDisableAutoCompactions(true)
          .setMemTableConfig(new HnswMemTableConfig(dim)
                                     .setSpace(SpaceType.L2)
                                     .setM(10))
          .setTableFormatConfig(new HnswTableOptions()
                                        .setDim(dim)
                                       .setSpace(SpaceType.L2)
                                        .setM(10));

  private final VectorSearchOptions vso = new VectorSearchOptions();

  private static byte[] floatArrayToByteArray(float[] src) {
    ByteBuffer bb = ByteBuffer.allocate(src.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.asFloatBuffer().put(src);
    return bb.array();
  }

  private static float bytesToFloat(byte[] bytes, int offset) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, offset, Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    return byteBuffer.getFloat();
  }

  private static byte[] longToBytes(long l) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    byteBuffer.putLong(l);
    return byteBuffer.array();
  }

  private static long bytesToLong(byte[] bytes, int offset) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, offset, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    return byteBuffer.getLong();
  }

  @Test
  public void VectorCFDescriptorName() {
    final byte[] vcfName = "some_name".getBytes(UTF_8);

    try(final VectorColumnFamilyOptions vcfOptions = new VectorColumnFamilyOptions()) {
      final VectorCFDescriptor vcfDescriptor = new VectorCFDescriptor(vcfName, vcfOptions);
      assertThat(vcfDescriptor.getName()).isEqualTo(vcfName);
    }
  }

  @Test
  public void VectorCFDescriptorOptions() {
    final byte[] vcfName = "some_name".getBytes(UTF_8);

    try (final VectorColumnFamilyOptions vcfOptions =
             new VectorColumnFamilyOptions().setCompressionType(CompressionType.BZLIB2_COMPRESSION)) {
      final VectorCFDescriptor vcfDescriptor =
          new VectorCFDescriptor(vcfName, vcfOptions);

      assertThat(vcfDescriptor.getOptions().compressionType())
          .isEqualTo(CompressionType.BZLIB2_COMPRESSION);
    }
  }

  @Test
  public void createVectorColumnFamily() throws RocksDBException {
    final byte[] vcfName = "new_vcf".getBytes(UTF_8);
    final VectorCFDescriptor vcfDescriptor =
        new VectorCFDescriptor(vcfName, new VectorColumnFamilyOptions());

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      try (final VectorColumnFamilyHandle VectorColumnFamilyHandle = db.createVectorColumnFamily(vcfDescriptor)) {
        assertThat(VectorColumnFamilyHandle.getName()).isEqualTo(vcfName);
        assertThat(VectorColumnFamilyHandle.getID()).isEqualTo(1);

        final VectorCFDescriptor latestDescriptor = VectorColumnFamilyHandle.getDescriptor();
        assertThat(latestDescriptor.getName()).isEqualTo(vcfName);

        final List<byte[]> columnFamilyNames =
            RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
        assertThat(columnFamilyNames).isNotNull();
        assertThat(columnFamilyNames.size()).isGreaterThan(0);
        assertThat(columnFamilyNames.size()).isEqualTo(3);
        assertThat(new String(columnFamilyNames.get(0))).isEqualTo("default");
        assertThat(new String(columnFamilyNames.get(1))).isEqualTo("new_vcf");
        assertThat(new String(columnFamilyNames.get(2))).isEqualTo("new_vcf" + RocksDB.vectorCFExtension);
      }
    }
  }

  @Test
  public void getWithOutValueAndVCf() throws RocksDBException {
    final byte[] vcfName = "new_vcf".getBytes(UTF_8);

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final VectorCFDescriptor vcfDescriptor =
              new VectorCFDescriptor(vcfName, vcf_opts);
      final VectorColumnFamilyHandle VectorColumnFamilyHandle = db.createVectorColumnFamily(vcfDescriptor);
      db.put(VectorColumnFamilyHandle, new WriteOptions(), longToBytes(1L), floatArrayToByteArray(new float[] {1.0f,
              1.0f}));
      db.put("key2".getBytes(), "12345678".getBytes());
      final byte[] outValue = new byte[100];
      // not found value
      int getResult = db.get("keyNotFound".getBytes(), outValue);
      assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.vectorSearch(VectorColumnFamilyHandle, floatArrayToByteArray(new float[] {0.0f, 0.0f}),
                                  outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(getResult).isEqualTo(Float.BYTES + Long.BYTES);
      long searchResult = bytesToLong(outValue, 0);
      assertThat(searchResult).isEqualTo(1L);
    }
  }

  @Test
  public void createWriteDropVectorColumnFamily() throws RocksDBException {
      try (final Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        final VectorColumnFamilyHandle tmpVectorColumnFamilyHandle;
        tmpVectorColumnFamilyHandle = db.createVectorColumnFamily(
                new VectorCFDescriptor("tmpVCF".getBytes(), vcf_opts));
        db.put(tmpVectorColumnFamilyHandle, longToBytes(1L), floatArrayToByteArray(new float[] {0.0f, 1.0f}));
        db.dropVectorColumnFamily(tmpVectorColumnFamilyHandle);
        assertThat(tmpVectorColumnFamilyHandle.isOwningHandle()).isTrue();
    }
  }

  @Test
  public void createWriteDropColumnFamilies() throws RocksDBException {
    final byte[] vcfName = "new_vcf".getBytes(UTF_8);

    try (final Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final VectorColumnFamilyHandle tmpVectorColumnFamilyHandle;
      final VectorColumnFamilyHandle tmpVectorColumnFamilyHandle2;
        tmpVectorColumnFamilyHandle = db.createVectorColumnFamily(
                new VectorCFDescriptor("tmpVCF".getBytes(), new VectorColumnFamilyOptions()));
        tmpVectorColumnFamilyHandle2 = db.createVectorColumnFamily(
                new VectorCFDescriptor("tmpVCF2".getBytes(), new VectorColumnFamilyOptions()));
        final VectorCFDescriptor vcfDescriptor =
                new VectorCFDescriptor(vcfName, vcf_opts);
        db.createVectorColumnFamily(vcfDescriptor);

        db.put(tmpVectorColumnFamilyHandle, longToBytes(1L), floatArrayToByteArray(new float[] {0.0f, 1.0f}));
        db.put(tmpVectorColumnFamilyHandle2, longToBytes(1L), floatArrayToByteArray(new float[] {0.0f, 1.0f}));
        db.dropVectorColumnFamilies(Arrays.asList(tmpVectorColumnFamilyHandle, tmpVectorColumnFamilyHandle2));
        assertThat(tmpVectorColumnFamilyHandle.isOwningHandle()).isTrue();
        assertThat(tmpVectorColumnFamilyHandle2.isOwningHandle()).isTrue();
    }
  }

  @Test
  public void properties() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
                                         dbFolder.getRoot().getAbsolutePath())) {
        final VectorCFDescriptor desc = new VectorCFDescriptor("new_vcf".getBytes(), vcf_opts);
        final VectorColumnFamilyHandle vcf = db.createVectorColumnFamily(desc);
        assertThat(db.getProperty("rocksdb.estimate-num-keys")).isNotNull();
        assertThat(db.getLongProperty("rocksdb.estimate-num-keys"))
                .isGreaterThanOrEqualTo(0);
        assertThat(db.getProperty("rocksdb.stats")).isNotNull();
        assertThat(db.getProperty("rocksdb.sstables")).isNotNull();
        assertThat(db.getProperty(vcf, "rocksdb.estimate-num-keys"))
                .isNotNull();
        assertThat(db.getProperty(vcf, "rocksdb.stats")).isNotNull();
        assertThat(db.getProperty(vcf, "rocksdb.sstables")).isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys")).isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys"))
                .isGreaterThanOrEqualTo(0);
    }
  }

  @Test(expected = RocksDBException.class)
  public void failPutDisposedVCF() throws RocksDBException {
      try (final Options options = new Options().setCreateIfMissing(true);
           final RocksDB db = RocksDB.open(options,
                                           dbFolder.getRoot().getAbsolutePath())) {
          final VectorCFDescriptor desc = new VectorCFDescriptor("new_vcf".getBytes(), vcf_opts);
          final VectorColumnFamilyHandle vcf = db.createVectorColumnFamily(desc);
          db.dropVectorColumnFamily(vcf);
          db.put(vcf, longToBytes(1L), floatArrayToByteArray(new float[] {0.0f, 1.0f}));
      }
  }

  @Test(expected = RocksDBException.class)
  public void failRemoveDisposedVCF() throws RocksDBException {
      try (final Options options = new Options().setCreateIfMissing(true);
           final RocksDB db = RocksDB.open(options,
                                           dbFolder.getRoot().getAbsolutePath())) {
          final VectorCFDescriptor desc = new VectorCFDescriptor("new_vcf".getBytes(), vcf_opts);
          final VectorColumnFamilyHandle vcf = db.createVectorColumnFamily(desc);
          db.dropVectorColumnFamily(vcf);
          db.delete(vcf, floatArrayToByteArray(new float[] {0.0f, 1.0f}));
      }
  }

  @Test(expected = RocksDBException.class)
  public void failGetDisposedVCF() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
            final VectorCFDescriptor desc = new VectorCFDescriptor("new_vcf".getBytes(), vcf_opts);
      final VectorColumnFamilyHandle vcf = db.createVectorColumnFamily(desc);
      db.dropVectorColumnFamily(vcf);
      db.vectorSearch(vcf, floatArrayToByteArray(new float[] {0.0f, 1.0f}));
    }
  }

  @Test
  public void testByteVectorCreateFolumnFamily() throws RocksDBException {

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      final byte[] b0 = new byte[]{(byte) 0x00};
      final byte[] b1 = new byte[]{(byte) 0x01};
      final byte[] b2 = new byte[]{(byte) 0x02};
      db.createVectorColumnFamily(new VectorCFDescriptor(b0));
      db.createVectorColumnFamily(new VectorCFDescriptor(b1));
      ByteBuffer b0_ext = ByteBuffer.allocate(b0.length + RocksDB.vectorCFExtension.getBytes().length);
      b0_ext.put(b0);
      b0_ext.put(RocksDB.vectorCFExtension.getBytes());
      ByteBuffer b1_ext = ByteBuffer.allocate(b1.length + RocksDB.vectorCFExtension.getBytes().length);
      b1_ext.put(b1);
      b1_ext.put(RocksDB.vectorCFExtension.getBytes());
      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), b0, b0_ext.array(), b1, b1_ext.array());
      db.createVectorColumnFamily(new VectorCFDescriptor(b2));
    }
  }

  @Test
  public void testvcfNamesWithZeroBytes() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final byte[] b0 = new byte[] {0, 0};
      final byte[] b1 = new byte[] {0, 1};
      db.createVectorColumnFamily(new VectorCFDescriptor(b0));
      db.createVectorColumnFamily(new VectorCFDescriptor(b1));
      ByteBuffer b0_ext = ByteBuffer.allocate(b0.length + RocksDB.vectorCFExtension.getBytes().length);
      b0_ext.put(b0);
      b0_ext.put(RocksDB.vectorCFExtension.getBytes());
      ByteBuffer b1_ext = ByteBuffer.allocate(b1.length + RocksDB.vectorCFExtension.getBytes().length);
      b1_ext.put(b1);
      b1_ext.put(RocksDB.vectorCFExtension.getBytes());
      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), b0, b0_ext.array()
              ,b1, b1_ext.array());
    }
  }

  @Test
  public void testvcfNameSimplifiedChinese() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final String simplifiedChinese = "\u7b80\u4f53\u5b57";
      db.createVectorColumnFamily(new VectorCFDescriptor(simplifiedChinese.getBytes()));

      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), simplifiedChinese.getBytes(),
                                    (simplifiedChinese + RocksDB.vectorCFExtension).getBytes());
    }
  }

  @Test
  public void testDestroyVectorColumnFamilyHandle() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final byte[] name1 = "vcf1".getBytes();
      final byte[] name2 = "vcf2".getBytes();
      final VectorCFDescriptor desc1 = new VectorCFDescriptor(name1);
      final VectorCFDescriptor desc2 = new VectorCFDescriptor(name2);
      final VectorColumnFamilyHandle cf1 = db.createVectorColumnFamily(desc1);
      final VectorColumnFamilyHandle cf2 = db.createVectorColumnFamily(desc2);
      assertTrue(cf1.isOwningHandle());
      assertTrue(cf2.isOwningHandle());
      assertFalse(cf1.isDefaultColumnFamily());
      db.destroyVectorColumnFamilyHandle(cf1);
      // At this point cf1 should not be used!
      assertFalse(cf1.isOwningHandle());
      assertTrue(cf2.isOwningHandle());
    }
  }

  @Test
  public void vectorSearchTest() throws RocksDBException {
    final byte[] vcfName = "new_vcf".getBytes(UTF_8);

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final VectorCFDescriptor vcfDescriptor =
              new VectorCFDescriptor(vcfName, vcf_opts);
      final VectorColumnFamilyHandle vectorColumnFamilyHandle = db.createVectorColumnFamily(vcfDescriptor);
      db.put(vectorColumnFamilyHandle, new WriteOptions(), longToBytes(1L), floatArrayToByteArray(new float[] {1.0f,
              1.0f}));
      db.put(vectorColumnFamilyHandle, new WriteOptions(), longToBytes(2L), floatArrayToByteArray(new float[] {0.0f,
              0.5f}));
      db.put(vectorColumnFamilyHandle, new WriteOptions(), longToBytes(3L), floatArrayToByteArray(new float[] {0.0f,
              0.1f}));
      int k = 3;
      vso.setK(3);
      byte[] outValue = db.vectorSearch(vectorColumnFamilyHandle, vso, floatArrayToByteArray(new float[] {0.0f, 0.0f}));
      assertNotNull(outValue);
      assertThat(outValue.length).isEqualTo(k * (Float.BYTES + Long.BYTES));
      long searchResult = bytesToLong(outValue, 0);
      float dist = bytesToFloat(outValue, Long.BYTES);
      assertThat(searchResult).isEqualTo(3L);
      assertThat(dist).isCloseTo(0.01f, offset(0.000001f));
    }
  }
}
