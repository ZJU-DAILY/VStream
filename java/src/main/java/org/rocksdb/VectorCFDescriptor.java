// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;

/** Describes a vector column family with a name and respective Options. */
public class VectorCFDescriptor {

  /**
   * Creates a new Vector Column Family using a name and default options,
   *
   * @param columnFamilyName name of column family.
   * @since 3.10.0
   */
  public VectorCFDescriptor(final byte[] columnFamilyName) {
    this(columnFamilyName, new VectorColumnFamilyOptions(), new ColumnFamilyOptions());
  }

  public VectorCFDescriptor(
      final byte[] columnFamilyName, final VectorColumnFamilyOptions vectorCFOptions) {
    this(columnFamilyName, vectorCFOptions, new ColumnFamilyOptions());
  }

  /**
   * Creates a new Column Family using a name and custom options.
   *
   * @param columnFamilyName name of column family.
   * @param vectorCFOptions options to be used with column family.
   * @param vectorVersionCFOptions options to be used with version column family.
   * @since 3.10.0
   */
  public VectorCFDescriptor(
      final byte[] columnFamilyName,
      final VectorColumnFamilyOptions vectorCFOptions,
      final ColumnFamilyOptions vectorVersionCFOptions) {
    columnFamilyName_ = columnFamilyName;
    vectorCFOptions_ = vectorCFOptions;
    vectorVersionCFOptions_ = vectorVersionCFOptions;
  }

  /**
   * Retrieve name of column family.
   *
   * @return column family name.
   * @since 3.10.0
   */
  public byte[] getName() {
    return columnFamilyName_;
  }

  /**
   * Retrieve assigned options instance.
   *
   * @return Options instance assigned to this instance.
   */
  public VectorColumnFamilyOptions getOptions() {
    return vectorCFOptions_;
  }

  public ColumnFamilyOptions getVersionOptions() {
    return vectorVersionCFOptions_;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final VectorCFDescriptor that = (VectorCFDescriptor) o;
    return Arrays.equals(columnFamilyName_, that.columnFamilyName_)
        && vectorCFOptions_.nativeHandle_ == that.vectorCFOptions_.nativeHandle_;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(vectorCFOptions_.nativeHandle_);
    result = 31 * result + Arrays.hashCode(columnFamilyName_);
    return result;
  }

  private final byte[] columnFamilyName_;
  private final VectorColumnFamilyOptions vectorCFOptions_;
  private final ColumnFamilyOptions vectorVersionCFOptions_;
}
