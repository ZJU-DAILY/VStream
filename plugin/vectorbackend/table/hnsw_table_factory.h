// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/9.
//
#pragma once

// Some assumptions:
// - Key length and Value length are fixed.
// - Does not support Snapshot.
// - Does not support Merge operations.
// - Does not support prefix bloom filters.

#include "../memtable/hnsw_memtable_factory.h"
#include "../options/vector_options.h"
#include "../vectorbackend/vectorbackend_namespace.h"
#include "table/block_based/block_based_table_factory.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
using dist_t = float;
struct HnswTableOptions : public BlockBasedTableOptions, public HnswOptions {
  static const char* kName() { return "HnswTableOptions"; }
  explicit HnswTableOptions(HnswOptions hnsw_opts) : HnswOptions(hnsw_opts) {}
  HnswTableOptions() = default;
};

extern TableFactory* NewHnswTableFactory(const HnswTableOptions& options);

class HnswTableFactory : public BlockBasedTableFactory {
 public:
  explicit HnswTableFactory(HnswTableOptions table_option = {});
  ~HnswTableFactory() override = default;

  // Method to allow CheckedCast to work for this class
  static const char* kClassName() { return "HnswTable"; }

  const char* Name() const override { return "HnswTable"; }

  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

  // Valdates the specified DB Options.
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  std::string GetPrintableOptions() const override;

  TailPrefetchStats* tail_prefetch_stats() { return &tail_prefetch_stats_; }

  const HnswTableOptions& table_options() const { return table_options_; }

 private:
  HnswTableOptions table_options_;
  void InitializeOptions();
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
