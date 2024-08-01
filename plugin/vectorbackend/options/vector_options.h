// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/4.
//
#pragma once

#include "include/rocksdb/options.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "plugin/vectorbackend/vectorbackend/vectorbackend_namespace.h"

namespace hnswlib {
enum SpaceType : uint8_t;
}

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
struct ImmutableVectorCFOptions;
struct MutableVectorCFOptions;
struct VectorColumnFamilyOptions : public ColumnFamilyOptions {
  size_t dim = 128;
  hnswlib::SpaceType space;
  size_t max_elements = 100000;
  size_t M = 16;
  size_t ef_construction = 200;
  size_t random_seed = 100;
  size_t visit_list_pool_size = 1;
  bool allow_replace_deleted = true;
  float termination_threshold = 0.0f;
  float termination_weight = 0.0f;
  float termination_lower_bound = 0.3f;
  VectorColumnFamilyOptions() : ColumnFamilyOptions(Options()) {
    disable_auto_compactions = true;
  }
  // Create VectorColumnFamilyOptions from Options
  VectorColumnFamilyOptions(const Options& options)
      : ColumnFamilyOptions(options) {
    disable_auto_compactions = true;
  }
  VectorColumnFamilyOptions(const ColumnFamilyOptions&,
                            const ImmutableVectorCFOptions&,
                            const MutableVectorCFOptions&);
  VectorColumnFamilyOptions& operator=(const ColumnFamilyOptions& options) {
    *reinterpret_cast<ColumnFamilyOptions*>(this) = options;
    return *this;
  }

  void Dump(Logger* log) const;
};
struct VectorOptions : public DBOptions, public VectorColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  VectorOptions() = default;
  explicit VectorOptions(const Options& options)
      : DBOptions(options), VectorColumnFamilyOptions(options) {}

  VectorOptions(const DBOptions& db_options,
                const VectorColumnFamilyOptions& vector_column_family_options)
      : DBOptions(db_options),
        VectorColumnFamilyOptions(vector_column_family_options) {}

  VectorOptions& operator=(const Options& options) {
    *static_cast<DBOptions*>(this) = options;
    *static_cast<VectorColumnFamilyOptions*>(this) = options;
    return *this;
  }

  operator Options() {
    Options options;
    *static_cast<DBOptions*>(&options) = *static_cast<DBOptions*>(this);
    *static_cast<ColumnFamilyOptions*>(&options) =
        *static_cast<ColumnFamilyOptions*>(this);
    return options;
  }
};
struct VectorSearchOptions : public ReadOptions {
  ReadOptionsType GetType() const override { return kVectorSearch; }
  size_t k{10};
  uint64_t ts{0};
  bool is_evict = false;
  bool trigger_sort = false;
  float termination_factor = 1.0f;
  bool search_sst = true;
  VectorSearchOptions() = default;
  explicit VectorSearchOptions(const ReadOptions& options)
      : ReadOptions(options) {}

  VectorSearchOptions& operator=(const ReadOptions& options) {
    *static_cast<ReadOptions*>(this) = options;
    return *this;
  }
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE