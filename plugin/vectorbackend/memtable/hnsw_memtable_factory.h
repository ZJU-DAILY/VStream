// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include "plugin/vectorbackend/vectorbackend/vectorbackend_namespace.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/object_registry.h"

namespace hnswlib {
enum SpaceType : uint8_t;
}

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
using dist_t = float;
struct HnswOptions {
  static const char* kName() { return "HnswOptions"; }
  HnswOptions();
  size_t dim = 128;
  hnswlib::SpaceType space;
  size_t max_elements = 100000;
  size_t M = 16;
  size_t ef_construction = 200;
  size_t random_seed = 100;
  size_t visit_list_pool_size = 1;
  bool allow_replace_deleted = true;
};

class HnswMemTableFactory : public MemTableRepFactory {
 public:
  explicit HnswMemTableFactory(const HnswOptions& hnswOptions = {});

  // Methods for Configurable/Customizable class overrides
  static const char* kClassName() { return "HnswMemTableFactory"; }
  static const char* kNickName() { return "hnsw_memtable"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  // Methods for MemTableRepFactory class overrides
  using MemTableRepFactory::CreateMemTableRep;

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&, Allocator*, const SliceTransform*,
                                         Logger* logger) override;

  bool IsInsertConcurrentlySupported() const override { return true; }

  bool CanHandleDuplicatedKey() const override { return true; }

  [[nodiscard]] const HnswOptions& HnswOptions() const { return hnswOptions_; }

 private:
  struct HnswOptions hnswOptions_;
};

// Register the factory so that it can be used to create HnswMemTableRep
extern MemTableRepFactory* NewHnswMemTableFactory(
    const HnswOptions& options = HnswOptions());
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
