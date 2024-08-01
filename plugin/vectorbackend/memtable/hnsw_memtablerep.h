// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/7.
//
#pragma once

#include <utility>

#include "db/memtable.h"
#include "hnsw_memtable_factory.h"
#include "rocksdb/memtablerep.h"

namespace hnswlib {
template <typename dist_t>
class HierarchicalNSW;
typedef unsigned int tableint;
}  // namespace hnswlib

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
typedef unsigned long labeltype;
class HnswMemTableRep : public MemTableRep {
  std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> index_;
  const HnswOptions hnswOptions_;

 public:
  explicit HnswMemTableRep(Allocator* allocator,
                           const HnswOptions& hnswOptions);

  void Insert(KeyHandle handle) override { InsertImpl(handle, false); }
  void InsertConcurrently(KeyHandle handle) override {
    InsertImpl(handle, true);
  }
  void InsertImpl(KeyHandle handle, bool concurrent);
  void MarkReadOnly() override;
  size_t ApproximateMemoryUsage() override;
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;
  void Search(const LookupKey& key, std::string* value, Status* s,
              const ReadOptions& read_opts, bool immutable_memtable) override;
  bool Contains(const char* key) const override;
  MemTableRep::Iterator* GetIterator(Arena* arena) override;
  const char* VectorIndexName() const override { return "Hnsw"; }
  bool ShouldFlushNow() const override;
  KeyHandle Allocate(const size_t len, char** buf) override;
  std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> GetVectorIndex()
      const override {
    return index_;
  }

  // This iterator is not thread-safe.
  class Iterator : public MemTableRep::Iterator {
   private:
    std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> index_;
    uint32_t current;
    char* buf;

   public:
    explicit Iterator(std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> index);

    ~Iterator() override;

    bool Valid() const override;

    const char* key() const override;

    void Next() override;

    void Prev() override;

    void Seek(const Slice& internal_key, const char* memtable_key) override;

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override;

    void SeekToFirst() override;

    void SeekToLast() override;
  };

  ~HnswMemTableRep() override = default;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE