//
// Created by shb on 23-8-31.
//

#pragma once

#include "index_reader_common.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public HnswTable::IndexReaderCommon {
 public:
  // Read index from the file and create an instance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(const HnswTable* table, const ReadOptions& ro,
                       FilePrefetchBuffer* prefetch_buffer, bool use_cache,
                       bool prefetch, bool pin,
                       BlockCacheLookupContext* lookup_context,
                       std::unique_ptr<IndexReader>* index_reader);

  HnswIndexBlockIter* NewIterator(
      const ReadOptions& read_options, HnswIndexBlockIter* iter,
      GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override;

  size_t ApproximateMemoryUsage() const override {
    size_t usage = ApproximateIndexBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<BinarySearchIndexReader*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }

 private:
  BinarySearchIndexReader(const HnswTable* t,
                          CachableEntry<Block>&& index_block)
      : IndexReaderCommon(t, std::move(index_block)) {}
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
