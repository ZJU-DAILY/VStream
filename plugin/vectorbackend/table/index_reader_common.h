//
// Created by shb on 23-8-31.
//

#pragma once

#include "hnsw_table_reader.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
// Encapsulates common functionality for the various index reader
// implementations. Provides access to the index block regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
class HnswTable::IndexReaderCommon : public HnswTable::IndexReader {
 public:
  IndexReaderCommon(const HnswTable* t, CachableEntry<Block>&& index_block)
      : table_(t), index_block_(std::move(index_block)) {
    assert(table_ != nullptr);
  }

 protected:
  static Status ReadIndexBlock(const HnswTable* table,
                               FilePrefetchBuffer* prefetch_buffer,
                               const ReadOptions& read_options, bool use_cache,
                               GetContext* get_context,
                               BlockCacheLookupContext* lookup_context,
                               CachableEntry<Block>* index_block);

  const HnswTable* table() const { return table_; }

  const InternalKeyComparator* internal_comparator() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);

    return &table_->get_rep()->internal_comparator;
  }

  bool cache_index_blocks() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);
    return table_->get_rep()->table_options.cache_index_and_filter_blocks;
  }

  Status GetOrReadIndexBlock(bool no_io, GetContext* get_context,
                             BlockCacheLookupContext* lookup_context,
                             CachableEntry<Block>* index_block,
                             const ReadOptions& read_options) const;

  size_t ApproximateIndexBlockMemoryUsage() const {
    assert(!index_block_.GetOwnValue() || index_block_.GetValue() != nullptr);
    return index_block_.GetOwnValue()
               ? index_block_.GetValue()->ApproximateMemoryUsage()
               : 0;
  }

 private:
  const HnswTable* table_;
  CachableEntry<Block> index_block_;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
