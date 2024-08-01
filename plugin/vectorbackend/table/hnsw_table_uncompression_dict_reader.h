//
// Created by shb on 23-8-30.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "table/format.h"
#include "table/get_context.h"
#include "trace_replay/block_cache_tracer.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class HnswTable;

// Provides access to the uncompression dictionary regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
class UncompressionDictReader {
 public:
  static Status Create(
      const HnswTable* table, const ReadOptions& ro,
      FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
      bool pin, BlockCacheLookupContext* lookup_context,
      std::unique_ptr<UncompressionDictReader>* uncompression_dict_reader);

  Status GetOrReadUncompressionDictionary(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro, bool no_io,
      bool verify_checksums, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict) const;

  size_t ApproximateMemoryUsage() const;

 private:
  UncompressionDictReader(const HnswTable* t,
                          CachableEntry<UncompressionDict>&& uncompression_dict)
      : table_(t), uncompression_dict_(std::move(uncompression_dict)) {
    assert(table_);
  }

  bool cache_dictionary_blocks() const;

  static Status ReadUncompressionDictionary(
      const HnswTable* table, FilePrefetchBuffer* prefetch_buffer,
      const ReadOptions& read_options, bool use_cache, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict);

  const HnswTable* table_;
  CachableEntry<UncompressionDict> uncompression_dict_;
};

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
