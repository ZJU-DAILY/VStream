//
// Created by shb on 23-8-31.
//

#pragma once

#include "hnsw_table_reader.h"
#include "table/block_based/block_prefetcher.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class HnswBlockPrefetcher : public BlockPrefetcher {
 public:
  explicit HnswBlockPrefetcher(size_t compaction_readahead_size,
                               size_t initial_auto_readahead_size)
      : BlockPrefetcher(compaction_readahead_size,
                        initial_auto_readahead_size) {}

  void PrefetchIfNeeded(const HnswTable::Rep* rep, const BlockHandle& handle,
                        size_t readahead_size, bool is_for_compaction,
                        bool no_sequential_checking,
                        Env::IOPriority rate_limiter_priority);
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE