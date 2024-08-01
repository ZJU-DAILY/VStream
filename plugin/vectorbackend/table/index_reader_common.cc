//
// Created by shb on 23-8-31.
//

#include "index_reader_common.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
Status HnswTable::IndexReaderCommon::ReadIndexBlock(
    const HnswTable* table, FilePrefetchBuffer* prefetch_buffer,
    const ReadOptions& read_options, bool use_cache, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<Block>* index_block) {
  PERF_TIMER_GUARD(read_index_block_nanos);

  assert(table != nullptr);
  assert(index_block != nullptr);
  assert(index_block->IsEmpty());

  const Rep* const rep = table->get_rep();
  assert(rep != nullptr);

  const Status s = table->RetrieveBlock(
      prefetch_buffer, read_options, rep->footer.index_handle(),
      UncompressionDict::GetEmptyDict(), &index_block->As<Block_kIndex>(),
      get_context, lookup_context, /* for_compaction */ false, use_cache,
      /* async_read */ false);

  return s;
}

Status HnswTable::IndexReaderCommon::GetOrReadIndexBlock(
    bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, CachableEntry<Block>* index_block,
    const ReadOptions& ro) const {
  assert(index_block != nullptr);

  if (!index_block_.IsEmpty()) {
    index_block->SetUnownedValue(index_block_.GetValue());
    return Status::OK();
  }

  ReadOptions read_options = ro;
  if (no_io) {
    read_options.read_tier = kBlockCacheTier;
  }

  return ReadIndexBlock(table_, /*prefetch_buffer=*/nullptr, read_options,
                        cache_index_blocks(), get_context, lookup_context,
                        index_block);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
