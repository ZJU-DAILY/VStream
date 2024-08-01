//
// Created by shb on 23-8-31.
//

#include "binary_search_index_reader.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
Status BinarySearchIndexReader::Create(
    const HnswTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  assert(table != nullptr);
  assert(table->get_rep());
  assert(!pin || prefetch);
  assert(index_reader != nullptr);

  CachableEntry<Block> index_block;
  if (prefetch || !use_cache) {
    const Status s =
        ReadIndexBlock(table, prefetch_buffer, ro, use_cache,
                       /*get_context=*/nullptr, lookup_context, &index_block);
    if (!s.ok()) {
      return s;
    }

    if (use_cache && !pin) {
      index_block.Reset();
    }
  }

  index_reader->reset(
      new BinarySearchIndexReader(table, std::move(index_block)));

  return Status::OK();
}

HnswIndexBlockIter* BinarySearchIndexReader::NewIterator(
    const ReadOptions& read_options, HnswIndexBlockIter* iter,
    GetContext* get_context, BlockCacheLookupContext* lookup_context) {
  const bool no_io = (read_options.read_tier == kBlockCacheTier);
  CachableEntry<Block> index_block;
  const Status s = GetOrReadIndexBlock(no_io, get_context, lookup_context,
                                       &index_block, read_options);
  if (!s.ok()) {
    if (iter != nullptr) {
      iter->Invalidate(s);
      return iter;
    }

    assert(false);
    return nullptr;
  }

  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  auto it = index_block.GetValue()->NewHnswIndexBlockIterator(iter);

  assert(it != nullptr);
  index_block.TransferTo(it);

  return it;
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
