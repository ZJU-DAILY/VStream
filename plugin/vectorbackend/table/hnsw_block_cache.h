//
// Created by shb on 23-8-29.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "table/block_based/block_cache.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class Block_kHnsw : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole =
      CacheEntryRole::kVectorIndexBlock;
  static constexpr BlockType kBlockType = BlockType::kHnswIndex;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
