//
// Created by shb on 23-8-12.
//

#include "index_builder.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
IndexBuilder::IndexBuilder(const HnswTableOptions& table_opt) {}

void IndexBuilder::AddIndexEntry(const uint32_t last_key_in_current_block,
                                 const BlockHandle& block_handle) {
  PutFixed32(&index_block_contents_, last_key_in_current_block);
  // Only write uint32_t instead of uint64_t to save space.
  PutFixed32(&index_block_contents_,
             static_cast<uint32_t>(block_handle.offset()));
  PutFixed32(&index_block_contents_,
             static_cast<uint32_t>(block_handle.size()));
}

size_t IndexBuilder::IndexSize() const { return index_size; }

Status IndexBuilder::Finish(IndexBuilder::IndexBlocks* index_blocks) {
  PutFixed32(&index_block_contents_, static_cast<uint32_t>(0));
  index_blocks->index_block_contents = index_block_contents_;
  index_size = index_block_contents_.size();
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE