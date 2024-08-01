//
// Created by shb on 23-8-12.
//

#pragma once

#include "hnsw_block_builder.h"
#include "hnsw_table_factory.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  IndexBuilder(const HnswTableOptions& table_opt);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents;
  };

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  void AddIndexEntry(const uint32_t last_key_in_current_block,
                     const BlockHandle& block_handle);

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  Status Finish(IndexBlocks* index_blocks);

  // Get the size for index block. Must be called after ::Finish.
  size_t IndexSize() const;

 private:
  size_t index_size = 0;
  std::string index_block_contents_;  // Contents of the index block.
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE