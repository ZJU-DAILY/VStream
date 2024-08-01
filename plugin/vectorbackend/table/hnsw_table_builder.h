// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/9.
//
#pragma once

#include "hnsw_block_builder.h"
#include "hnsw_table_factory.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_type.h"
#include "table/meta_blocks.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

extern const uint64_t kHnswTableMagicNumber;

class HnswTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  HnswTableBuilder(const HnswTableOptions& table_options,
                   const TableBuilderOptions& table_builder_options,
                   WritableFileWriter* file);
  // No copying allowed
  HnswTableBuilder(const HnswTableBuilder&) = delete;
  HnswTableBuilder& operator=(const HnswTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~HnswTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  bool IsEmpty() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  uint64_t EstimatedFileSize() const override;

  uint64_t GetTailSize() const override;

  bool NeedCompact() const override;

  TableProperties GetTableProperties() const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(
      const std::string& encoded_seqno_to_time_mapping,
      uint64_t oldest_ancestor_time) override;

 private:
  bool ok() const { return status().ok(); }

  // Transition state from buffered to unbuffered. See `Rep::State` API comment
  // for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  void EnterUnbuffered();

  // Call block's Finish() method and then
  // - in buffered mode, buffer the uncompressed block contents.
  // - in unbuffered mode, write the compressed block contents to file.
  void WriteBlock(HnswBlockBuilder* block, BlockHandle* handle,
                  BlockType blocktype);

  // Compress and write block content to the file.
  void WriteBlock(const Slice& block_contents, BlockHandle* handle,
                  BlockType block_type);
  // Directly write data to the file.
  void WriteMaybeCompressedBlock(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data = nullptr);

  void SetupCacheKeyPrefix(const TableBuilderOptions& tbo);

  template <typename TBlocklike>
  Status InsertBlockInCache(const Slice& block_contents,
                            const BlockHandle* handle, BlockType block_type);

  Status InsertBlockInCacheHelper(const Slice& block_contents,
                                  const BlockHandle* handle,
                                  BlockType block_type);

  Status InsertBlockInCompressedCache(const Slice& block_contents,
                                      const CompressionType type,
                                      const BlockHandle* handle);

  void WriteIndexBlock(BlockHandle* index_block_handle);
  void WriteHnswBlock(BlockHandle* handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& hnsw_block_handle,
                   BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  struct Rep;
  class HnswTablePropertiesCollector;
  Rep* rep_;

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush();

  // Some compression libraries fail when the uncompressed size is bigger than
  // int. If uncompressed size is bigger than kCompressionSizeLimit, don't
  // compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();

  // Get blocks from mem-table walking thread, compress them and
  // pass them to the write thread. Used in parallel compression mode only
  void BGWorkCompression(const CompressionContext& compression_ctx,
                         UncompressionContext* verify_ctx);

  // Given uncompressed block content, try to compress it and return result and
  // compression type
  void CompressAndVerifyBlock(const Slice& uncompressed_block_data,
                              bool is_data_block,
                              const CompressionContext& compression_ctx,
                              UncompressionContext* verify_ctx,
                              std::string* compressed_output,
                              Slice* result_block_contents,
                              CompressionType* result_compression_type,
                              Status* out_status);

  // Get compressed blocks from BGWorkCompression and write them into SST
  void BGWorkWriteMaybeCompressedBlock();

  // Initialize parallel compression context and
  // start BGWorkCompression and BGWorkWriteMaybeCompressedBlock threads
  void StartParallelCompression();

  // Stop BGWorkCompression and BGWorkWriteMaybeCompressedBlock threads
  void StopParallelCompression();
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
