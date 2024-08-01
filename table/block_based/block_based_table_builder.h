//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <array>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "block_based_table_reader.h"
#include "db/version_edit.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/compression.h"
#include "util/work_queue.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

class BlockBasedTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  BlockBasedTableBuilder(const BlockBasedTableOptions& table_options,
                         const TableBuilderOptions& table_builder_options,
                         WritableFileWriter* file);

  // No copying allowed
  BlockBasedTableBuilder(const BlockBasedTableBuilder&) = delete;
  BlockBasedTableBuilder& operator=(const BlockBasedTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~BlockBasedTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: Unless key has type kTypeRangeDeletion, key is after any
  //           previously added non-kTypeRangeDeletion key according to
  //           comparator.
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

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  uint64_t EstimatedFileSize() const override;

  // Get the size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t GetTailSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(
      const std::string& encoded_seqno_to_time_mapping,
      uint64_t oldest_ancestor_time) override;

  // Convert private to public enabling outer class can use it
  struct ParallelCompressionRep {
    static constexpr size_t kBlockTrailerSize =
        BlockBasedTable::kBlockTrailerSize;
    // Keys is a wrapper of vector of strings avoiding
    // releasing string memories during vector clear()
    // in order to save memory allocation overhead
    class Keys {
     public:
      Keys() : keys_(kKeysInitSize), size_(0) {}
      void PushBack(const Slice& key) {
        if (size_ == keys_.size()) {
          keys_.emplace_back(key.data(), key.size());
        } else {
          keys_[size_].assign(key.data(), key.size());
        }
        size_++;
      }
      void SwapAssign(std::vector<std::string>& keys) {
        size_ = keys.size();
        std::swap(keys_, keys);
      }
      void Clear() { size_ = 0; }
      size_t Size() { return size_; }
      std::string& Back() { return keys_[size_ - 1]; }
      std::string& operator[](size_t idx) {
        assert(idx < size_);
        return keys_[idx];
      }

     private:
      const size_t kKeysInitSize = 32;
      std::vector<std::string> keys_;
      size_t size_;
    };
    std::unique_ptr<Keys> curr_block_keys;

    class BlockRepSlot;

    // BlockRep instances are fetched from and recycled to
    // block_rep_pool during parallel compression.
    struct BlockRep {
      Slice contents;
      Slice compressed_contents;
      std::unique_ptr<std::string> data;
      std::unique_ptr<std::string> compressed_data;
      CompressionType compression_type;
      std::unique_ptr<std::string> first_key_in_next_block;
      std::unique_ptr<Keys> keys;
      std::unique_ptr<BlockRepSlot> slot;
      Status status;
    };
    // Use a vector of BlockRep as a buffer for a determined number
    // of BlockRep structures. All data referenced by pointers in
    // BlockRep will be freed when this vector is destructed.
    using BlockRepBuffer = std::vector<BlockRep>;
    BlockRepBuffer block_rep_buf;
    // Use a thread-safe queue for concurrent access from block
    // building thread and writer thread.
    using BlockRepPool = WorkQueue<BlockRep*>;
    BlockRepPool block_rep_pool;

    // Use BlockRepSlot to keep block order in write thread.
    // slot_ will pass references to BlockRep
    class BlockRepSlot {
     public:
      BlockRepSlot() : slot_(1) {}
      template <typename T>
      void Fill(T&& rep) {
        slot_.push(std::forward<T>(rep));
      };
      void Take(BlockRep*& rep) { slot_.pop(rep); }

     private:
      // slot_ will pass references to BlockRep in block_rep_buf,
      // and those references are always valid before the destruction of
      // block_rep_buf.
      WorkQueue<BlockRep*> slot_;
    };

    // Compression queue will pass references to BlockRep in block_rep_buf,
    // and those references are always valid before the destruction of
    // block_rep_buf.
    using CompressQueue = WorkQueue<BlockRep*>;
    CompressQueue compress_queue;
    std::vector<port::Thread> compress_thread_pool;

    // Write queue will pass references to BlockRep::slot in block_rep_buf,
    // and those references are always valid before the corresponding
    // BlockRep::slot is destructed, which is before the destruction of
    // block_rep_buf.
    using WriteQueue = WorkQueue<BlockRepSlot*>;
    WriteQueue write_queue;
    std::unique_ptr<port::Thread> write_thread;

    // Estimate output file size when parallel compression is enabled. This is
    // necessary because compression & flush are no longer synchronized,
    // and BlockBasedTableBuilder::FileSize() is no longer accurate.
    // memory_order_relaxed suffices because accurate statistics is not
    // required.
    class FileSizeEstimator {
     public:
      explicit FileSizeEstimator()
          : uncomp_bytes_compressed(0),
            uncomp_bytes_curr_block(0),
            uncomp_bytes_curr_block_set(false),
            uncomp_bytes_inflight(0),
            blocks_inflight(0),
            curr_compression_ratio(0),
            estimated_file_size(0) {}

      // Estimate file size when a block is about to be emitted to
      // compression thread
      void EmitBlock(uint64_t uncomp_block_size, uint64_t curr_file_size) {
        uint64_t new_uncomp_bytes_inflight =
            uncomp_bytes_inflight.fetch_add(uncomp_block_size,
                                            std::memory_order_relaxed) +
            uncomp_block_size;

        uint64_t new_blocks_inflight =
            blocks_inflight.fetch_add(1, std::memory_order_relaxed) + 1;

        estimated_file_size.store(
            curr_file_size +
                static_cast<uint64_t>(
                    static_cast<double>(new_uncomp_bytes_inflight) *
                    curr_compression_ratio.load(std::memory_order_relaxed)) +
                new_blocks_inflight * kBlockTrailerSize,
            std::memory_order_relaxed);
      }

      // Estimate file size when a block is already reaped from
      // compression thread
      void ReapBlock(uint64_t compressed_block_size, uint64_t curr_file_size) {
        assert(uncomp_bytes_curr_block_set);

        uint64_t new_uncomp_bytes_compressed =
            uncomp_bytes_compressed + uncomp_bytes_curr_block;
        assert(new_uncomp_bytes_compressed > 0);

        curr_compression_ratio.store(
            (curr_compression_ratio.load(std::memory_order_relaxed) *
                 uncomp_bytes_compressed +
             compressed_block_size) /
                static_cast<double>(new_uncomp_bytes_compressed),
            std::memory_order_relaxed);
        uncomp_bytes_compressed = new_uncomp_bytes_compressed;

        uint64_t new_uncomp_bytes_inflight =
            uncomp_bytes_inflight.fetch_sub(uncomp_bytes_curr_block,
                                            std::memory_order_relaxed) -
            uncomp_bytes_curr_block;

        uint64_t new_blocks_inflight =
            blocks_inflight.fetch_sub(1, std::memory_order_relaxed) - 1;

        estimated_file_size.store(
            curr_file_size +
                static_cast<uint64_t>(
                    static_cast<double>(new_uncomp_bytes_inflight) *
                    curr_compression_ratio.load(std::memory_order_relaxed)) +
                new_blocks_inflight * kBlockTrailerSize,
            std::memory_order_relaxed);

        uncomp_bytes_curr_block_set = false;
      }

      void SetEstimatedFileSize(uint64_t size) {
        estimated_file_size.store(size, std::memory_order_relaxed);
      }

      uint64_t GetEstimatedFileSize() {
        return estimated_file_size.load(std::memory_order_relaxed);
      }

      void SetCurrBlockUncompSize(uint64_t size) {
        uncomp_bytes_curr_block = size;
        uncomp_bytes_curr_block_set = true;
      }

     private:
      // Input bytes compressed so far.
      uint64_t uncomp_bytes_compressed;
      // Size of current block being appended.
      uint64_t uncomp_bytes_curr_block;
      // Whether uncomp_bytes_curr_block has been set for next
      // ReapBlock call.
      bool uncomp_bytes_curr_block_set;
      // Input bytes under compression and not appended yet.
      std::atomic<uint64_t> uncomp_bytes_inflight;
      // Number of blocks under compression and not appended yet.
      std::atomic<uint64_t> blocks_inflight;
      // Current compression ratio, maintained by
      // BGWorkWriteMaybeCompressedBlock.
      std::atomic<double> curr_compression_ratio;
      // Estimated SST file size.
      std::atomic<uint64_t> estimated_file_size;
    };
    FileSizeEstimator file_size_estimator;

    // Facilities used for waiting first block completion. Need to Wait for
    // the completion of first block compression and flush to get a non-zero
    // compression ratio.
    std::atomic<bool> first_block_processed;
    std::condition_variable first_block_cond;
    std::mutex first_block_mutex;

    explicit ParallelCompressionRep(uint32_t parallel_threads)
        : curr_block_keys(new Keys()),
          block_rep_buf(parallel_threads),
          block_rep_pool(parallel_threads),
          compress_queue(parallel_threads),
          write_queue(parallel_threads),
          first_block_processed(false) {
      for (uint32_t i = 0; i < parallel_threads; i++) {
        block_rep_buf[i].contents = Slice();
        block_rep_buf[i].compressed_contents = Slice();
        block_rep_buf[i].data.reset(new std::string());
        block_rep_buf[i].compressed_data.reset(new std::string());
        block_rep_buf[i].compression_type = CompressionType();
        block_rep_buf[i].first_key_in_next_block.reset(new std::string());
        block_rep_buf[i].keys.reset(new Keys());
        block_rep_buf[i].slot.reset(new BlockRepSlot());
        block_rep_buf[i].status = Status::OK();
        block_rep_pool.push(&block_rep_buf[i]);
      }
    }

    ~ParallelCompressionRep() { block_rep_pool.finish(); }

    // Make a block prepared to be emitted to compression thread
    // Used in non-buffered mode
    BlockRep* PrepareBlock(CompressionType compression_type,
                           const Slice* first_key_in_next_block,
                           BlockBuilder* data_block) {
      BlockRep* block_rep =
          PrepareBlockInternal(compression_type, first_key_in_next_block);
      assert(block_rep != nullptr);
      data_block->SwapAndReset(*(block_rep->data));
      block_rep->contents = *(block_rep->data);
      std::swap(block_rep->keys, curr_block_keys);
      curr_block_keys->Clear();
      return block_rep;
    }

    // Used in EnterUnbuffered
    BlockRep* PrepareBlock(CompressionType compression_type,
                           const Slice* first_key_in_next_block,
                           std::string* data_block,
                           std::vector<std::string>* keys) {
      BlockRep* block_rep =
          PrepareBlockInternal(compression_type, first_key_in_next_block);
      assert(block_rep != nullptr);
      std::swap(*(block_rep->data), *data_block);
      block_rep->contents = *(block_rep->data);
      block_rep->keys->SwapAssign(*keys);
      return block_rep;
    }

    // Emit a block to compression thread
    void EmitBlock(BlockRep* block_rep) {
      assert(block_rep != nullptr);
      assert(block_rep->status.ok());
      if (!write_queue.push(block_rep->slot.get())) {
        return;
      }
      if (!compress_queue.push(block_rep)) {
        return;
      }

      if (!first_block_processed.load(std::memory_order_relaxed)) {
        std::unique_lock<std::mutex> lock(first_block_mutex);
        first_block_cond.wait(lock, [this] {
          return first_block_processed.load(std::memory_order_relaxed);
        });
      }
    }

    // Reap a block from compression thread
    void ReapBlock(BlockRep* block_rep) {
      assert(block_rep != nullptr);
      block_rep->compressed_data->clear();
      block_rep_pool.push(block_rep);

      if (!first_block_processed.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lock(first_block_mutex);
        first_block_processed.store(true, std::memory_order_relaxed);
        first_block_cond.notify_one();
      }
    }

   private:
    BlockRep* PrepareBlockInternal(CompressionType compression_type,
                                   const Slice* first_key_in_next_block) {
      BlockRep* block_rep = nullptr;
      block_rep_pool.pop(block_rep);
      assert(block_rep != nullptr);

      assert(block_rep->data);

      block_rep->compression_type = compression_type;

      if (first_key_in_next_block == nullptr) {
        block_rep->first_key_in_next_block.reset(nullptr);
      } else {
        block_rep->first_key_in_next_block->assign(
            first_key_in_next_block->data(), first_key_in_next_block->size());
      }

      return block_rep;
    }
  };

 private:
  bool ok() const { return status().ok(); }

  // Transition state from buffered to unbuffered. See `Rep::State` API comment
  // for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  void EnterUnbuffered();

  // Call block's Finish() method and then
  // - in buffered mode, buffer the uncompressed block contents.
  // - in unbuffered mode, write the compressed block contents to file.
  void WriteBlock(BlockBuilder* block, BlockHandle* handle,
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

  void WriteFilterBlock(MetaIndexBuilder* meta_index_builder);
  void WriteIndexBlock(MetaIndexBuilder* meta_index_builder,
                       BlockHandle* index_block_handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  void WriteRangeDelBlock(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  struct Rep;
  class BlockBasedTablePropertiesCollectorFactory;
  class BlockBasedTablePropertiesCollector;
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

Slice CompressBlock(const Slice& uncompressed_data, const CompressionInfo& info,
                    CompressionType* type, uint32_t format_version,
                    bool do_sample, std::string* compressed_output,
                    std::string* sampled_output_fast,
                    std::string* sampled_output_slow);

}  // namespace ROCKSDB_NAMESPACE
