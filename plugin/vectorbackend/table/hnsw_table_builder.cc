// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/9.
//

#include "hnsw_table_builder.h"

#include <memory>
#include <utility>

#include "hnsw_table_format.h"
#include "hnsw_table_reader.h"
#include "index_builder.h"
#include "plugin/vectorbackend/options/vcf_options.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswalg.h"
#include "rocksdb/merge_operator.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_cache.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
using ParallelCompressionRep = BlockBasedTableBuilder::ParallelCompressionRep;

const uint64_t kHnswTableMagicNumber = 0x88e241b785f4cff8ull;
constexpr size_t kBlockTrailerSize = BlockBasedTable::kBlockTrailerSize;

class HnswTableBuilder::HnswTablePropertiesCollector
    : public IntTblPropCollector {
 public:
  HnswTablePropertiesCollector() = default;

  Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                     uint64_t /*file_size*/) override {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    // Intentionally left blank. No interest in collecting stats for
    // blocks.
  }

  Status Finish(UserCollectedProperties* properties) override {
    return Status::OK();
  }

  const char* Name() const override { return "HnswTablePropertiesCollector"; }

  UserCollectedProperties GetReadableProperties() const override {
    // Intentionally left blank.
    return {};
  }
};

struct HnswTableBuilder::Rep {
  const ImmutableVectorOptions ioptions;
  const HnswTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  Arena arena;
  WritableFileWriter* file;
  std::atomic<uint64_t> offset;
  size_t alignment;
  HnswBlockBuilder data_block;
  // Buffers uncompressed data blocks to replay later. Needed when
  // compression dictionary is enabled, so we can finalize the dictionary before
  // compressing any data blocks.
  std::vector<std::string> data_block_buffers;

  std::unique_ptr<IndexBuilder> index_builder;

  hnswlib::tableint last_key;
  CompressionType compression_type;
  uint64_t sample_for_compression;
  std::atomic<uint64_t> compressible_input_data_bytes;
  std::atomic<uint64_t> uncompressible_input_data_bytes;
  std::atomic<uint64_t> sampled_input_data_bytes;
  std::atomic<uint64_t> sampled_output_slow_data_bytes;
  std::atomic<uint64_t> sampled_output_fast_data_bytes;
  CompressionOptions compression_opts;
  std::unique_ptr<CompressionDict> compression_dict;
  std::vector<std::unique_ptr<CompressionContext>> compression_ctxs;
  std::vector<std::unique_ptr<UncompressionContext>> verify_ctxs;
  std::unique_ptr<UncompressionDict> verify_dict;

  size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state;
  // `kBuffered` state is allowed only as long as the buffering of uncompressed
  // data blocks (see `data_block_buffers`) does not exceed `buffer_limit`.
  uint64_t buffer_limit;
  std::shared_ptr<CacheReservationManager>
      compression_dict_buffer_cache_res_mgr;
  OffsetableCacheKey base_cache_key;
  const TableFileCreationReason reason;

  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  std::vector<std::unique_ptr<IntTblPropCollector>> table_properties_collectors;

  std::unique_ptr<ParallelCompressionRep> pc_rep;
  BlockCreateContext create_context;

  // The size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t tail_size;

  std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> hnsw;

  uint64_t get_offset() { return offset.load(std::memory_order_relaxed); }
  void set_offset(uint64_t o) { offset.store(o, std::memory_order_relaxed); }

  bool IsParallelCompressionEnabled() const {
    return compression_opts.parallel_threads > 1;
  }

  Status GetStatus() {
    // We need to make modifications of status visible when status_ok is set
    // to false, and this is ensured by status_mutex, so no special memory
    // order for status_ok is required.
    if (status_ok.load(std::memory_order_relaxed)) {
      return Status::OK();
    } else {
      return CopyStatus();
    }
  }

  Status CopyStatus() {
    std::lock_guard<std::mutex> lock(status_mutex);
    return status;
  }

  IOStatus GetIOStatus() {
    // We need to make modifications of io_status visible when status_ok is set
    // to false, and this is ensured by io_status_mutex, so no special memory
    // order for io_status_ok is required.
    if (io_status_ok.load(std::memory_order_relaxed)) {
      return IOStatus::OK();
    } else {
      return CopyIOStatus();
    }
  }

  IOStatus CopyIOStatus() {
    std::lock_guard<std::mutex> lock(io_status_mutex);
    return io_status;
  }

  // Never erase an existing status that is not OK.
  void SetStatus(const Status& s) {
    if (!s.ok() && status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(status_mutex);
      status = s;
      status_ok.store(false, std::memory_order_relaxed);
    }
  }

  // Never erase an existing I/O status that is not OK.
  // Calling this will also SetStatus(ios)
  void SetIOStatus(const IOStatus& ios) {
    if (!ios.ok() && io_status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(io_status_mutex);
      io_status = ios;
      io_status_ok.store(false, std::memory_order_relaxed);
    }
    SetStatus(ios);
  }

  Rep(const HnswTableOptions& table_opt, const TableBuilderOptions& tbo,
      WritableFileWriter* f)
      : ioptions(tbo.ioptions),
        table_options(table_opt),
        internal_comparator(tbo.internal_comparator),
        arena(tbo.moptions.arena_block_size),
        file(f),
        offset(0),
        alignment(table_options.block_align
                      ? std::min(static_cast<size_t>(table_options.block_size),
                                 kDefaultPageSize)
                      : 0),
        data_block(table_options.block_restart_interval, table_opt.dim,
                   table_opt.M),
        compression_type(tbo.compression_type),
        sample_for_compression(tbo.moptions.sample_for_compression),
        compressible_input_data_bytes(0),
        uncompressible_input_data_bytes(0),
        sampled_input_data_bytes(0),
        sampled_output_slow_data_bytes(0),
        sampled_output_fast_data_bytes(0),
        compression_opts(tbo.compression_opts),
        compression_dict(),
        compression_ctxs(tbo.compression_opts.parallel_threads),
        verify_ctxs(tbo.compression_opts.parallel_threads),
        verify_dict(),
        state((tbo.compression_opts.max_dict_bytes > 0) ? State::kBuffered
                                                        : State::kUnbuffered),
        reason(tbo.reason),
        flush_block_policy(
            table_options.flush_block_policy_factory->NewFlushBlockPolicy(
                table_options, data_block)),
        create_context(&table_options, ioptions.stats,
                       compression_type == kZSTD ||
                           compression_type == kZSTDNotFinalCompression,
                       tbo.moptions.block_protection_bytes_per_key,
                       tbo.internal_comparator.user_comparator(), true, false),
        tail_size(0),
        status_ok(true),
        io_status_ok(true) {
    if (tbo.target_file_size == 0) {
      buffer_limit = compression_opts.max_dict_buffer_bytes;
    } else if (compression_opts.max_dict_buffer_bytes == 0) {
      buffer_limit = tbo.target_file_size;
    } else {
      buffer_limit = std::min(tbo.target_file_size,
                              compression_opts.max_dict_buffer_bytes);
    }

    const auto compress_dict_build_buffer_charged =
        table_options.cache_usage_options.options_overrides
            .at(CacheEntryRole::kCompressionDictionaryBuildingBuffer)
            .charged;
    if (table_options.block_cache &&
        (compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kEnabled ||
         compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kFallback)) {
      compression_dict_buffer_cache_res_mgr =
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kCompressionDictionaryBuildingBuffer>>(
              table_options.block_cache);
    } else {
      compression_dict_buffer_cache_res_mgr = nullptr;
    }

    for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
      compression_ctxs[i] =
          std::make_unique<CompressionContext>(compression_type);
    }
    index_builder = std::make_unique<IndexBuilder>(table_options);

    assert(tbo.int_tbl_prop_collector_factories);
    for (auto& factory : *tbo.int_tbl_prop_collector_factories) {
      assert(factory);

      table_properties_collectors.emplace_back(
          factory->CreateIntTblPropCollector(tbo.column_family_id,
                                             tbo.level_at_creation));
    }
    table_properties_collectors.emplace_back(
        new HnswTablePropertiesCollector());
    const Comparator* ucmp = tbo.internal_comparator.user_comparator();
    assert(ucmp);
    if (ucmp->timestamp_size() > 0) {
      table_properties_collectors.emplace_back(
          new TimestampTablePropertiesCollector(ucmp));
    }
    if (table_options.verify_compression) {
      for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
        verify_ctxs[i] =
            std::make_unique<UncompressionContext>(compression_type);
      }
    }

    // These are only needed for populating table properties
    props.column_family_id = tbo.column_family_id;
    props.column_family_name = tbo.column_family_name;
    props.oldest_key_time = tbo.oldest_key_time;
    props.file_creation_time = tbo.file_creation_time;
    props.orig_file_number = tbo.cur_file_num;
    props.db_id = tbo.db_id;
    props.db_session_id = tbo.db_session_id;
    props.db_host_id = ioptions.db_host_id;
    if (!ReifyDbHostIdProperty(ioptions.env, &props.db_host_id).ok()) {
      ROCKS_LOG_INFO(ioptions.logger, "db_host_id property will not be set");
    }

    if (tbo.hnsw == nullptr) {
      hnsw = std::make_shared<hnswlib::HierarchicalNSW<dist_t>>(
          table_opt.dim, table_opt.space, &arena,
          table_opt.max_elements, table_opt.M, table_opt.ef_construction,
          table_opt.random_seed, table_opt.visit_list_pool_size,
          table_opt.allow_replace_deleted);

    } else {
      hnsw = tbo.hnsw;
    }
    assert(hnsw);
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;

 private:
  // Synchronize status & io_status accesses across threads from main thread,
  // compression thread and write thread in parallel compression.
  std::mutex status_mutex;
  std::atomic<bool> status_ok;
  Status status;
  std::mutex io_status_mutex;
  std::atomic<bool> io_status_ok;
  IOStatus io_status;
};

HnswTableBuilder::HnswTableBuilder(const HnswTableOptions& table_options,
                                   const TableBuilderOptions& tbo,
                                   WritableFileWriter* file) {
  HnswTableOptions sanitized_table_options{table_options};
  if (sanitized_table_options.format_version == 0 &&
      sanitized_table_options.checksum != kCRC32c) {
    ROCKS_LOG_WARN(
        tbo.ioptions.logger,
        "Silently converting format_version to 1 because checksum is "
        "non-default");
    // silently convert format_version to 1 to keep consistent with current
    // behavior
    sanitized_table_options.format_version = 1;
  }

  rep_ = new Rep(sanitized_table_options, tbo, file);

  TEST_SYNC_POINT_CALLBACK(
      "HnswTableBuilder::HnswTableBuilder:PreSetupBaseCacheKey",
      const_cast<TableProperties*>(&rep_->props));

  HnswTable::SetupBaseCacheKey(&rep_->props, tbo.db_session_id,
                               tbo.cur_file_num, &rep_->base_cache_key);

  if (rep_->IsParallelCompressionEnabled()) {
    StartParallelCompression();
  }
}

HnswTableBuilder::~HnswTableBuilder() {
  // Catch errors where caller forgot to call Finish()
  assert(rep_->state == Rep::State::kClosed);
  delete rep_;
}

void HnswTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;

  auto should_flush = r->flush_block_policy->Update(key, value);
  if (should_flush) {
    assert(!r->data_block.empty());
    Flush();
    if (r->state == Rep::State::kBuffered) {
      bool exceeds_buffer_limit =
          (r->buffer_limit != 0 && r->data_begin_offset > r->buffer_limit);
      bool exceeds_global_block_cache_limit = false;

      // Increase cache charging for the last buffered data block
      // only if the block is not going to be unbuffered immediately
      // and there exists a cache reservation manager
      if (!exceeds_buffer_limit &&
          r->compression_dict_buffer_cache_res_mgr != nullptr) {
        Status s =
            r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
                r->data_begin_offset);
        exceeds_global_block_cache_limit = s.IsMemoryLimit();
      }

      if (exceeds_buffer_limit || exceeds_global_block_cache_limit) {
        EnterUnbuffered();
      }
    }

    // Add item to index block.
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.
    if (ok() && r->state == Rep::State::kUnbuffered) {
      if (r->IsParallelCompressionEnabled()) {
        r->pc_rep->curr_block_keys->Clear();
      } else {
        r->index_builder->AddIndexEntry(r->last_key, r->pending_handle);
      }
    }
  }

  if (r->state == Rep::State::kUnbuffered &&
      r->IsParallelCompressionEnabled()) {
    r->pc_rep->curr_block_keys->PushBack(key);
  }

  // No need to transfer the last key because it is already stored by the
  // compressor
  r->data_block.Add(key, value);
  r->last_key = *reinterpret_cast<const hnswlib::tableint*>(key.data());
  // TODO offset passed in is not accurate for parallel compression case
  NotifyCollectTableCollectorsOnAdd(key, value, r->get_offset(),
                                    r->table_properties_collectors,
                                    r->ioptions.logger);

  r->props.num_entries++;
  r->props.raw_key_size += key.size();
  r->props.raw_value_size += value.size();
}

void HnswTableBuilder::Flush() {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  if (r->IsParallelCompressionEnabled() &&
      r->state == Rep::State::kUnbuffered) {
    r->data_block.Finish();
    ParallelCompressionRep::BlockRep* block_rep =
        r->pc_rep->PrepareBlock(r->compression_type, nullptr, &(r->data_block));
    assert(block_rep != nullptr);
    r->pc_rep->file_size_estimator.EmitBlock(block_rep->data->size(),
                                             r->get_offset());
    r->pc_rep->EmitBlock(block_rep);
  } else {
    WriteBlock(&r->data_block, &r->pending_handle, BlockType::kData);
  }
}

void HnswTableBuilder::WriteBlock(HnswBlockBuilder* block, BlockHandle* handle,
                                  BlockType block_type) {
  block->Finish();
  std::string uncompressed_block_data;
  uncompressed_block_data.reserve(rep_->table_options.block_size);
  block->SwapAndReset(uncompressed_block_data);
  if (rep_->state == Rep::State::kBuffered) {
    assert(block_type == BlockType::kData);
    rep_->data_block_buffers.emplace_back(std::move(uncompressed_block_data));
    rep_->data_begin_offset += rep_->data_block_buffers.back().size();
    return;
  }
  WriteBlock(uncompressed_block_data, handle, block_type);
}

void HnswTableBuilder::WriteBlock(const Slice& uncompressed_block_data,
                                  BlockHandle* handle, BlockType block_type) {
  Rep* r = rep_;
  assert(r->state == Rep::State::kUnbuffered);
  Slice block_contents;
  CompressionType type;
  Status compress_status;
  bool is_data_block = block_type == BlockType::kData;
  CompressAndVerifyBlock(uncompressed_block_data, is_data_block,
                         *(r->compression_ctxs[0]), r->verify_ctxs[0].get(),
                         &(r->compressed_output), &(block_contents), &type,
                         &compress_status);
  r->SetStatus(compress_status);
  if (!ok()) {
    return;
  }

  WriteMaybeCompressedBlock(block_contents, type, handle, block_type,
                            &uncompressed_block_data);
  r->compressed_output.clear();
  if (is_data_block) {
    r->props.data_size = r->get_offset();
    ++r->props.num_data_blocks;
  }
}

void HnswTableBuilder::BGWorkCompression(
    const CompressionContext& compression_ctx,
    UncompressionContext* verify_ctx) {
  ParallelCompressionRep::BlockRep* block_rep = nullptr;
  while (rep_->pc_rep->compress_queue.pop(block_rep)) {
    assert(block_rep != nullptr);
    CompressAndVerifyBlock(block_rep->contents, true, /* is_data_block */
                           compression_ctx, verify_ctx,
                           block_rep->compressed_data.get(),
                           &block_rep->compressed_contents,
                           &(block_rep->compression_type), &block_rep->status);
    block_rep->slot->Fill(block_rep);
  }
}

void HnswTableBuilder::CompressAndVerifyBlock(
    const Slice& uncompressed_block_data, bool is_data_block,
    const CompressionContext& compression_ctx, UncompressionContext* verify_ctx,
    std::string* compressed_output, Slice* block_contents,
    CompressionType* type, Status* out_status) {
  Rep* r = rep_;
  bool is_status_ok = ok();
  if (!r->IsParallelCompressionEnabled()) {
    assert(is_status_ok);
  }

  if (is_status_ok && uncompressed_block_data.size() < kCompressionSizeLimit) {
    StopWatchNano timer(
        r->ioptions.clock,
        ShouldReportDetailedTime(r->ioptions.env, r->ioptions.stats));

    if (is_data_block) {
      r->compressible_input_data_bytes.fetch_add(uncompressed_block_data.size(),
                                                 std::memory_order_relaxed);
    }
    const CompressionDict* compression_dict;
    if (!is_data_block || r->compression_dict == nullptr) {
      compression_dict = &CompressionDict::GetEmptyDict();
    } else {
      compression_dict = r->compression_dict.get();
    }
    assert(compression_dict != nullptr);
    CompressionInfo compression_info(r->compression_opts, compression_ctx,
                                     *compression_dict, r->compression_type,
                                     r->sample_for_compression);

    std::string sampled_output_fast;
    std::string sampled_output_slow;
    *block_contents = CompressBlock(
        uncompressed_block_data, compression_info, type,
        r->table_options.format_version, is_data_block /* allow_sample */,
        compressed_output, &sampled_output_fast, &sampled_output_slow);

    if (!sampled_output_slow.empty() || !sampled_output_fast.empty()) {
      // Currently compression sampling is only enabled for data block.
      assert(is_data_block);
      r->sampled_input_data_bytes.fetch_add(uncompressed_block_data.size(),
                                            std::memory_order_relaxed);
      r->sampled_output_slow_data_bytes.fetch_add(sampled_output_slow.size(),
                                                  std::memory_order_relaxed);
      r->sampled_output_fast_data_bytes.fetch_add(sampled_output_fast.size(),
                                                  std::memory_order_relaxed);
    }
    // notify collectors on block add
    NotifyCollectTableCollectorsOnBlockAdd(
        r->table_properties_collectors, uncompressed_block_data.size(),
        sampled_output_fast.size(), sampled_output_slow.size());

    // Some of the compression algorithms are known to be unreliable. If
    // the verify_compression flag is set then try to de-compress the
    // compressed data and compare to the input.
    if (*type != kNoCompression && r->table_options.verify_compression) {
      // Retrieve the uncompressed contents into a new buffer
      const UncompressionDict* verify_dict;
      if (!is_data_block || r->verify_dict == nullptr) {
        verify_dict = &UncompressionDict::GetEmptyDict();
      } else {
        verify_dict = r->verify_dict.get();
      }
      assert(verify_dict != nullptr);
      BlockContents contents;
      UncompressionInfo uncompression_info(*verify_ctx, *verify_dict,
                                           r->compression_type);
      Status uncompress_status = UncompressBlockData(
          uncompression_info, block_contents->data(), block_contents->size(),
          &contents, r->table_options.format_version, r->ioptions);

      if (uncompress_status.ok()) {
        bool data_match = contents.data.compare(uncompressed_block_data) == 0;
        if (!data_match) {
          // The result of the compression was invalid. abort.
          const char* const msg =
              "Decompressed block did not match pre-compression block";
          ROCKS_LOG_ERROR(r->ioptions.logger, "%s", msg);
          *out_status = Status::Corruption(msg);
          *type = kNoCompression;
        }
      } else {
        // Decompression reported an error. abort.
        *out_status = Status::Corruption(std::string("Could not decompress: ") +
                                         uncompress_status.getState());
        *type = kNoCompression;
      }
    }
    if (timer.IsStarted()) {
      RecordTimeToHistogram(r->ioptions.stats, COMPRESSION_TIMES_NANOS,
                            timer.ElapsedNanos());
    }
  } else {
    // Status is not OK, or block is too big to be compressed.
    if (is_data_block) {
      r->uncompressible_input_data_bytes.fetch_add(
          uncompressed_block_data.size(), std::memory_order_relaxed);
    }
    *type = kNoCompression;
  }
  if (is_data_block) {
    r->uncompressible_input_data_bytes.fetch_add(kBlockTrailerSize,
                                                 std::memory_order_relaxed);
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (*type == kNoCompression) {
    *block_contents = uncompressed_block_data;
    bool compression_attempted = !compressed_output->empty();
    RecordTick(r->ioptions.stats, compression_attempted
                                      ? NUMBER_BLOCK_COMPRESSION_REJECTED
                                      : NUMBER_BLOCK_COMPRESSION_BYPASSED);
    RecordTick(r->ioptions.stats,
               compression_attempted ? BYTES_COMPRESSION_REJECTED
                                     : BYTES_COMPRESSION_BYPASSED,
               uncompressed_block_data.size());
  } else {
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_COMPRESSED);
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_FROM,
               uncompressed_block_data.size());
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_TO,
               compressed_output->size());
  }
}

void HnswTableBuilder::WriteMaybeCompressedBlock(
    const Slice& block_contents, CompressionType comp_type, BlockHandle* handle,
    BlockType block_type, const Slice* uncompressed_block_data) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compression_type: uint8
  //    checksum: uint32
  Rep* r = rep_;
  bool is_data_block = block_type == BlockType::kData;
  // Old, misleading name of this function: WriteRawBlock
  StopWatch sw(r->ioptions.clock, r->ioptions.stats, WRITE_RAW_BLOCK_MICROS);
  handle->set_offset(r->get_offset());
  handle->set_size(block_contents.size());
  assert(status().ok());
  assert(io_status().ok());
  if (uncompressed_block_data == nullptr) {
    uncompressed_block_data = &block_contents;
    assert(comp_type == kNoCompression);
  }

  {
    IOStatus io_s = r->file->Append(block_contents);
    if (!io_s.ok()) {
      r->SetIOStatus(io_s);
      return;
    }
  }

  std::array<char, kBlockTrailerSize> trailer;
  trailer[0] = comp_type;
  uint32_t checksum = ComputeBuiltinChecksumWithLastByte(
      r->table_options.checksum, block_contents.data(), block_contents.size(),
      /*last_byte*/ comp_type);

  EncodeFixed32(trailer.data() + 1, checksum);
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:TamperWithChecksum",
      trailer.data());
  {
    IOStatus io_s = r->file->Append(Slice(trailer.data(), trailer.size()));
    if (!io_s.ok()) {
      r->SetIOStatus(io_s);
      return;
    }
  }

  {
    bool warm_cache;
    switch (r->table_options.prepopulate_block_cache) {
      case BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly:
        warm_cache = (r->reason == TableFileCreationReason::kFlush);
        break;
      case BlockBasedTableOptions::PrepopulateBlockCache::kDisable:
        warm_cache = false;
        break;
      default:
        // missing case
        assert(false);
        warm_cache = false;
    }
    if (warm_cache) {
      Status s = InsertBlockInCacheHelper(*uncompressed_block_data, handle,
                                          block_type);
      if (!s.ok()) {
        r->SetStatus(s);
        return;
      }
    }
  }

  r->set_offset(r->get_offset() + block_contents.size() + kBlockTrailerSize);
  if (r->table_options.block_align && is_data_block) {
    size_t pad_bytes =
        (r->alignment -
         ((block_contents.size() + kBlockTrailerSize) & (r->alignment - 1))) &
        (r->alignment - 1);
    IOStatus io_s = r->file->Pad(pad_bytes);
    if (io_s.ok()) {
      r->set_offset(r->get_offset() + pad_bytes);
    } else {
      r->SetIOStatus(io_s);
      return;
    }
  }

  if (r->IsParallelCompressionEnabled()) {
    if (is_data_block) {
      r->pc_rep->file_size_estimator.ReapBlock(block_contents.size(),
                                               r->get_offset());
    } else {
      r->pc_rep->file_size_estimator.SetEstimatedFileSize(r->get_offset());
    }
  }
}

void HnswTableBuilder::BGWorkWriteMaybeCompressedBlock() {
  Rep* r = rep_;
  ParallelCompressionRep::BlockRepSlot* slot = nullptr;
  ParallelCompressionRep::BlockRep* block_rep = nullptr;
  while (r->pc_rep->write_queue.pop(slot)) {
    assert(slot != nullptr);
    slot->Take(block_rep);
    assert(block_rep != nullptr);
    if (!block_rep->status.ok()) {
      r->SetStatus(block_rep->status);
      // Reap block so that blocked Flush() can finish
      // if there is one, and Flush() will notice !ok() next time.
      block_rep->status = Status::OK();
      r->pc_rep->ReapBlock(block_rep);
      continue;
    }

    r->pc_rep->file_size_estimator.SetCurrBlockUncompSize(
        block_rep->data->size());
    WriteMaybeCompressedBlock(block_rep->compressed_contents,
                              block_rep->compression_type, &r->pending_handle,
                              BlockType::kData, &block_rep->contents);
    if (!ok()) {
      break;
    }

    r->props.data_size = r->get_offset();
    ++r->props.num_data_blocks;

    r->index_builder->AddIndexEntry(
        DecodeFixed32(block_rep->keys->Back().data()), r->pending_handle);

    r->pc_rep->ReapBlock(block_rep);
  }
}

void HnswTableBuilder::StartParallelCompression() {
  rep_->pc_rep = std::make_unique<ParallelCompressionRep>(
      rep_->compression_opts.parallel_threads);
  rep_->pc_rep->compress_thread_pool.reserve(
      rep_->compression_opts.parallel_threads);
  for (uint32_t i = 0; i < rep_->compression_opts.parallel_threads; i++) {
    rep_->pc_rep->compress_thread_pool.emplace_back([this, i] {
      BGWorkCompression(*(rep_->compression_ctxs[i]),
                        rep_->verify_ctxs[i].get());
    });
  }
  rep_->pc_rep->write_thread = std::make_unique<port::Thread>(
      [this] { BGWorkWriteMaybeCompressedBlock(); });
}

void HnswTableBuilder::StopParallelCompression() {
  rep_->pc_rep->compress_queue.finish();
  for (auto& thread : rep_->pc_rep->compress_thread_pool) {
    thread.join();
  }
  rep_->pc_rep->write_queue.finish();
  rep_->pc_rep->write_thread->join();
}

Status HnswTableBuilder::status() const { return rep_->GetStatus(); }

IOStatus HnswTableBuilder::io_status() const { return rep_->GetIOStatus(); }

Status HnswTableBuilder::InsertBlockInCacheHelper(const Slice& block_contents,
                                                  const BlockHandle* handle,
                                                  BlockType block_type) {
  Cache* block_cache = rep_->table_options.block_cache.get();
  Status s;
  auto helper =
      GetCacheItemHelper(static_cast<ROCKSDB_NAMESPACE::BlockType>(block_type),
                         rep_->ioptions.lowest_used_cache_tier);
  if (block_cache && helper && helper->create_cb) {
    CacheKey key = BlockBasedTable::GetCacheKey(rep_->base_cache_key, *handle);
    size_t charge;
    s = WarmInCache(block_cache, key.AsSlice(), block_contents,
                    &rep_->create_context, helper, Cache::Priority::LOW,
                    &charge);

    if (s.ok()) {
      HnswTable::UpdateCacheInsertionMetrics(
          block_type, nullptr /*get_context*/, charge, s.IsOkOverwritten(),
          rep_->ioptions.stats);
    } else {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_ADD_FAILURES);
    }
  }
  return s;
}

void HnswTableBuilder::WriteIndexBlock(BlockHandle* index_block_handle) {
  if (!ok()) {
    return;
  }
  IndexBuilder::IndexBlocks index_blocks;
  auto index_builder_status = rep_->index_builder->Finish(&index_blocks);
  if (index_builder_status.IsIncomplete()) {
    // We have more than one index partition then meta_blocks are not
    // supported for the index. Currently meta_blocks are used only by
    // HashIndexBuilder which is not multi-partition.
    // assert(index_blocks.meta_blocks.empty());
  } else if (ok() && !index_builder_status.ok()) {
    rep_->SetStatus(index_builder_status);
  }
  if (ok()) {
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(index_blocks.index_block_contents, index_block_handle,
                 BlockType::kIndex);
    } else {
      WriteMaybeCompressedBlock(index_blocks.index_block_contents,
                                kNoCompression, index_block_handle,
                                BlockType::kIndex);
    }
  }
  // If there are more index partitions, finish them and write them out
  if (index_builder_status.IsIncomplete()) {
    bool index_building_finished = false;
    while (ok() && !index_building_finished) {
      Status s = rep_->index_builder->Finish(&index_blocks);
      if (s.ok()) {
        index_building_finished = true;
      } else if (s.IsIncomplete()) {
        // More partitioned index after this one
        assert(!index_building_finished);
      } else {
        // Error
        rep_->SetStatus(s);
        return;
      }

      if (rep_->table_options.enable_index_compression) {
        WriteBlock(index_blocks.index_block_contents, index_block_handle,
                   BlockType::kIndex);
      } else {
        WriteMaybeCompressedBlock(index_blocks.index_block_contents,
                                  kNoCompression, index_block_handle,
                                  BlockType::kIndex);
      }
    }
  }
}

void HnswTableBuilder::WriteHnswBlock(BlockHandle* handle) {
  if (ok()) {
    std::string block_contents;
    rep_->hnsw->saveIndex(&block_contents);
    PutFixed32(&block_contents, static_cast<uint32_t>(0));
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(block_contents, handle, BlockType::kHnswIndex);
    } else {
      WriteMaybeCompressedBlock(block_contents, kNoCompression, handle,
                                BlockType::kHnswIndex);
    }
  }
}

void HnswTableBuilder::WritePropertiesBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle properties_block_handle;
  if (ok()) {
    PropertyBlockBuilder property_block_builder;
    rep_->props.filter_policy_name = "";
    rep_->props.index_size =
        rep_->index_builder->IndexSize() + kBlockTrailerSize;
    rep_->props.comparator_name = "nullptr";
    rep_->props.merge_operator_name = "nullptr";
    rep_->props.compression_name =
        CompressionTypeToString(rep_->compression_type);
    rep_->props.compression_options =
        CompressionOptionsToString(rep_->compression_opts);
    rep_->props.prefix_extractor_name = "nullptr";
    std::string property_collectors_names = "[";
    property_collectors_names += "]";
    rep_->props.property_collectors_names = property_collectors_names;

    rep_->props.index_key_is_user_key = true;

    if (rep_->sampled_input_data_bytes > 0) {
      rep_->props.slow_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(rep_->sampled_output_slow_data_bytes) /
              rep_->sampled_input_data_bytes *
              rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes + 0.5);
      rep_->props.fast_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(rep_->sampled_output_fast_data_bytes) /
              rep_->sampled_input_data_bytes *
              rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes + 0.5);
    } else if (rep_->sample_for_compression > 0) {
      // We tried to sample but none were found. Assume worst-case (compression
      // ratio 1.0) so data is complete and aggregatable.
      rep_->props.slow_compression_estimated_data_size =
          rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes;
      rep_->props.fast_compression_estimated_data_size =
          rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes;
    }

    // Add basic properties
    property_block_builder.AddTableProperty(rep_->props);

    // Add use collected properties
    NotifyCollectTableCollectorsOnFinish(rep_->table_properties_collectors,
                                         rep_->ioptions.logger,
                                         &property_block_builder);

    Slice block_data = property_block_builder.Finish();
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:BlockData", &block_data);
    WriteMaybeCompressedBlock(block_data, kNoCompression,
                              &properties_block_handle, BlockType::kProperties);
  }
  if (ok()) {
#ifndef NDEBUG
    {
      uint64_t props_block_offset = properties_block_handle.offset();
      uint64_t props_block_size = properties_block_handle.size();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
          &props_block_offset);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
          &props_block_size);
    }
#endif  // !NDEBUG

    const std::string* properties_block_meta = &kPropertiesBlockName;
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:Meta",
        &properties_block_meta);
    meta_index_builder->Add(*properties_block_meta, properties_block_handle);
  }
}

void HnswTableBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (rep_->compression_dict != nullptr &&
      rep_->compression_dict->GetRawDict().size()) {
    BlockHandle compression_dict_block_handle;
    if (ok()) {
      WriteMaybeCompressedBlock(rep_->compression_dict->GetRawDict(),
                                kNoCompression, &compression_dict_block_handle,
                                BlockType::kCompressionDictionary);
#ifndef NDEBUG
      Slice compression_dict = rep_->compression_dict->GetRawDict();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
          &compression_dict);
#endif  // NDEBUG
    }
    if (ok()) {
      meta_index_builder->Add(kCompressionDictBlockName,
                              compression_dict_block_handle);
    }
  }
}

void HnswTableBuilder::WriteFooter(BlockHandle& hnsw_block_handle,
                                   BlockHandle& metaindex_block_handle,
                                   BlockHandle& index_block_handle) {
  Rep* r = rep_;
  // this is guaranteed by BlockBasedTableBuilder's constructor
  assert(r->table_options.checksum == kCRC32c ||
         r->table_options.format_version != 0);
  assert(ok());

  FooterBuilder footer;
  footer.Build(kHnswTableMagicNumber, r->table_options.format_version,
               r->get_offset(), r->table_options.checksum, hnsw_block_handle,
               metaindex_block_handle, index_block_handle);
  IOStatus ios = r->file->Append(footer.GetSlice());
  if (ios.ok()) {
    r->set_offset(r->get_offset() + footer.GetSlice().size());
  } else {
    r->SetIOStatus(ios);
  }
}

void HnswTableBuilder::EnterUnbuffered() {
  Rep* r = rep_;
  assert(r->state == Rep::State::kBuffered);
  r->state = Rep::State::kUnbuffered;
  const size_t kSampleBytes = r->compression_opts.zstd_max_train_bytes > 0
                                  ? r->compression_opts.zstd_max_train_bytes
                                  : r->compression_opts.max_dict_bytes;
  const size_t kNumBlocksBuffered = r->data_block_buffers.size();
  if (kNumBlocksBuffered == 0) {
    // The below code is neither safe nor necessary for handling zero data
    // blocks.
    return;
  }

  // Abstract algebra teaches us that a finite cyclic group (such as the
  // additive group of integers modulo N) can be generated by a number that is
  // coprime with N. Since N is variable (number of buffered data blocks), we
  // must then pick a prime number in order to guarantee coprimeness with any N.
  //
  // One downside of this approach is the spread will be poor when
  // `kPrimeGeneratorRemainder` is close to zero or close to
  // `kNumBlocksBuffered`.
  //
  // Picked a random number between one and one trillion and then chose the
  // next prime number greater than or equal to it.
  const uint64_t kPrimeGenerator = 545055921143ull;
  // Can avoid repeated division by just adding the remainder repeatedly.
  const size_t kPrimeGeneratorRemainder = static_cast<size_t>(
      kPrimeGenerator % static_cast<uint64_t>(kNumBlocksBuffered));
  const size_t kInitSampleIdx = kNumBlocksBuffered / 2;

  std::string compression_dict_samples;
  std::vector<size_t> compression_dict_sample_lens;
  size_t buffer_idx = kInitSampleIdx;
  for (size_t i = 0;
       i < kNumBlocksBuffered && compression_dict_samples.size() < kSampleBytes;
       ++i) {
    size_t copy_len = std::min(kSampleBytes - compression_dict_samples.size(),
                               r->data_block_buffers[buffer_idx].size());
    compression_dict_samples.append(r->data_block_buffers[buffer_idx], 0,
                                    copy_len);
    compression_dict_sample_lens.emplace_back(copy_len);

    buffer_idx += kPrimeGeneratorRemainder;
    if (buffer_idx >= kNumBlocksBuffered) {
      buffer_idx -= kNumBlocksBuffered;
    }
  }

  // final data block flushed, now we can generate dictionary from the samples.
  // OK if compression_dict_samples is empty, we'll just get empty dictionary.
  std::string dict;
  if (r->compression_opts.zstd_max_train_bytes > 0) {
    if (r->compression_opts.use_zstd_dict_trainer) {
      dict = ZSTD_TrainDictionary(compression_dict_samples,
                                  compression_dict_sample_lens,
                                  r->compression_opts.max_dict_bytes);
    } else {
      dict = ZSTD_FinalizeDictionary(
          compression_dict_samples, compression_dict_sample_lens,
          r->compression_opts.max_dict_bytes, r->compression_opts.level);
    }
  } else {
    dict = std::move(compression_dict_samples);
  }
  r->compression_dict.reset(new CompressionDict(dict, r->compression_type,
                                                r->compression_opts.level));
  r->verify_dict.reset(new UncompressionDict(
      dict, r->compression_type == kZSTD ||
                r->compression_type == kZSTDNotFinalCompression));

  auto get_iterator_for_block = [&r](size_t i) {
    auto& data_block = r->data_block_buffers[i];
    assert(!data_block.empty());

    Block reader{BlockContents{data_block}};
    DataBlockIter* iter = reader.NewDataIterator(
        r->internal_comparator.user_comparator(), kDisableGlobalSequenceNumber);

    iter->SeekToFirst();
    assert(iter->Valid());
    return std::unique_ptr<DataBlockIter>(iter);
  };

  std::unique_ptr<DataBlockIter> iter = nullptr, next_block_iter = nullptr;

  for (size_t i = 0; ok() && i < r->data_block_buffers.size(); ++i) {
    if (iter == nullptr) {
      iter = get_iterator_for_block(i);
      assert(iter != nullptr);
    };

    if (i + 1 < r->data_block_buffers.size()) {
      next_block_iter = get_iterator_for_block(i + 1);
    }

    auto& data_block = r->data_block_buffers[i];
    if (r->IsParallelCompressionEnabled()) {
      std::vector<std::string> keys;
      for (; iter->Valid(); iter->Next()) {
        keys.emplace_back(iter->key().ToString());
      }

      ParallelCompressionRep::BlockRep* block_rep = r->pc_rep->PrepareBlock(
          r->compression_type, nullptr, &data_block, &keys);

      assert(block_rep != nullptr);
      r->pc_rep->file_size_estimator.EmitBlock(block_rep->data->size(),
                                               r->get_offset());
      r->pc_rep->EmitBlock(block_rep);
    } else {
      WriteBlock(Slice(data_block), &r->pending_handle, BlockType::kData);
      if (ok() && i + 1 < r->data_block_buffers.size()) {
        assert(next_block_iter != nullptr);

        iter->SeekToLast();
        std::string last_key = iter->key().ToString();
        r->index_builder->AddIndexEntry(DecodeFixed32(last_key.data()),
                                        r->pending_handle);
      }
    }
    std::swap(iter, next_block_iter);
  }
  r->data_block_buffers.clear();
  r->data_begin_offset = 0;
  // Release all reserved cache for data block buffers
  if (r->compression_dict_buffer_cache_res_mgr != nullptr) {
    Status s = r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
        r->data_begin_offset);
    s.PermitUncheckedError();
  }
}

Status HnswTableBuilder::Finish() {
  Rep* r = rep_;
  assert(r->state != Rep::State::kClosed);
  bool empty_data_block = r->data_block.empty();
  Flush();
  if (r->state == Rep::State::kBuffered) {
    EnterUnbuffered();
  }
  if (r->IsParallelCompressionEnabled()) {
    StopParallelCompression();
#ifndef NDEBUG
    for (const auto& br : r->pc_rep->block_rep_buf) {
      assert(br.status.ok());
    }
#endif  // !NDEBUG
  } else {
    // To make sure properties block is able to keep the accurate size of index
    // block, we will finish writing all index entries first.
    if (ok() && !empty_data_block) {
      r->index_builder->AddIndexEntry(r->last_key, r->pending_handle);
    }
  }

  r->props.tail_start_offset = r->offset;

  // Write meta blocks, metaindex block and footer in the following order.
  //    1. [hnsw block]
  //    2. [meta block: index]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: properties]
  //    5. [metaindex block]
  //    6. Footer
  BlockHandle hnsw_block_handle, metaindex_block_handle, index_block_handle;
  MetaIndexBuilder meta_index_builder;
  WriteHnswBlock(&hnsw_block_handle);
  WriteIndexBlock(&index_block_handle);
  WriteCompressionDictBlock(&meta_index_builder);
  WritePropertiesBlock(&meta_index_builder);
  if (ok()) {
    // flush the meta index block
    WriteMaybeCompressedBlock(meta_index_builder.Finish(), kNoCompression,
                              &metaindex_block_handle, BlockType::kMetaIndex);
  }
  if (ok()) {
    WriteFooter(hnsw_block_handle, metaindex_block_handle, index_block_handle);
  }
  r->state = Rep::State::kClosed;
  r->SetStatus(r->CopyIOStatus());
  Status ret_status = r->CopyStatus();
  assert(!ret_status.ok() || io_status().ok());
  r->tail_size = r->offset - r->props.tail_start_offset;
  return ret_status;
}

void HnswTableBuilder::Abandon() {
  assert(rep_->state != Rep::State::kClosed);
  if (rep_->IsParallelCompressionEnabled()) {
    StopParallelCompression();
  }
  rep_->state = Rep::State::kClosed;
  rep_->CopyStatus().PermitUncheckedError();
  rep_->CopyIOStatus().PermitUncheckedError();
}

uint64_t HnswTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

bool HnswTableBuilder::IsEmpty() const {
  return rep_->props.num_entries == 0 && rep_->props.num_range_deletions == 0;
}

uint64_t HnswTableBuilder::FileSize() const {
  // todo: Is it need to count the HnswIndex size?
  return rep_->offset;
}

uint64_t HnswTableBuilder::EstimatedFileSize() const {
  // todo: Is it need to count the HnswIndex size?
  if (rep_->IsParallelCompressionEnabled()) {
    // Use compression ratio so far and inflight uncompressed bytes to estimate
    // final SST size.
    return rep_->pc_rep->file_size_estimator.GetEstimatedFileSize();
  } else {
    return FileSize();
  }
}

uint64_t HnswTableBuilder::GetTailSize() const { return rep_->tail_size; }

bool HnswTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties HnswTableBuilder::GetTableProperties() const {
  TableProperties ret = rep_->props;
  for (const auto& collector : rep_->table_properties_collectors) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties).PermitUncheckedError();
  }
  return ret;
}

std::string HnswTableBuilder::GetFileChecksum() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* HnswTableBuilder::GetFileChecksumFuncName() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}
void HnswTableBuilder::SetSeqnoTimeTableProperties(
    const std::string& encoded_seqno_to_time_mapping,
    uint64_t oldest_ancestor_time) {
  rep_->props.seqno_to_time_mapping = encoded_seqno_to_time_mapping;
  rep_->props.creation_time = oldest_ancestor_time;
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE