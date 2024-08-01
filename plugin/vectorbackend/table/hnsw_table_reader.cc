//
// Created by shb on 23-8-29.
//

#include "hnsw_table_reader.h"

#include <filesystem>
#include <memory>

#include "binary_search_index_reader.h"
#include "hnsw_block_cache.h"
#include "hnsw_reader.h"
#include "hnsw_table_iterator.h"
#include "hnsw_table_reader_impl.h"
#include "hnsw_table_uncompression_dict_reader.h"
#include "logging/logging.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswlib.h"
#include "table/block_based/block_cache.h"
#include "table/block_based/reader_common.h"
#include "table/block_fetcher.h"
#include "table/meta_blocks.h"
#include "table/sst_file_writer_collectors.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
namespace {

CacheAllocationPtr CopyBufferToHeap(MemoryAllocator* allocator, Slice& buf) {
  CacheAllocationPtr heap_buf;
  heap_buf = AllocateBlock(buf.size(), allocator);
  memcpy(heap_buf.get(), buf.data(), buf.size());
  return heap_buf;
}
}  // namespace

// Explicitly instantiate templates for each "blocklike" type we use (and
// before implicit specialization).
// This makes it possible to keep the template definitions in the .cc file.
#define INSTANTIATE_RETRIEVE_BLOCK(T)                                         \
  template Status HnswTable::RetrieveBlock<T>(                                \
      FilePrefetchBuffer * prefetch_buffer, const ReadOptions& ro,            \
      const BlockHandle& handle, const UncompressionDict& uncompression_dict, \
      CachableEntry<T>* out_parsed_block, GetContext* get_context,            \
      BlockCacheLookupContext* lookup_context, bool for_compaction,           \
      bool use_cache, bool async_read) const;

INSTANTIATE_RETRIEVE_BLOCK(UncompressionDict);
INSTANTIATE_RETRIEVE_BLOCK(Block_kData);
INSTANTIATE_RETRIEVE_BLOCK(Block_kIndex);
INSTANTIATE_RETRIEVE_BLOCK(Block_kMetaIndex);
INSTANTIATE_RETRIEVE_BLOCK(Block_kHnsw);

extern const uint64_t kHnswTableMagicNumber;

HnswTable::~HnswTable() { delete rep_; }

namespace {
// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param uncompression_dict Data for presetting the compression library's
//    dictionary.
template <typename TBlocklike>
Status ReadAndParseBlockFromFile(
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ReadOptions& options, const BlockHandle& handle,
    std::unique_ptr<TBlocklike>* result, const ImmutableOptions& ioptions,
    BlockCreateContext& create_context, bool maybe_compressed,
    const UncompressionDict& uncompression_dict,
    const PersistentCacheOptions& cache_options,
    MemoryAllocator* memory_allocator, bool for_compaction, bool async_read) {
  assert(result);

  BlockContents contents;
  BlockFetcher block_fetcher(
      file, prefetch_buffer, footer, options, handle, &contents, ioptions,
      /*do_uncompress*/ maybe_compressed, maybe_compressed,
      TBlocklike::kBlockType, uncompression_dict, cache_options,
      memory_allocator, nullptr, for_compaction);
  Status s;
  // If prefetch_buffer is not allocated, it will fall back to synchronous
  // reading of block contents.
  if (async_read && prefetch_buffer != nullptr) {
    s = block_fetcher.ReadAsyncBlockContents();
    if (!s.ok()) {
      return s;
    }
  } else {
    s = block_fetcher.ReadBlockContents();
  }
  if (s.ok()) {
    create_context.Create(result, std::move(contents));
  }
  return s;
}

template <typename TBlocklike>
uint32_t GetBlockNumRestarts(const TBlocklike& block) {
  if constexpr (std::is_convertible_v<const TBlocklike&, const Block&>) {
    const Block& b = block;
    return b.NumRestarts();
  } else {
    return 0;
  }
}

}  // namespace

void HnswTable::UpdateCacheHitMetrics(BlockType block_type,
                                      GetContext* get_context,
                                      size_t usage) const {
  Statistics* const statistics = rep_->ioptions.stats;

  PERF_COUNTER_ADD(block_cache_hit_count, 1);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1,
                            static_cast<uint32_t>(rep_->level));

  if (get_context) {
    ++get_context->get_context_stats_.num_cache_hit;
    get_context->get_context_stats_.num_cache_bytes_read += usage;
  } else {
    RecordTick(statistics, BLOCK_CACHE_HIT);
    RecordTick(statistics, BLOCK_CACHE_BYTES_READ, usage);
  }

  switch (block_type) {
      //    case BlockType::kHnswIndex:
      //      PERF_COUNTER_ADD(block_cache_vector_index_hit_count, 1);
      //
      //      if (get_context) {
      //        ++get_context->get_context_stats_.num_cache_vector_index_hit;
      //      } else {
      //        RecordTick(statistics, BLOCK_CACHE_VECTOR_INDEX_HIT);
      //      }
      //      break;

    case BlockType::kCompressionDictionary:
      // TODO: introduce perf counter for compression dictionary hit count
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_HIT);
      }
      break;

    case BlockType::kIndex:
      PERF_COUNTER_ADD(block_cache_index_hit_count, 1);

      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_HIT);
      }
      break;

    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_HIT);
      }
      break;
  }
}

void HnswTable::UpdateCacheMissMetrics(BlockType block_type,
                                       GetContext* get_context) const {
  Statistics* const statistics = rep_->ioptions.stats;

  // TODO: introduce aggregate (not per-level) block cache miss count
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 1,
                            static_cast<uint32_t>(rep_->level));

  if (get_context) {
    ++get_context->get_context_stats_.num_cache_miss;
  } else {
    RecordTick(statistics, BLOCK_CACHE_MISS);
  }

  // TODO: introduce perf counters for misses per block type
  switch (block_type) {
      //    case BlockType::kHnswIndex:
      //      if (get_context) {
      //        ++get_context->get_context_stats_.num_cache_vector_index_miss;
      //      } else {
      //        RecordTick(statistics, BLOCK_CACHE_VECTOR_INDEX_MISS);
      //      }
      //      break;

    case BlockType::kCompressionDictionary:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_MISS);
      }
      break;

    case BlockType::kIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_MISS);
      }
      break;

    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_MISS);
      }
      break;
  }
}

void HnswTable::UpdateCacheInsertionMetrics(BlockType block_type,
                                            GetContext* get_context,
                                            size_t usage, bool redundant,
                                            Statistics* const statistics) {
  // TODO: introduce perf counters for block cache insertions
  if (get_context) {
    ++get_context->get_context_stats_.num_cache_add;
    if (redundant) {
      ++get_context->get_context_stats_.num_cache_add_redundant;
    }
    get_context->get_context_stats_.num_cache_bytes_write += usage;
  } else {
    RecordTick(statistics, BLOCK_CACHE_ADD);
    if (redundant) {
      RecordTick(statistics, BLOCK_CACHE_ADD_REDUNDANT);
    }
    RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usage);
  }

  switch (block_type) {
      //    case BlockType::kHnswIndex:
      //      if (get_context) {
      //        ++get_context->get_context_stats_.num_cache_vector_index_add;
      //        if (redundant) {
      //          ++get_context->get_context_stats_
      //                .num_cache_vector_index_add_redundant;
      //        }
      //        get_context->get_context_stats_.num_cache_vector_index_bytes_insert
      //        +=
      //            usage;
      //      } else {
      //        RecordTick(statistics, BLOCK_CACHE_VECTOR_INDEX_ADD);
      //        if (redundant) {
      //          RecordTick(statistics,
      //          BLOCK_CACHE_VECTOR_INDEX_ADD_REDUNDANT);
      //        }
      //        RecordTick(statistics, BLOCK_CACHE_VECTOR_INDEX_BYTES_INSERT,
      //        usage);
      //      }
      //      break;

    case BlockType::kCompressionDictionary:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_add;
        if (redundant) {
          ++get_context->get_context_stats_
                .num_cache_compression_dict_add_redundant;
        }
        get_context->get_context_stats_
            .num_cache_compression_dict_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
                   usage);
      }
      break;

    case BlockType::kIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_add;
        if (redundant) {
          ++get_context->get_context_stats_.num_cache_index_add_redundant;
        }
        get_context->get_context_stats_.num_cache_index_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_INDEX_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, usage);
      }
      break;

    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_add;
        if (redundant) {
          ++get_context->get_context_stats_.num_cache_data_add_redundant;
        }
        get_context->get_context_stats_.num_cache_data_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_DATA_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT, usage);
      }
      break;
  }
}

namespace {
// Return True if table_properties has `user_prop_name` has a `true` value
// or it doesn't contain this property (for backward compatible).
bool IsFeatureSupported(const TableProperties& table_properties,
                        const std::string& user_prop_name, Logger* info_log) {
  auto& props = table_properties.user_collected_properties;
  auto pos = props.find(user_prop_name);
  // Older version doesn't have this value set. Skip this check.
  if (pos != props.end()) {
    if (pos->second == kPropFalse) {
      return false;
    } else if (pos->second != kPropTrue) {
      ROCKS_LOG_WARN(info_log, "Property %s has invalidate value %s",
                     user_prop_name.c_str(), pos->second.c_str());
    }
  }
  return true;
}

// Caller has to ensure seqno is not nullptr.
Status GetGlobalSequenceNumber(const TableProperties& table_properties,
                               SequenceNumber largest_seqno,
                               SequenceNumber* seqno) {
  const auto& props = table_properties.user_collected_properties;
  const auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
  const auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

  *seqno = kDisableGlobalSequenceNumber;
  if (version_pos == props.end()) {
    if (seqno_pos != props.end()) {
      std::array<char, 200> msg_buf;
      // This is not an external sst file, global_seqno is not supported.
      snprintf(
          msg_buf.data(), msg_buf.max_size(),
          "A non-external sst file have global seqno property with value %s",
          seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  uint32_t version = DecodeFixed32(version_pos->second.c_str());
  if (version < 2) {
    if (seqno_pos != props.end() || version != 1) {
      std::array<char, 200> msg_buf;
      // This is a v1 external sst file, global_seqno is not supported.
      snprintf(msg_buf.data(), msg_buf.max_size(),
               "An external sst file with version %u have global seqno "
               "property with value %s",
               version, seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  // Since we have a plan to deprecate global_seqno, we do not return failure
  // if seqno_pos == props.end(). We rely on version_pos to detect whether the
  // SST is external.
  SequenceNumber global_seqno(0);
  if (seqno_pos != props.end()) {
    global_seqno = DecodeFixed64(seqno_pos->second.c_str());
  }
  // SstTableReader open table reader with kMaxSequenceNumber as largest_seqno
  // to denote it is unknown.
  if (largest_seqno < kMaxSequenceNumber) {
    if (global_seqno == 0) {
      global_seqno = largest_seqno;
    }
    if (global_seqno != largest_seqno) {
      std::array<char, 200> msg_buf;
      snprintf(
          msg_buf.data(), msg_buf.max_size(),
          "An external sst file with version %u have global seqno property "
          "with value %s, while largest seqno in the file is %llu",
          version, seqno_pos->second.c_str(),
          static_cast<unsigned long long>(largest_seqno));
      return Status::Corruption(msg_buf.data());
    }
  }
  *seqno = global_seqno;

  if (global_seqno > kMaxSequenceNumber) {
    std::array<char, 200> msg_buf;
    snprintf(msg_buf.data(), msg_buf.max_size(),
             "An external sst file with version %u have global seqno property "
             "with value %llu, which is greater than kMaxSequenceNumber",
             version, static_cast<unsigned long long>(global_seqno));
    return Status::Corruption(msg_buf.data());
  }

  return Status::OK();
}
}  // namespace

void HnswTable::SetupBaseCacheKey(const TableProperties* properties,
                                  const std::string& cur_db_session_id,
                                  uint64_t cur_file_number,
                                  OffsetableCacheKey* out_base_cache_key,
                                  bool* out_is_stable) {
  // Use a stable cache key if sufficient data is in table properties
  std::string db_session_id;
  uint64_t file_num;
  std::string db_id;
  if (properties && !properties->db_session_id.empty() &&
      properties->orig_file_number > 0) {
    // (Newer SST file case)
    // We must have both properties to get a stable unique id because
    // CreateColumnFamilyWithImport or IngestExternalFiles can change the
    // file numbers on a file.
    db_session_id = properties->db_session_id;
    file_num = properties->orig_file_number;
    // Less critical, populated in earlier release than above
    db_id = properties->db_id;
    if (out_is_stable) {
      *out_is_stable = true;
    }
  } else {
    // (Old SST file case)
    // We use (unique) cache keys based on current identifiers. These are at
    // least stable across table file close and re-open, but not across
    // different DBs nor DB close and re-open.
    db_session_id = cur_db_session_id;
    file_num = cur_file_number;
    // Plumbing through the DB ID to here would be annoying, and of limited
    // value because of the case of VersionSet::Recover opening some table
    // files and later setting the DB ID. So we just rely on uniqueness
    // level provided by session ID.
    db_id = "unknown";
    if (out_is_stable) {
      *out_is_stable = false;
    }
  }

  // Too many tests to update to get these working
  // assert(file_num > 0);
  // assert(!db_session_id.empty());
  // assert(!db_id.empty());

  // Minimum block size is 5 bytes; therefore we can trim off two lower bits
  // from offsets. See GetCacheKey.
  *out_base_cache_key = OffsetableCacheKey(db_id, db_session_id, file_num);
}

CacheKey HnswTable::GetCacheKey(const OffsetableCacheKey& base_cache_key,
                                const BlockHandle& handle) {
  // Minimum block size is 5 bytes; therefore we can trim off two lower bits
  // from offet.
  return base_cache_key.WithOffset(handle.offset() >> 2);
}

Status HnswTable::Open(
    const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const EnvOptions& env_options, const HnswTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    uint8_t block_protection_bytes_per_key,
    std::unique_ptr<TableReader>* table_reader, uint64_t tail_size,
    const std::shared_ptr<CacheReservationManager>& table_reader_cache_res_mgr,
    bool prefetch_index_in_cache, int level, const bool immortal_table,
    const SequenceNumber largest_seqno, bool force_direct_prefetch,
    TailPrefetchStats* tail_prefetch_stats,
    BlockCacheTracer* const block_cache_tracer,
    size_t max_file_size_for_l0_meta_pin, const std::string& cur_db_session_id,
    uint64_t cur_file_num, UniqueId64x2 expected_unique_id) {
  table_reader->reset();

  Status s;
  Footer footer;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;

  // From read_options, retain deadline, io_timeout, rate_limiter_priority, and
  // verify_checksums. In future, we may retain more options.
  ReadOptions ro;
  ro.deadline = read_options.deadline;
  ro.io_timeout = read_options.io_timeout;
  ro.rate_limiter_priority = read_options.rate_limiter_priority;
  ro.verify_checksums = read_options.verify_checksums;
  ro.io_activity = read_options.io_activity;

  // prefetch both index and filters, down to all partitions
  const bool prefetch_all = prefetch_index_in_cache || level == 0;
  const bool preload_all = !table_options.cache_index_and_filter_blocks;

  if (!ioptions.allow_mmap_reads) {
    s = PrefetchTail(ro, file.get(), file_size, force_direct_prefetch,
                     tail_prefetch_stats, prefetch_all, preload_all,
                     &prefetch_buffer, ioptions.stats, tail_size,
                     ioptions.logger);
    // Return error in prefetch path to users.
    if (!s.ok()) {
      return s;
    }
  } else {
    // Should not prefetch for mmap mode.
    prefetch_buffer = std::make_unique<FilePrefetchBuffer>(
        0 /* readahead_size */, 0 /* max_readahead_size */, false /* enable */,
        true /* track_min_offset */);
  }

  // Read in the following order:
  //    1. Footer
  //    2. [metaindex block]
  //    3. [meta block: properties]
  //    4. [meta block: compression dictionary]
  //    5. [meta block: index]
  // todo:    6. [hnsw block]
  IOOptions opts;
  s = file->PrepareIOOptions(ro, opts);
  if (s.ok()) {
    s = ReadFooterFromFile(opts, file.get(), *ioptions.fs,
                           prefetch_buffer.get(), file_size, &footer,
                           kHnswTableMagicNumber);
  }
  if (!s.ok()) {
    return s;
  }
  if (!IsSupportedFormatVersion(footer.format_version())) {
    return Status::Corruption(
        "Unknown Footer version. Maybe this file was created with newer "
        "version of RocksDB?");
  }

  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  Rep* rep =
      new HnswTable::Rep(ioptions, env_options, table_options,
                         internal_comparator, file_size, level, immortal_table);
  rep->file = std::move(file);
  rep->footer = footer;

  // For fully portable/stable cache keys, we need to read the properties
  // block before setting up cache keys. TODO: consider setting up a bootstrap
  // cache key for PersistentCache to use for metaindex and properties blocks.
  rep->persistent_cache_options = PersistentCacheOptions();

  // Meta-blocks are not dictionary compressed. Explicitly set the dictionary
  // handle to null, otherwise it may be seen as uninitialized during the below
  // meta-block reads.
  rep->compression_dict_handle = BlockHandle::NullBlockHandle();

  rep->create_context.protection_bytes_per_key = block_protection_bytes_per_key;
  // Read metaindex
  std::unique_ptr<HnswTable> new_table(new HnswTable(rep, block_cache_tracer));
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  s = new_table->ReadMetaIndexBlock(ro, prefetch_buffer.get(), &metaindex,
                                    &metaindex_iter);
  if (!s.ok()) {
    return s;
  }

  // Populates table_properties and some fields that depend on it,
  // such as index_type.
  s = new_table->ReadPropertiesBlock(ro, prefetch_buffer.get(),
                                     metaindex_iter.get(), largest_seqno);
  if (!s.ok()) {
    return s;
  }

  // Populate BlockCreateContext
  bool blocks_definitely_zstd_compressed =
      rep->table_properties &&
      (rep->table_properties->compression_name ==
           CompressionTypeToString(kZSTD) ||
       rep->table_properties->compression_name ==
           CompressionTypeToString(kZSTDNotFinalCompression));
  rep->create_context = BlockCreateContext(
      &rep->table_options, rep->ioptions.stats,
      blocks_definitely_zstd_compressed, block_protection_bytes_per_key,
      rep->internal_comparator.user_comparator(), true, false);

  // Check expected unique id if provided
  if (expected_unique_id != kNullUniqueId64x2) {
    auto props = rep->table_properties;
    if (!props) {
      return Status::Corruption("Missing table properties on file " +
                                std::to_string(cur_file_num) +
                                " with known unique ID");
    }
    UniqueId64x2 actual_unique_id{};
    s = GetSstInternalUniqueId(props->db_id, props->db_session_id,
                               props->orig_file_number, &actual_unique_id,
                               /*force*/ true);
    assert(s.ok());  // because force=true
    if (expected_unique_id != actual_unique_id) {
      return Status::Corruption(
          "Mismatch in unique ID on table file " +
          std::to_string(cur_file_num) +
          ". Expected: " + InternalUniqueIdToHumanString(&expected_unique_id) +
          " Actual: " + InternalUniqueIdToHumanString(&actual_unique_id));
    }
    TEST_SYNC_POINT_CALLBACK("HnswTable::Open::PassedVerifyUniqueId",
                             &actual_unique_id);
  } else {
    TEST_SYNC_POINT_CALLBACK("HnswTable::Open::SkippedVerifyUniqueId", nullptr);
    if (ioptions.verify_sst_unique_id_in_manifest && ioptions.logger) {
      // A crude but isolated way of reporting unverified files. This should not
      // be an ongoing concern so doesn't deserve a place in Statistics IMHO.
      static std::atomic<uint64_t> unverified_count{0};
      auto prev_count =
          unverified_count.fetch_add(1, std::memory_order_relaxed);
      if (prev_count == 0) {
        ROCKS_LOG_WARN(
            ioptions.logger,
            "At least one SST file opened without unique ID to verify: %" PRIu64
            ".sst",
            cur_file_num);
      } else if (prev_count % 1000 == 0) {
        ROCKS_LOG_WARN(
            ioptions.logger,
            "Another ~1000 SST files opened without unique ID to verify");
      }
    }
  }

  // With properties loaded, we can set up portable/stable cache keys
  SetupBaseCacheKey(rep->table_properties.get(), cur_db_session_id,
                    cur_file_num, &rep->base_cache_key);

  rep->persistent_cache_options =
      PersistentCacheOptions(rep->table_options.persistent_cache,
                             rep->base_cache_key, rep->ioptions.stats);

  if (!s.ok()) {
    return s;
  }
  s = new_table->PrefetchIndexAndFilterBlocks(
      ro, prefetch_buffer.get(), metaindex_iter.get(), new_table.get(),
      prefetch_all, table_options, level, file_size,
      max_file_size_for_l0_meta_pin, &lookup_context);

  if (s.ok()) {
    // Update tail prefetch stats
    assert(prefetch_buffer.get() != nullptr);
    if (tail_prefetch_stats != nullptr) {
      assert(prefetch_buffer->min_offset_read() < file_size);
      tail_prefetch_stats->RecordEffectiveSize(
          static_cast<size_t>(file_size) - prefetch_buffer->min_offset_read());
    }
  }

  if (s.ok() && table_reader_cache_res_mgr) {
    std::size_t mem_usage = new_table->ApproximateMemoryUsage();
    s = table_reader_cache_res_mgr->MakeCacheReservation(
        mem_usage, &(rep->table_reader_cache_res_handle));
    if (s.IsMemoryLimit()) {
      s = Status::MemoryLimit(
          "Can't allocate " +
          kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
              CacheEntryRole::kHnswTableReader)] +
          " due to memory limit based on "
          "cache capacity for memory allocation");
    }
  }

  if (s.ok()) {
    *table_reader = std::move(new_table);
  }
  return s;
}

Status HnswTable::PrefetchTail(
    const ReadOptions& ro, RandomAccessFileReader* file, uint64_t file_size,
    bool force_direct_prefetch, TailPrefetchStats* tail_prefetch_stats,
    const bool prefetch_all, const bool preload_all,
    std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer, Statistics* stats,
    uint64_t tail_size, Logger* const logger) {
  assert(tail_size <= file_size);

  size_t tail_prefetch_size = 0;
  if (tail_size != 0) {
    tail_prefetch_size = tail_size;
  } else {
    if (tail_prefetch_stats != nullptr) {
      // Multiple threads may get a 0 (no history) when running in parallel,
      // but it will get cleared after the first of them finishes.
      tail_prefetch_size = tail_prefetch_stats->GetSuggestedPrefetchSize();
    }
    if (tail_prefetch_size == 0) {
      // Before read footer, readahead backwards to prefetch data. Do more
      // readahead if we're going to read index/filter.
      // TODO: This may incorrectly select small readahead in case partitioned
      // index/filter is enabled and top-level partition pinning is enabled.
      // That's because we need to issue readahead before we read the
      // properties, at which point we don't yet know the index type.
      tail_prefetch_size = prefetch_all || preload_all ? 512 * 1024 : 4 * 1024;

      ROCKS_LOG_WARN(logger,
                     "Tail prefetch size %zu is calculated based on heuristics",
                     tail_prefetch_size);
    } else {
      ROCKS_LOG_WARN(
          logger,
          "Tail prefetch size %zu is calculated based on TailPrefetchStats",
          tail_prefetch_size);
    }
  }
  size_t prefetch_off;
  size_t prefetch_len;
  if (file_size < tail_prefetch_size) {
    prefetch_off = 0;
    prefetch_len = static_cast<size_t>(file_size);
  } else {
    prefetch_off = static_cast<size_t>(file_size - tail_prefetch_size);
    prefetch_len = tail_prefetch_size;
  }
  TEST_SYNC_POINT_CALLBACK("BlockBasedTable::Open::TailPrefetchLen",
                           &tail_prefetch_size);

  // Try file system prefetch
  if (!file->use_direct_io() && !force_direct_prefetch) {
    if (!file->Prefetch(prefetch_off, prefetch_len, ro.rate_limiter_priority)
             .IsNotSupported()) {
      prefetch_buffer->reset(new FilePrefetchBuffer(
          0 /* readahead_size */, 0 /* max_readahead_size */,
          false /* enable */, true /* track_min_offset */));
      return Status::OK();
    }
  }

  // Use `FilePrefetchBuffer`
  prefetch_buffer->reset(new FilePrefetchBuffer(
      0 /* readahead_size */, 0 /* max_readahead_size */, true /* enable */,
      true /* track_min_offset */, false /* implicit_auto_readahead */,
      0 /* num_file_reads */, 0 /* num_file_reads_for_auto_readahead */,
      nullptr /* fs */, nullptr /* clock */, stats,
      FilePrefetchBufferUsage::kTableOpenPrefetchTail));

  IOOptions opts;
  Status s = file->PrepareIOOptions(ro, opts);
  if (s.ok()) {
    s = (*prefetch_buffer)
            ->Prefetch(opts, file, prefetch_off, prefetch_len,
                       ro.rate_limiter_priority);
  }
  return s;
}

Status HnswTable::ReadPropertiesBlock(const ReadOptions& ro,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      InternalIterator* meta_iter,
                                      const SequenceNumber largest_seqno) {
  Status s;
  BlockHandle handle;
  s = FindOptionalMetaBlock(meta_iter, kPropertiesBlockName, &handle);

  if (!s.ok()) {
    ROCKS_LOG_WARN(rep_->ioptions.logger,
                   "Error when seeking to properties block from file: %s",
                   s.ToString().c_str());
  } else if (!handle.IsNull()) {
    s = meta_iter->status();
    std::unique_ptr<TableProperties> table_properties;
    if (s.ok()) {
      s = ReadTablePropertiesHelper(
          ro, handle, rep_->file.get(), prefetch_buffer, rep_->footer,
          rep_->ioptions, &table_properties, nullptr /* memory_allocator */);
    }
    IGNORE_STATUS_IF_ERROR(s);

    if (!s.ok()) {
      ROCKS_LOG_WARN(rep_->ioptions.logger,
                     "Encountered error while reading data from properties "
                     "block %s",
                     s.ToString().c_str());
    } else {
      assert(table_properties != nullptr);
      rep_->table_properties = std::move(table_properties);
      rep_->blocks_maybe_compressed =
          rep_->table_properties->compression_name !=
          CompressionTypeToString(kNoCompression);
    }
  } else {
    ROCKS_LOG_ERROR(rep_->ioptions.logger,
                    "Cannot find Properties block from file.");
  }

  // Read the table properties, if provided.
  if (rep_->table_properties) {
    // Update index_type with the true type.
    // If table properties don't contain index type, we assume that the table
    // is in very old format and has kBinarySearch index type.
    auto& props = rep_->table_properties->user_collected_properties;
    auto index_type_pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (index_type_pos != props.end()) {
      rep_->index_type = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(index_type_pos->second.c_str()));
    }
    auto min_ts_pos = props.find("rocksdb.timestamp_min");
    if (min_ts_pos != props.end()) {
      rep_->min_timestamp = Slice(min_ts_pos->second);
    }
    auto max_ts_pos = props.find("rocksdb.timestamp_max");
    if (max_ts_pos != props.end()) {
      rep_->max_timestamp = Slice(max_ts_pos->second);
    }

    s = GetGlobalSequenceNumber(*(rep_->table_properties), largest_seqno,
                                &(rep_->global_seqno));
    if (!s.ok()) {
      ROCKS_LOG_ERROR(rep_->ioptions.logger, "%s", s.ToString().c_str());
    }
  }
  return s;
}

Status HnswTable::PrefetchIndexAndFilterBlocks(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter, HnswTable* new_table, bool prefetch_all,
    const HnswTableOptions& table_options, const int level, size_t file_size,
    size_t max_file_size_for_l0_meta_pin,
    BlockCacheLookupContext* lookup_context) {
  // Find compression dictionary handle
  Status s = FindOptionalMetaBlock(meta_iter, kCompressionDictBlockName,
                                   &rep_->compression_dict_handle);
  if (!s.ok()) {
    return s;
  }

  HnswTableOptions::IndexType index_type = rep_->index_type;

  const bool use_cache = table_options.cache_index_and_filter_blocks;

  const bool maybe_flushed =
      level == 0 && file_size <= max_file_size_for_l0_meta_pin;
  std::function<bool(PinningTier, PinningTier)> is_pinned =
      [maybe_flushed, &is_pinned](PinningTier pinning_tier,
                                  PinningTier fallback_pinning_tier) {
        // Fallback to fallback would lead to infinite recursion. Disallow it.
        assert(fallback_pinning_tier != PinningTier::kFallback);

        switch (pinning_tier) {
          case PinningTier::kFallback:
            return is_pinned(fallback_pinning_tier,
                             PinningTier::kNone /* fallback_pinning_tier */);
          case PinningTier::kNone:
            return false;
          case PinningTier::kFlushedAndSimilar:
            return maybe_flushed;
          case PinningTier::kAll:
            return true;
        };

        // In GCC, this is needed to suppress `control reaches end of non-void
        // function [-Werror=return-type]`.
        assert(false);
        return false;
      };
  const bool pin_top_level_index = is_pinned(
      table_options.metadata_cache_options.top_level_index_pinning,
      table_options.pin_top_level_index_and_filter ? PinningTier::kAll
                                                   : PinningTier::kNone);
  const bool pin_partition =
      is_pinned(table_options.metadata_cache_options.partition_pinning,
                table_options.pin_l0_filter_and_index_blocks_in_cache
                    ? PinningTier::kFlushedAndSimilar
                    : PinningTier::kNone);
  const bool pin_unpartitioned =
      is_pinned(table_options.metadata_cache_options.unpartitioned_pinning,
                table_options.pin_l0_filter_and_index_blocks_in_cache
                    ? PinningTier::kFlushedAndSimilar
                    : PinningTier::kNone);

  // pin the first level of index
  const bool pin_index =
      index_type == BlockBasedTableOptions::kTwoLevelIndexSearch
          ? pin_top_level_index
          : pin_unpartitioned;
  // prefetch the first level of index
  // WART: this might be redundant (unnecessary cache hit) if !pin_index,
  // depending on prepopulate_block_cache option
  const bool prefetch_index = prefetch_all || pin_index;

  std::unique_ptr<IndexReader> index_reader;
  s = new_table->CreateIndexReader(ro, prefetch_buffer, meta_iter, use_cache,
                                   prefetch_index, pin_index, lookup_context,
                                   &index_reader);
  if (!s.ok()) {
    return s;
  }

  rep_->index_reader = std::move(index_reader);

  // The partitions of partitioned index are always stored in cache. They
  // are hence follow the configuration for pin and prefetch regardless of
  // the value of cache_index_and_filter_blocks
  if (prefetch_all || pin_partition) {
    s = rep_->index_reader->CacheDependencies(ro, pin_partition,
                                              prefetch_buffer);
  }
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<HnswReader> hnsw_reader;
  s = new_table->ReadHnswBlock(ro, prefetch_buffer, use_cache, prefetch_index,
                               pin_index, lookup_context, &hnsw_reader);

  if (!s.ok()) {
    return s;
  }

  rep_->hnsw_reader = std::move(hnsw_reader);

  if (!rep_->compression_dict_handle.IsNull()) {
    std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;
    s = UncompressionDictReader::Create(
        this, ro, prefetch_buffer, use_cache, prefetch_all || pin_unpartitioned,
        pin_unpartitioned, lookup_context, &uncompression_dict_reader);
    if (!s.ok()) {
      return s;
    }

    rep_->uncompression_dict_reader = std::move(uncompression_dict_reader);
  }

  assert(s.ok());
  return s;
}

void HnswTable::SetupForCompaction() {
  switch (rep_->ioptions.access_hint_on_compaction_start) {
    case Options::NONE:
      break;
    case Options::NORMAL:
      rep_->file->file()->Hint(FSRandomAccessFile::kNormal);
      break;
    case Options::SEQUENTIAL:
      rep_->file->file()->Hint(FSRandomAccessFile::kSequential);
      break;
    case Options::WILLNEED:
      rep_->file->file()->Hint(FSRandomAccessFile::kWillNeed);
      break;
    default:
      assert(false);
  }
}

std::shared_ptr<const TableProperties> HnswTable::GetTableProperties() const {
  return rep_->table_properties;
}

size_t HnswTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_) {
    usage += rep_->ApproximateMemoryUsage();
  } else {
    return usage;
  }
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  if (rep_->hnsw_reader) {
    usage += rep_->hnsw_reader->ApproximateMemoryUsage();
  }
  if (rep_->uncompression_dict_reader) {
    usage += rep_->uncompression_dict_reader->ApproximateMemoryUsage();
  }
  if (rep_->table_properties) {
    usage += rep_->table_properties->ApproximateMemoryUsage();
  }
  return usage;
}

// Load the meta-index-block from the file. On success, return the loaded
// metaindex
// block and its iterator.
Status HnswTable::ReadMetaIndexBlock(const ReadOptions& ro,
                                     FilePrefetchBuffer* prefetch_buffer,
                                     std::unique_ptr<Block>* metaindex_block,
                                     std::unique_ptr<InternalIterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  std::unique_ptr<Block_kMetaIndex> metaindex;
  Status s = ReadAndParseBlockFromFile(
      rep_->file.get(), prefetch_buffer, rep_->footer, ro,
      rep_->footer.metaindex_handle(), &metaindex, rep_->ioptions,
      rep_->create_context, true /*maybe_compressed*/,
      UncompressionDict::GetEmptyDict(), rep_->persistent_cache_options,
      GetMemoryAllocator(rep_->table_options), false /* for_compaction */,
      false /* async_read */);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep_->ioptions.logger,
                    "Encountered error while reading data from properties"
                    " block %s",
                    s.ToString().c_str());
    return s;
  }

  *metaindex_block = std::move(metaindex);
  // meta block uses bytewise comparator.
  iter->reset(metaindex_block->get()->NewMetaIterator());
  return Status::OK();
}

Status HnswTable::ReadHnswBlock(const ReadOptions& ro,
                                FilePrefetchBuffer* prefetch_buffer,
                                bool use_cache, bool prefetch, bool pin,
                                BlockCacheLookupContext* lookup_context,
                                std::unique_ptr<HnswReader>* hnsw_reader) {
  CachableEntry<Block_kHnsw> hnsw_block;
  Status s = RetrieveBlock(prefetch_buffer, ro, rep_->footer.hnsw_handle(),
                           UncompressionDict::GetEmptyDict(), &hnsw_block,
                           /*get_context=*/nullptr, lookup_context,
                           /* for_compaction */ false, use_cache,
                           /* async_read */ false);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep_->ioptions.logger,
                    "Encountered error while reading data from block %s",
                    s.ToString().c_str());
    return s;
  }

  return HnswReader::Create(this, ro, &hnsw_block, hnsw_reader);
}

template <typename TBlocklike>
Cache::Priority HnswTable::GetCachePriority() const {
  // Here we treat the legacy name "...index_and_filter_blocks..." to mean all
  // metadata blocks that might go into block cache, EXCEPT only those needed
  // for the read path (Get, etc.). TableProperties should not be needed on the
  // read path (prefix extractor setting is an O(1) size special case that we
  // are working not to require from TableProperties), so it is not given
  // high-priority treatment if it should go into BlockCache.
  if constexpr (TBlocklike::kBlockType == BlockType::kData ||
                TBlocklike::kBlockType == BlockType::kProperties) {
    return Cache::Priority::LOW;
  } else if (rep_->table_options
                 .cache_index_and_filter_blocks_with_high_priority) {
    return Cache::Priority::HIGH;
  } else {
    return Cache::Priority::LOW;
  }
}

template <typename TBlocklike>
WithBlocklikeCheck<Status, TBlocklike> HnswTable::GetDataBlockFromCache(
    const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
    CachableEntry<TBlocklike>* out_parsed_block,
    GetContext* get_context) const {
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  Status s;
  Statistics* statistics = rep_->ioptions.statistics.get();

  // Lookup uncompressed cache first
  if (block_cache) {
    assert(!cache_key.empty());
    auto cache_handle = block_cache.LookupFull(
        cache_key, &rep_->create_context, GetCachePriority<TBlocklike>(),
        statistics, rep_->ioptions.lowest_used_cache_tier);

    // Avoid updating metrics here if the handle is not complete yet. This
    // happens with MultiGet and secondary cache. So update the metrics only
    // if its a miss, or a hit and value is ready
    if (!cache_handle) {
      UpdateCacheMissMetrics(TBlocklike::kBlockType, get_context);
    } else {
      TBlocklike* value = block_cache.Value(cache_handle);
      if (value) {
        UpdateCacheHitMetrics(TBlocklike::kBlockType, get_context,
                              block_cache.get()->GetUsage(cache_handle));
      }
      out_parsed_block->SetCachedValue(value, block_cache.get(), cache_handle);
      return s;
    }
  }

  // If not found, search from the compressed block cache.
  assert(out_parsed_block->IsEmpty());

  return s;
}

template <typename TBlocklike>
WithBlocklikeCheck<Status, TBlocklike> HnswTable::PutDataBlockToCache(
    const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
    CachableEntry<TBlocklike>* out_parsed_block, BlockContents&& block_contents,
    CompressionType block_comp_type,
    const UncompressionDict& uncompression_dict,
    MemoryAllocator* memory_allocator, GetContext* get_context) const {
  const ImmutableOptions& ioptions = rep_->ioptions;
  const uint32_t format_version = rep_->table_options.format_version;
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  Status s;
  Statistics* statistics = ioptions.stats;

  std::unique_ptr<TBlocklike> block_holder;
  if (block_comp_type != kNoCompression) {
    // Retrieve the uncompressed contents into a new buffer
    BlockContents uncompressed_block_contents;
    UncompressionContext context(block_comp_type);
    UncompressionInfo info(context, uncompression_dict, block_comp_type);
    s = UncompressBlockData(info, block_contents.data.data(),
                            block_contents.data.size(),
                            &uncompressed_block_contents, format_version,
                            ioptions, memory_allocator);
    if (!s.ok()) {
      return s;
    }
    rep_->create_context.Create(&block_holder,
                                std::move(uncompressed_block_contents));
  } else {
    rep_->create_context.Create(&block_holder, std::move(block_contents));
  }

  // insert into uncompressed block cache
  if (block_cache && block_holder->own_bytes()) {
    size_t charge = block_holder->ApproximateMemoryUsage();
    BlockCacheTypedHandle<TBlocklike>* cache_handle = nullptr;
    s = block_cache.InsertFull(cache_key, block_holder.get(), charge,
                               &cache_handle, GetCachePriority<TBlocklike>(),
                               rep_->ioptions.lowest_used_cache_tier);

    if (s.ok()) {
      assert(cache_handle != nullptr);
      out_parsed_block->SetCachedValue(block_holder.release(),
                                       block_cache.get(), cache_handle);

      UpdateCacheInsertionMetrics(TBlocklike::kBlockType, get_context, charge,
                                  s.IsOkOverwritten(), rep_->ioptions.stats);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
    }
  } else {
    out_parsed_block->SetOwnedValue(std::move(block_holder));
  }

  return s;
}

// disable_prefix_seek should be set to true when prefix_extractor found in SST
// differs from the one in mutable_cf_options and index type is HashBasedIndex
HnswIndexBlockIter* HnswTable::NewIndexIterator(
    const ReadOptions& read_options, HnswIndexBlockIter* input_iter,
    GetContext* get_context, BlockCacheLookupContext* lookup_context) const {
  assert(rep_ != nullptr);
  assert(rep_->index_reader != nullptr);

  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  return rep_->index_reader->NewIterator(read_options, input_iter, get_context,
                                         lookup_context);
}

// TODO?
template <>
DataBlockIter* HnswTable::InitBlockIterator<DataBlockIter>(
    const Rep* rep, Block* block, BlockType block_type,
    DataBlockIter* input_iter, bool block_contents_pinned) {
  return block->NewDataIterator(rep->internal_comparator.user_comparator(),
                                rep->get_global_seqno(block_type), input_iter,
                                rep->ioptions.stats, block_contents_pinned);
}

template <>
GorillaCompressedDataBlockIter*
HnswTable::InitBlockIterator<GorillaCompressedDataBlockIter>(
    const Rep* rep, Block* block, BlockType block_type,
    GorillaCompressedDataBlockIter* input_iter, bool block_contents_pinned) {
  return block->NewGorillaCompressedDataBlockIterator(
      input_iter, &(rep->table_options),
      rep->base_cache_key.CommonPrefixSlice().ToString(),
      block_contents_pinned);
}

// TODO?
template <>
HnswIndexBlockIter* HnswTable::InitBlockIterator<HnswIndexBlockIter>(
    const Rep* rep, Block* block, BlockType block_type,
    HnswIndexBlockIter* input_iter, bool block_contents_pinned) {
  return block->NewHnswIndexBlockIterator(input_iter, block_contents_pinned);
}

// If contents is nullptr, this function looks up the block caches for the
// data block referenced by handle, and read the block from disk if necessary.
// If contents is non-null, it skips the cache lookup and disk read, since
// the caller has already read it. In both cases, if ro.fill_cache is true,
// it inserts the block into the block cache.
template <typename TBlocklike>
WithBlocklikeCheck<Status, TBlocklike> HnswTable::MaybeReadBlockAndLoadToCache(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
    const BlockHandle& handle, const UncompressionDict& uncompression_dict,
    bool for_compaction, CachableEntry<TBlocklike>* out_parsed_block,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    BlockContents* contents, bool async_read) const {
  assert(out_parsed_block != nullptr);
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  BlockCacheInterface<TBlocklike> block_cache{
      rep_->table_options.block_cache.get()};

  // First, try to get the block from the cache
  //
  // If either block cache is enabled, we'll try to read from it.
  Status s;
  CacheKey key_data;
  Slice key;
  bool is_cache_hit = false;
  if (block_cache) {
    // create key for block cache
    key_data = GetCacheKey(rep_->base_cache_key, handle);
    key = key_data.AsSlice();

    if (!contents) {
      s = GetDataBlockFromCache(key, block_cache, out_parsed_block,
                                get_context);
      // Value could still be null at this point, so check the cache handle
      // and update the read pattern for prefetching
      if (out_parsed_block->GetValue() || out_parsed_block->GetCacheHandle()) {
        // TODO(haoyu): Differentiate cache hit on uncompressed block cache and
        // compressed block cache.
        is_cache_hit = true;
        if (prefetch_buffer) {
          // Update the block details so that PrefetchBuffer can use the read
          // pattern to determine if reads are sequential or not for
          // prefetching. It should also take in account blocks read from cache.
          prefetch_buffer->UpdateReadPattern(
              handle.offset(), BlockSizeWithTrailer(handle),
              ro.adaptive_readahead /*decrease_readahead_size*/);
        }
      }
    }

    // Can't find the block from the cache. If I/O is allowed, read from the
    // file.
    if (out_parsed_block->GetValue() == nullptr &&
        out_parsed_block->GetCacheHandle() == nullptr && !no_io &&
        ro.fill_cache) {
      Statistics* statistics = rep_->ioptions.stats;
      const bool maybe_compressed =
          TBlocklike::kBlockType != BlockType::kFilter &&
          TBlocklike::kBlockType != BlockType::kCompressionDictionary &&
          rep_->blocks_maybe_compressed;
      const bool do_uncompress = maybe_compressed;
      CompressionType contents_comp_type;
      // Maybe serialized or uncompressed
      BlockContents tmp_contents;
      if (!contents) {
        Histograms histogram = for_compaction ? READ_BLOCK_COMPACTION_MICROS
                                              : READ_BLOCK_GET_MICROS;
        StopWatch sw(rep_->ioptions.clock, statistics, histogram);
        BlockFetcher block_fetcher(
            rep_->file.get(), prefetch_buffer, rep_->footer, ro, handle,
            &tmp_contents, rep_->ioptions, do_uncompress, maybe_compressed,
            TBlocklike::kBlockType, uncompression_dict,
            rep_->persistent_cache_options,
            GetMemoryAllocator(rep_->table_options),
            /*allocator=*/nullptr);

        // If prefetch_buffer is not allocated, it will fallback to synchronous
        // reading of block contents.
        if (async_read && prefetch_buffer != nullptr) {
          s = block_fetcher.ReadAsyncBlockContents();
          if (!s.ok()) {
            return s;
          }
        } else {
          s = block_fetcher.ReadBlockContents();
        }

        contents_comp_type = block_fetcher.get_compression_type();
        contents = &tmp_contents;
        if (get_context) {
          switch (TBlocklike::kBlockType) {
            case BlockType::kIndex:
              ++get_context->get_context_stats_.num_index_read;
              break;
            case BlockType::kFilter:
            case BlockType::kFilterPartitionIndex:
              ++get_context->get_context_stats_.num_filter_read;
              break;
              //            case BlockType::kHnswIndex:
              //              ++get_context->get_context_stats_.num_vector_index_read;
              //              break;
            default:
              break;
          }
        }
      } else {
        contents_comp_type = GetBlockCompressionType(*contents);
      }

      if (s.ok()) {
        // If filling cache is allowed and a cache is configured, try to put the
        // block to the cache.
        s = PutDataBlockToCache(
            key, block_cache, out_parsed_block, std::move(*contents),
            contents_comp_type, uncompression_dict,
            GetMemoryAllocator(rep_->table_options), get_context);
      }
    }
  }

  // TODO: optimize so that lookup_context != nullptr implies the others
  if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled() &&
      lookup_context) {
    SaveLookupContextOrTraceRecord(
        key, is_cache_hit, ro, out_parsed_block->GetValue(), lookup_context);
  }

  assert(s.ok() || out_parsed_block->GetValue() == nullptr);
  return s;
}

template <typename TBlocklike>
WithBlocklikeCheck<void, TBlocklike> HnswTable::SaveLookupContextOrTraceRecord(
    const Slice& block_key, bool is_cache_hit, const ReadOptions& ro,
    const TBlocklike* parsed_block_value,
    BlockCacheLookupContext* lookup_context) const {
  assert(lookup_context);
  size_t usage = 0;
  uint64_t nkeys = 0;
  if (parsed_block_value) {
    // Approximate the number of keys in the block using restarts.
    int interval = rep_->table_options.block_restart_interval;
    nkeys = interval * GetBlockNumRestarts(*parsed_block_value);
    // On average, the last restart should be just over half utilized.
    // Specifically, 1..N should be N/2 + 0.5. For example, 7 -> 4, 8 -> 4.5.
    // Use the get_id to alternate between rounding up vs. down.
    if (nkeys > 0) {
      bool rounding = static_cast<int>(lookup_context->get_id) & 1;
      nkeys -= (interval - rounding) / 2;
    }
    usage = parsed_block_value->ApproximateMemoryUsage();
  }
  TraceType trace_block_type = TraceType::kTraceMax;
  switch (TBlocklike::kBlockType) {
    case BlockType::kData:
      trace_block_type = TraceType::kBlockTraceDataBlock;
      break;
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      trace_block_type = TraceType::kBlockTraceFilterBlock;
      break;
    case BlockType::kCompressionDictionary:
      trace_block_type = TraceType::kBlockTraceUncompressionDictBlock;
      break;
    case BlockType::kRangeDeletion:
      trace_block_type = TraceType::kBlockTraceRangeDeletionBlock;
      break;
    case BlockType::kIndex:
      trace_block_type = TraceType::kBlockTraceIndexBlock;
      break;
    case BlockType::kHnswIndex:
      trace_block_type = TraceType::kBlockTraceVectorIndexBlock;
      break;
    default:
      // This cannot happen.
      assert(false);
      break;
  }
  const bool no_io = ro.read_tier == kBlockCacheTier;
  bool no_insert = no_io || !ro.fill_cache;
  if (BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(
          trace_block_type, lookup_context->caller)) {
    // Make a copy of the block key here since it will be logged later.
    lookup_context->FillLookupContext(is_cache_hit, no_insert, trace_block_type,
                                      /*block_size=*/usage,
                                      block_key.ToString(), nkeys);

    // Defer logging the access to Get() and MultiGet() to trace additional
    // information, e.g., referenced_key
  } else {
    // Avoid making copy of block_key if it doesn't need to be saved in
    // BlockCacheLookupContext
    lookup_context->FillLookupContext(is_cache_hit, no_insert, trace_block_type,
                                      /*block_size=*/usage,
                                      /*block_key=*/{}, nkeys);

    // Fill in default values for irrelevant/unknown fields
    FinishTraceRecord(*lookup_context, block_key,
                      lookup_context->referenced_key,
                      /*does_referenced_key_exist*/ false,
                      /*referenced_data_size*/ 0);
  }
}

void HnswTable::FinishTraceRecord(const BlockCacheLookupContext& lookup_context,
                                  const Slice& block_key,
                                  const Slice& referenced_key,
                                  bool does_referenced_key_exist,
                                  uint64_t referenced_data_size) const {
  // Avoid making copy of referenced_key if it doesn't need to be saved in
  // BlockCacheLookupContext
  BlockCacheTraceRecord access_record(
      rep_->ioptions.clock->NowMicros(),
      /*block_key=*/"", lookup_context.block_type, lookup_context.block_size,
      rep_->cf_id_for_tracing(),
      /*cf_name=*/"", rep_->level_for_tracing(), rep_->sst_number_for_tracing(),
      lookup_context.caller, lookup_context.is_cache_hit,
      lookup_context.no_insert, lookup_context.get_id,
      lookup_context.get_from_user_specified_snapshot,
      /*referenced_key=*/"", referenced_data_size,
      lookup_context.num_keys_in_block, does_referenced_key_exist);
  // TODO: Should handle status here?
  block_cache_tracer_
      ->WriteBlockAccess(access_record, block_key, rep_->cf_name_for_tracing(),
                         referenced_key)
      .PermitUncheckedError();
}

template <typename TBlocklike /*, auto*/>
WithBlocklikeCheck<Status, TBlocklike> HnswTable::RetrieveBlock(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
    const BlockHandle& handle, const UncompressionDict& uncompression_dict,
    CachableEntry<TBlocklike>* out_parsed_block, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, bool for_compaction,
    bool use_cache, bool async_read) const {
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  Status s;
  if (use_cache) {
    s = MaybeReadBlockAndLoadToCache(
        prefetch_buffer, ro, handle, uncompression_dict, for_compaction,
        out_parsed_block, get_context, lookup_context,
        /*contents=*/nullptr, async_read);

    if (!s.ok()) {
      return s;
    }

    if (out_parsed_block->GetValue() != nullptr ||
        out_parsed_block->GetCacheHandle() != nullptr) {
      assert(s.ok());
      return s;
    }
  }

  assert(out_parsed_block->IsEmpty());

  const bool no_io = ro.read_tier == kBlockCacheTier;
  if (no_io) {
    return Status::Incomplete("no blocking io");
  }

  const bool maybe_compressed =
      TBlocklike::kBlockType != BlockType::kFilter &&
      TBlocklike::kBlockType != BlockType::kCompressionDictionary &&
      rep_->blocks_maybe_compressed;
  std::unique_ptr<TBlocklike> block;

  {
    Histograms histogram =
        for_compaction ? READ_BLOCK_COMPACTION_MICROS : READ_BLOCK_GET_MICROS;
    StopWatch sw(rep_->ioptions.clock, rep_->ioptions.stats, histogram);
    s = ReadAndParseBlockFromFile(
        rep_->file.get(), prefetch_buffer, rep_->footer, ro, handle, &block,
        rep_->ioptions, rep_->create_context, maybe_compressed,
        uncompression_dict, rep_->persistent_cache_options,
        GetMemoryAllocator(rep_->table_options), for_compaction, async_read);

    if (get_context) {
      switch (TBlocklike::kBlockType) {
        case BlockType::kIndex:
          ++(get_context->get_context_stats_.num_index_read);
          break;
        case BlockType::kFilter:
        case BlockType::kFilterPartitionIndex:
          ++(get_context->get_context_stats_.num_filter_read);
          break;
          //        case BlockType::kHnswIndex:
          //          ++(get_context->get_context_stats_.num_vector_index_read);
        default:
          break;
      }
    }
  }

  if (!s.ok()) {
    return s;
  }

  out_parsed_block->SetOwnedValue(std::move(block));

  assert(s.ok());
  return s;
}

Statistics* HnswTable::GetStatistics() const { return rep_->ioptions.stats; }
bool HnswTable::IsLastLevel() const {
  return rep_->level == rep_->ioptions.num_levels - 1;
}

InternalIterator* HnswTable::NewIterator(const ReadOptions& read_options,
                                         const SliceTransform* prefix_extractor,
                                         Arena* arena, bool skip_filters,
                                         TableReaderCaller caller,
                                         size_t compaction_readahead_size,
                                         bool allow_unprepared_value) {
  BlockCacheLookupContext lookup_context{caller};
  std::unique_ptr<HnswIndexBlockIter> index_iter(NewIndexIterator(
      read_options,
      /*input_iter=*/nullptr, /*get_context=*/nullptr, &lookup_context));
  if (arena == nullptr) {
    return new HnswTableIterator(this, read_options, std::move(index_iter),
                                 caller, compaction_readahead_size,
                                 allow_unprepared_value);
  } else {
    auto* mem = arena->AllocateAligned(sizeof(HnswTableIterator));
    return new (mem)
        HnswTableIterator(this, read_options, std::move(index_iter), caller,
                          compaction_readahead_size, allow_unprepared_value);
  }
}

Status HnswTable::ApproximateKeyAnchors(const ReadOptions& read_options,
                                        std::vector<Anchor>& anchors) {
  // We iterator the whole index block here. More efficient implementation
  // is possible if we push this operation into IndexReader. For example, we
  // can directly sample from restart block entries in the index block and
  // only read keys needed. Here we take a simple solution. Performance is
  // likely not to be a problem. We are compacting the whole file, so all
  // keys will be read out anyway. An extra read to index block might be
  // a small share of the overhead. We can try to optimize if needed.
  //
  // `CacheDependencies()` brings all the blocks into cache using one I/O. That
  // way the full index scan usually finds the index data it is looking for in
  // cache rather than doing an I/O for each "dependency" (partition).
  Status s = rep_->index_reader->CacheDependencies(
      read_options, false /* pin */, nullptr /* prefetch_buffer */);
  if (!s.ok()) {
    return s;
  }

  HnswIndexBlockIter iiter_on_stack;
  auto iiter =
      NewIndexIterator(read_options, &iiter_on_stack,
                       /*get_context=*/nullptr, /*lookup_context=*/nullptr);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr.reset(iiter);
  }

  // If needed the threshold could be more adaptive. For example, it can be
  // based on size, so that a larger will be sampled to more partitions than a
  // smaller file. The size might also need to be passed in by the caller based
  // on total compaction size.
  const uint64_t kMaxNumAnchors = uint64_t{128};
  uint64_t num_blocks = this->GetTableProperties()->num_data_blocks;
  uint64_t num_blocks_per_anchor = num_blocks / kMaxNumAnchors;
  if (num_blocks_per_anchor == 0) {
    num_blocks_per_anchor = 1;
  }

  uint64_t count = 0;
  std::string last_key;
  uint64_t range_size = 0;
  uint64_t prev_offset = 0;
  for (iiter->SeekToFirst(); iiter->Valid(); iiter->Next()) {
    const BlockHandle& bh = iiter->value();
    range_size += bh.offset() + bh.size() - prev_offset;
    prev_offset = bh.offset() + bh.size();
    if (++count % num_blocks_per_anchor == 0) {
      count = 0;
      anchors.emplace_back(iiter->user_key(), range_size);
      range_size = 0;
    } else {
      last_key = iiter->user_key().ToString();
    }
  }
  if (count != 0) {
    anchors.emplace_back(last_key, range_size);
  }
  return Status::OK();
}

Status HnswTable::GetData(const ReadOptions& read_options,
                          uint32_t target_internal_id, Entry* entry) {
  // assert(key.size() >= 8);  // key must be internal key
  // assert(get_context != nullptr);
  Status s;
  const bool no_io = read_options.read_tier == kBlockCacheTier;

  HnswIndexBlockIter iiter_on_stack;
  auto iiter =
      NewIndexIterator(read_options, &iiter_on_stack, nullptr, nullptr);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr.reset(iiter);
  }

  bool done = false;
  for (iiter->Seek(target_internal_id); iiter->Valid(); iiter->Next()) {
    BlockHandle handle = iiter->value();

    GorillaCompressedDataBlockIter biter;
    uint64_t referenced_data_size = 0;
    Status tmp_status;
    NewDataBlockIterator<GorillaCompressedDataBlockIter>(
        read_options, handle, &biter, BlockType::kData,
        /*prefetch_buffer=*/nullptr,
        /*for_compaction=*/false, /*async_read=*/false, tmp_status);

    if (no_io && biter.status().IsIncomplete()) {
      s = biter.status();
      break;
    }
    if (!biter.status().ok()) {
      s = biter.status();
      break;
    }

    biter.Seek(target_internal_id);

    // Call the *saver function on each entry/block until it returns false
    for (; biter.Valid(); biter.Next()) {
      uint32_t internal_id = biter.current_internal_id();

      if (internal_id == target_internal_id) {
        entry = biter.value();
        done = true;
        break;
      }
    }
    s = biter.status();
    if (!s.ok()) {
      break;
    }

    if (done) {
      // Avoid the extra Next which is expensive in two-level indexes
      break;
    }
  }

  if (s.ok() && !iiter->status().IsNotFound()) {
    s = iiter->status();
  }

  return s;
}

Status HnswTable::Get(const ReadOptions& read_options, const Slice& key,
                      GetContext* get_context,
                      const SliceTransform* prefix_extractor,
                      bool skip_filters) {
  if (read_options.GetType() != ReadOptions::kVectorSearch) {
    return Status::InvalidArgument("read options is not VectorSearchOptions");
  }
  auto* vector_search_options =
      static_cast<const VectorSearchOptions*>(&read_options);
  auto* snapshot = read_options.snapshot;
  SequenceNumber sequence_number;
  if (snapshot != nullptr) {
    sequence_number = snapshot->GetSequenceNumber();
  }
  size_t k = vector_search_options->k;
  uint64_t ts = vector_search_options->ts;
  uint64_t file_number = get_context->GetFileNumber();
  auto result = rep_->hnsw_reader->searchKnn(
      key.data(), k, ts, snapshot != nullptr ? &sequence_number : nullptr,
      file_number);
  auto value_slice = get_context->Value();
  std::string* value;
  std::string s;
  dist_t total_dist = 0.0f;
  if (value_slice) {
    value = value_slice->GetSelf();
    if (!value->empty()) {
      // 归并
      const char* data = value->data();
      uint16_t size;
      memcpy(&size, data, sizeof(size));
      data = data + sizeof(size);
      for (uint32_t i = 0; i < size; ++i) {
        hnswlib::ResultItem<dist_t> item;
        memcpy(&(item.dist), data, sizeof(dist_t));
        data = data + sizeof(dist_t);
        memcpy(&(item.label), data, sizeof(hnswlib::labeltype));
        data = data + sizeof(hnswlib::labeltype);
        memcpy(&(item.version), data, sizeof(SequenceNumber));
        data = data + sizeof(SequenceNumber);
        memcpy(&(item.file_number), data, sizeof(uint64_t));
        data = data + sizeof(uint64_t);
        result.push(item);
        if (result.size() > k) {
          uint64_t _file_number;
          auto evicted_item = result.top();
          result.pop();
          if (evicted_item.file_number == file_number) {
            total_dist += evicted_item.dist;
          }
        }
      }
    } else {
      value = new std::string();
      value->resize(sizeof(uint16_t) +
                    k * (sizeof(dist_t) + sizeof(hnswlib::labeltype) +
                         sizeof(SequenceNumber) + sizeof(uint64_t)));
    }
    char* data = value->data();
    uint16_t result_size = result.size();
    memcpy(data, &result_size, sizeof(uint16_t));
    data += sizeof(uint16_t);
    while (!result.empty()) {
      auto item = result.top();
      result.pop();
      memcpy(data, &(item.dist), sizeof(dist_t));
      data += sizeof(dist_t);
      memcpy(data, &(item.label), sizeof(hnswlib::labeltype));
      data += sizeof(hnswlib::labeltype);
      memcpy(data, &(item.version), sizeof(SequenceNumber));
      data += sizeof(SequenceNumber);
      memcpy(data, &(item.file_number), sizeof(uint64_t));
      data += sizeof(uint64_t);
      if (item.file_number == file_number) {
        total_dist += item.dist;
      }
    }
    Slice value_slice(value->data(), data - value->data());
    // value->resize(data - value->data());
    get_context->SaveValue(value_slice);
    PutFixed32(&s, *reinterpret_cast<uint32_t*>(&total_dist));
  }
  // 通过 Status 返回参数
  return {Status::Code::kOk, Status::SubCode::kNone, Status::Severity::kNoError,
          s};
}

Status HnswTable::Prefetch(const ReadOptions& read_options,
                           const Slice* const begin, const Slice* const end) {
  auto& comparator = rep_->internal_comparator;
  UserComparatorWrapper user_comparator(comparator.user_comparator());
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }
  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  HnswIndexBlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(read_options, &iiter_on_stack,
                                /*get_context=*/nullptr, &lookup_context);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr =
        std::unique_ptr<InternalIteratorBase<BlockHandle>>(iiter);
  }

  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter->Seek(*begin) : iiter->SeekToFirst(); iiter->Valid();
       iiter->Next()) {
    BlockHandle block_handle = iiter->value();
    const bool is_user_key = true;
    // todo
    if (end &&
        user_comparator.Compare(iiter->key(), ExtractUserKey(*end)) >= 0) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    GorillaCompressedDataBlockIter biter;
    Status tmp_status;
    NewDataBlockIterator<GorillaCompressedDataBlockIter>(
        read_options, block_handle, &biter, /*type=*/BlockType::kData,
        /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
        /*async_read=*/false, tmp_status);

    if (!biter.status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter.status();
    }
  }

  return Status::OK();
}

Status HnswTable::VerifyChecksum(const ReadOptions& read_options,
                                 TableReaderCaller caller) {
  Status s;
  // Check Meta blocks
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  s = ReadMetaIndexBlock(read_options, nullptr /* prefetch buffer */,
                         &metaindex, &metaindex_iter);
  if (s.ok()) {
    s = VerifyChecksumInMetaBlocks(read_options, metaindex_iter.get());
    if (!s.ok()) {
      return s;
    }
  } else {
    return s;
  }
  // Check Data blocks
  HnswIndexBlockIter iiter_on_stack;
  BlockCacheLookupContext context{caller};
  InternalIteratorBase<BlockHandle>* iiter =
      NewIndexIterator(read_options, &iiter_on_stack,
                       /*get_context=*/nullptr, &context);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr =
        std::unique_ptr<InternalIteratorBase<BlockHandle>>(iiter);
  }
  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }
  s = VerifyChecksumInBlocks(read_options, iiter);
  return s;
}

Status HnswTable::VerifyChecksumInBlocks(
    const ReadOptions& read_options,
    InternalIteratorBase<BlockHandle>* index_iter) {
  Status s;
  // We are scanning the whole file, so no need to do exponential
  // increasing of the buffer size.
  size_t readahead_size = (read_options.readahead_size != 0)
                              ? read_options.readahead_size
                              : rep_->table_options.max_auto_readahead_size;
  // FilePrefetchBuffer doesn't work in mmap mode and readahead is not
  // needed there.
  FilePrefetchBuffer prefetch_buffer(
      readahead_size /* readahead_size */,
      readahead_size /* max_readahead_size */,
      !rep_->ioptions.allow_mmap_reads /* enable */);

  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle = index_iter->value();
    BlockContents contents;
    BlockFetcher block_fetcher(
        rep_->file.get(), &prefetch_buffer, rep_->footer, read_options, handle,
        &contents, rep_->ioptions, false /* decompress */,
        false /*maybe_compressed*/, BlockType::kData,
        UncompressionDict::GetEmptyDict(), rep_->persistent_cache_options);
    s = block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    // In the case of two level indexes, we would have exited the above loop
    // by checking index_iter->Valid(), but Valid() might have returned false
    // due to an IO error. So check the index_iter status
    s = index_iter->status();
  }
  return s;
}

BlockType HnswTable::GetBlockTypeForMetaBlockByName(
    const Slice& meta_block_name) {
  if (meta_block_name == kPropertiesBlockName) {
    return BlockType::kProperties;
  }

  if (meta_block_name == kCompressionDictBlockName) {
    return BlockType::kCompressionDictionary;
  }

  if (meta_block_name == kRangeDelBlockName) {
    return BlockType::kRangeDeletion;
  }

  if (meta_block_name == kHashIndexPrefixesBlock) {
    return BlockType::kHashIndexPrefixes;
  }

  if (meta_block_name == kHashIndexPrefixesMetadataBlock) {
    return BlockType::kHashIndexMetadata;
  }

  assert(false);
  return BlockType::kInvalid;
}

Status HnswTable::VerifyChecksumInMetaBlocks(
    const ReadOptions& read_options, InternalIteratorBase<Slice>* index_iter) {
  Status s;
  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle;
    Slice input = index_iter->value();
    s = handle.DecodeFrom(&input);
    BlockContents contents;
    const Slice meta_block_name = index_iter->key();
    if (meta_block_name == kPropertiesBlockName) {
      // Unfortunate special handling for properties block checksum w/
      // global seqno
      std::unique_ptr<TableProperties> table_properties;
      s = ReadTablePropertiesHelper(read_options, handle, rep_->file.get(),
                                    nullptr /* prefetch_buffer */, rep_->footer,
                                    rep_->ioptions, &table_properties,
                                    nullptr /* memory_allocator */);
    } else {
      s = BlockFetcher(
              rep_->file.get(), nullptr /* prefetch buffer */, rep_->footer,
              read_options, handle, &contents, rep_->ioptions,
              false /* decompress */, false /*maybe_compressed*/,
              GetBlockTypeForMetaBlockByName(meta_block_name),
              UncompressionDict::GetEmptyDict(), rep_->persistent_cache_options)
              .ReadBlockContents();
    }
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

bool HnswTable::TEST_BlockInCache(const BlockHandle& handle) const {
  assert(rep_ != nullptr);

  Cache* const cache = rep_->table_options.block_cache.get();
  if (cache == nullptr) {
    return false;
  }

  CacheKey key = GetCacheKey(rep_->base_cache_key, handle);

  Cache::Handle* const cache_handle = cache->Lookup(key.AsSlice());
  if (cache_handle == nullptr) {
    return false;
  }

  cache->Release(cache_handle);

  return true;
}

bool HnswTable::TEST_KeyInCache(const ReadOptions& options, const Slice& key) {
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter(
      NewIndexIterator(options, /*input_iter=*/nullptr,
                       /*get_context=*/nullptr, /*lookup_context=*/nullptr));
  iiter->Seek(key);
  assert(iiter->Valid());

  return TEST_BlockInCache(iiter->value());
}

// REQUIRES: The following fields of rep_ should have already been populated:
//  1. file
//  2. index_handle,
//  3. options
//  4. internal_comparator
//  5. index_type
Status HnswTable::CreateIndexReader(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter, bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  switch (rep_->index_type) {
    case BlockBasedTableOptions::kBinarySearch:
      FALLTHROUGH_INTENDED;
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: {
      return BinarySearchIndexReader::Create(this, ro, prefetch_buffer,
                                             use_cache, prefetch, pin,
                                             lookup_context, index_reader);
    }
    default: {
      std::string error_message =
          "Unrecognized index type: " + std::to_string(rep_->index_type);
      return Status::InvalidArgument(error_message.c_str());
    }
  }
}

uint64_t HnswTable::ApproximateDataOffsetOf(
    const InternalIteratorBase<BlockHandle>& index_iter,
    uint64_t data_size) const {
  assert(index_iter.status().ok());
  if (index_iter.Valid()) {
    BlockHandle handle = index_iter.value();
    return handle.offset();
  } else {
    // The iterator is past the last key in the file.
    return data_size;
  }
}

uint64_t HnswTable::GetApproximateDataSize() {
  // Should be in table properties unless super old version
  if (rep_->table_properties) {
    return rep_->table_properties->data_size;
  }
  // Fall back to rough estimate from footer
  return rep_->footer.metaindex_handle().offset();
}

uint64_t HnswTable::ApproximateOffsetOf(const ReadOptions& read_options,
                                        const Slice& key,
                                        TableReaderCaller caller) {
  uint64_t data_size = GetApproximateDataSize();
  if (UNLIKELY(data_size == 0)) {
    // Hmm. Let's just split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  BlockCacheLookupContext context(caller);
  HnswIndexBlockIter iiter_on_stack;
  ReadOptions ro;
  ro.total_order_seek = true;
  ro.io_activity = read_options.io_activity;
  auto index_iter =
      NewIndexIterator(ro,
                       /*input_iter=*/&iiter_on_stack, /*get_context=*/nullptr,
                       /*lookup_context=*/&context);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (index_iter != &iiter_on_stack) {
    iiter_unique_ptr.reset(index_iter);
  }

  index_iter->Seek(key);
  uint64_t offset;
  if (index_iter->status().ok()) {
    offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  // Pro-rate file metadata (incl filters) size-proportionally across data
  // blocks.
  double size_ratio =
      static_cast<double>(offset) / static_cast<double>(data_size);
  return static_cast<uint64_t>(size_ratio *
                               static_cast<double>(rep_->file_size));
}

uint64_t HnswTable::ApproximateSize(const ReadOptions& read_options,
                                    const Slice& start, const Slice& end,
                                    TableReaderCaller caller) {
  assert(rep_->internal_comparator.Compare(start, end) <= 0);

  uint64_t data_size = GetApproximateDataSize();
  if (UNLIKELY(data_size == 0)) {
    // Hmm. Assume whole file is involved, since we have lower and upper
    // bound. This likely skews the estimate if we consider that this function
    // is typically called with `[start, end]` fully contained in the file's
    // key-range.
    return rep_->file_size;
  }

  BlockCacheLookupContext context(caller);
  HnswIndexBlockIter iiter_on_stack;
  ReadOptions ro;
  ro.total_order_seek = true;
  ro.io_activity = read_options.io_activity;
  auto index_iter =
      NewIndexIterator(ro,
                       /*input_iter=*/&iiter_on_stack, /*get_context=*/nullptr,
                       /*lookup_context=*/&context);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (index_iter != &iiter_on_stack) {
    iiter_unique_ptr.reset(index_iter);
  }

  index_iter->Seek(start);
  uint64_t start_offset;
  if (index_iter->status().ok()) {
    start_offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Assume file is involved from the start. This likely skews the estimate
    // but is consistent with the above error handling.
    start_offset = 0;
  }

  index_iter->Seek(end);
  uint64_t end_offset;
  if (index_iter->status().ok()) {
    end_offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Assume file is involved until the end. This likely skews the estimate
    // but is consistent with the above error handling.
    end_offset = data_size;
  }

  assert(end_offset >= start_offset);
  // Pro-rate file metadata (incl filters) size-proportionally across data
  // blocks.
  double size_ratio = static_cast<double>(end_offset - start_offset) /
                      static_cast<double>(data_size);
  return static_cast<uint64_t>(size_ratio *
                               static_cast<double>(rep_->file_size));
}

bool HnswTable::TEST_IndexBlockInCache() const {
  assert(rep_ != nullptr);

  return TEST_BlockInCache(rep_->footer.index_handle());
}

bool HnswTable::TEST_HnswBlockInCache() const {
  assert(rep_ != nullptr);

  return TEST_BlockInCache(rep_->footer.index_handle());
}

Status HnswTable::DumpTable(WritableFile* out_file) {
  WritableFileStringStreamAdapter out_file_wrapper(out_file);
  std::ostream out_stream(&out_file_wrapper);
  // Output Footer
  out_stream << "Footer Details:\n"
                "--------------------------------------\n";
  out_stream << "  " << rep_->footer.ToString() << "\n";

  // Output MetaIndex
  out_stream << "Metaindex Details:\n"
                "--------------------------------------\n";
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  // TODO: plumb Env::IOActivity
  const ReadOptions ro;
  Status s = ReadMetaIndexBlock(ro, nullptr /* prefetch_buffer */, &metaindex,
                                &metaindex_iter);
  if (s.ok()) {
    for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
         metaindex_iter->Next()) {
      s = metaindex_iter->status();
      if (!s.ok()) {
        return s;
      }
      if (metaindex_iter->key() == kPropertiesBlockName) {
        out_stream << "  Properties block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
      } else if (metaindex_iter->key() == kCompressionDictBlockName) {
        out_stream << "  Compression dictionary block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
      }
    }
    out_stream << "\n";
  } else {
    return s;
  }

  // Output TableProperties
  const ROCKSDB_NAMESPACE::TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_stream << "Table Properties:\n"
                  "--------------------------------------\n";
    out_stream << "  " << table_properties->ToString("\n  ", ": ") << "\n";
  }

  // Output Index block
  s = DumpIndexBlock(out_stream);
  if (!s.ok()) {
    return s;
  }

  // Output compression dictionary
  if (rep_->uncompression_dict_reader) {
    CachableEntry<UncompressionDict> uncompression_dict;
    s = rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr /* prefetch_buffer */, ro, false /* no_io */,
        false, /* verify_checksums */
        nullptr /* get_context */, nullptr /* lookup_context */,
        &uncompression_dict);
    if (!s.ok()) {
      return s;
    }

    assert(uncompression_dict.GetValue());

    const Slice& raw_dict = uncompression_dict.GetValue()->GetRawDict();
    out_stream << "Compression Dictionary:\n"
                  "--------------------------------------\n";
    out_stream << "  size (bytes): " << raw_dict.size() << "\n\n";
    out_stream << "  HEX    " << raw_dict.ToString(true) << "\n\n";
  }

  // Output Hnsw block
  s = DumpHnswBlock(out_stream);

  if (!s.ok()) {
    return s;
  }

  // Output Data blocks
  s = DumpDataBlocks(out_stream);

  if (!s.ok()) {
    return s;
  }

  if (!out_stream.good()) {
    return Status::IOError("Failed to write to output file");
  }
  return Status::OK();
}

Status HnswTable::DumpIndexBlock(std::ostream& out_stream) {
  out_stream << "Index Details:\n"
                "--------------------------------------\n";
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  std::unique_ptr<InternalIteratorBase<BlockHandle>> blockhandles_iter(
      NewIndexIterator(read_options,
                       /*input_iter=*/nullptr, /*get_context=*/nullptr,
                       /*lookup_contex=*/nullptr));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_stream << "Can not read Index Block \n\n";
    return s;
  }

  out_stream << "  Block key hex dump: Data block handle\n";
  out_stream << "  Block key ascii\n\n";
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    Slice user_key;
    InternalKey ikey;
    user_key = key;

    out_stream << "  HEX    " << user_key.ToString(true)
               << ": "
               //               << blockhandles_iter->value()
               << " offset " << blockhandles_iter->value().offset() << " size "
               << blockhandles_iter->value().size() << "\n";

    std::string str_key = user_key.ToString();
    std::string res_key("");
    char c_space = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, c_space);
    }
    out_stream << "  ASCII  " << res_key << "\n";
    out_stream << "  ------\n";
  }
  out_stream << "\n";
  return Status::OK();
}

Status HnswTable::DumpDataBlocks(std::ostream& out_stream) {
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  std::unique_ptr<InternalIteratorBase<BlockHandle>> blockhandles_iter(
      NewIndexIterator(read_options,
                       /*input_iter=*/nullptr, /*get_context=*/nullptr,
                       /*lookup_contex=*/nullptr));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_stream << "Can not read Index Block \n\n";
    return s;
  }

  uint64_t datablock_size_min = std::numeric_limits<uint64_t>::max();
  uint64_t datablock_size_max = 0;
  uint64_t datablock_size_sum = 0;

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    BlockHandle bh = blockhandles_iter->value();
    uint64_t datablock_size = bh.size();
    datablock_size_min = std::min(datablock_size_min, datablock_size);
    datablock_size_max = std::max(datablock_size_max, datablock_size);
    datablock_size_sum += datablock_size;

    out_stream << "Data Block # " << block_id << " @ "
               << blockhandles_iter->value().ToString(true) << "\n";
    out_stream << "--------------------------------------\n";

    std::unique_ptr<GorillaCompressedDataBlockIter> datablock_iter;
    Status tmp_status;
    datablock_iter.reset(NewDataBlockIterator<GorillaCompressedDataBlockIter>(
        read_options, blockhandles_iter->value(),
        /*input_iter=*/nullptr, /*type=*/BlockType::kData,
        /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
        /*async_read=*/false, tmp_status));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_stream << "Error reading the block - Skipped \n\n";
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_stream << "Error reading the block - Skipped \n";
        break;
      }
      DumpKeyValue(datablock_iter->key(), datablock_iter->ReadableValueString(),
                   out_stream);
    }
    out_stream << "\n";
  }

  uint64_t num_datablocks = block_id - 1;
  if (num_datablocks) {
    double datablock_size_avg =
        static_cast<double>(datablock_size_sum) / num_datablocks;
    out_stream << "Data Block Summary:\n";
    out_stream << "--------------------------------------\n";
    out_stream << "  # data blocks: " << num_datablocks << "\n";
    out_stream << "  min data block size: " << datablock_size_min << "\n";
    out_stream << "  max data block size: " << datablock_size_max << "\n";
    out_stream << "  avg data block size: "
               << std::to_string(datablock_size_avg) << "\n";
  }

  return Status::OK();
}

void HnswTable::DumpKeyValue(const Slice& key, const Slice& value,
                             std::ostream& out_stream) {
  InternalKey ikey;
  ikey.DecodeFrom(key);

  out_stream << "  HEX    " << ikey.user_key().ToString(true) << ": "
             << value.ToString(true) << "\n";

  std::string str_key = ikey.user_key().ToString();
  std::string str_value = value.ToString();
  std::string res_key, res_value;
  char c_space = ' ';
  for (char& i : str_key) {
    if (i == '\0') {
      res_key.append("\\0", 2);
    } else {
      res_key.append(&i, 1);
    }
    res_key.append(1, c_space);
  }
  for (char& i : str_value) {
    if (i == '\0') {
      res_value.append("\\0", 2);
    } else {
      res_value.append(&i, 1);
    }
    res_value.append(1, c_space);
  }

  out_stream << "  ASCII  " << res_key << ": " << res_value << "\n";
  out_stream << "  ------\n";
}

Status HnswTable::DumpHnswBlock(std::ostream& out_stream) {
  out_stream << "HnswBlock\n";
  out_stream << "Dump HnswBlock contents to be implemented.\n";
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE