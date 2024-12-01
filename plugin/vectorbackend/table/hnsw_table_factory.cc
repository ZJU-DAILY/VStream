// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "hnsw_table_factory.h"

#include <utility>

#include "../vectorindex/hnswlib/hnswlib.h"
#include "hnsw_table_builder.h"
#include "hnsw_table_reader.h"
#include "options/options_helper.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

static std::unordered_map<std::string, OptionTypeInfo> hnsw_table_type_info{
    // region block_based_table_type_info
    /* currently not supported
      std::shared_ptr<Cache> block_cache = nullptr;
      CacheUsageOptions cache_usage_options;
     */
    {"flush_block_policy_factory",
     OptionTypeInfo::AsCustomSharedPtr<FlushBlockPolicyFactory>(
         offsetof(struct HnswTableOptions, flush_block_policy_factory),
         OptionVerificationType::kByName, OptionTypeFlags::kCompareNever)},
    {"cache_index_and_filter_blocks",
     {offsetof(struct HnswTableOptions, cache_index_and_filter_blocks),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"cache_index_and_filter_blocks_with_high_priority",
     {offsetof(struct HnswTableOptions,
               cache_index_and_filter_blocks_with_high_priority),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"pin_l0_filter_and_index_blocks_in_cache",
     {offsetof(struct HnswTableOptions,
               pin_l0_filter_and_index_blocks_in_cache),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"index_type", OptionTypeInfo::Enum<HnswTableOptions::IndexType>(
                       offsetof(struct HnswTableOptions, index_type),
                       &block_base_table_index_type_string_map)},
    {"hash_index_allow_collision",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"data_block_index_type",
     OptionTypeInfo::Enum<HnswTableOptions::DataBlockIndexType>(
         offsetof(struct HnswTableOptions, data_block_index_type),
         &block_base_table_data_block_index_type_string_map)},
    {"index_shortening",
     OptionTypeInfo::Enum<HnswTableOptions::IndexShorteningMode>(
         offsetof(struct HnswTableOptions, index_shortening),
         &block_base_table_index_shortening_mode_string_map)},
    {"data_block_hash_table_util_ratio",
     {offsetof(struct HnswTableOptions, data_block_hash_table_util_ratio),
      OptionType::kDouble, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"checksum",
     {offsetof(struct HnswTableOptions, checksum), OptionType::kChecksumType,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"no_block_cache",
     {offsetof(struct HnswTableOptions, no_block_cache), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"block_size",
     {offsetof(struct HnswTableOptions, block_size), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
    {"block_size_deviation",
     {offsetof(struct HnswTableOptions, block_size_deviation), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"block_restart_interval",
     {offsetof(struct HnswTableOptions, block_restart_interval),
      OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable}},
    {"index_block_restart_interval",
     {offsetof(struct HnswTableOptions, index_block_restart_interval),
      OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"index_per_partition",
     {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"metadata_block_size",
     {offsetof(struct HnswTableOptions, metadata_block_size),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"partition_filters",
     {offsetof(struct HnswTableOptions, partition_filters),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"optimize_filters_for_memory",
     {offsetof(struct HnswTableOptions, optimize_filters_for_memory),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"filter_policy",
     OptionTypeInfo::AsCustomSharedPtr<const FilterPolicy>(
         offsetof(struct HnswTableOptions, filter_policy),
         OptionVerificationType::kByNameAllowFromNull, OptionTypeFlags::kNone)},
    {"whole_key_filtering",
     {offsetof(struct HnswTableOptions, whole_key_filtering),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"detect_filter_construct_corruption",
     {offsetof(struct HnswTableOptions, detect_filter_construct_corruption),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable}},
    {"reserve_table_builder_memory",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"reserve_table_reader_memory",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"skip_table_builder_flush",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"format_version",
     {offsetof(struct HnswTableOptions, format_version), OptionType::kUInt32T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"verify_compression",
     {offsetof(struct HnswTableOptions, verify_compression),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"read_amp_bytes_per_bit",
     {offsetof(struct HnswTableOptions, read_amp_bytes_per_bit),
      OptionType::kUInt32T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
         const std::string& value, void* addr) {
        // A workaround to fix a bug in 6.10, 6.11, 6.12, 6.13
        // and 6.14. The bug will write out 8 bytes to OPTIONS file from the
        // starting address of HnswTableOptions.read_amp_bytes_per_bit
        // which is actually a uint32. Consequently, the value of
        // read_amp_bytes_per_bit written in the OPTIONS file is wrong.
        // From 6.15, RocksDB will try to parse the read_amp_bytes_per_bit
        // from OPTIONS file as a uint32. To be able to load OPTIONS file
        // generated by affected releases before the fix, we need to
        // manually parse read_amp_bytes_per_bit with this special hack.
        uint64_t read_amp_bytes_per_bit = ParseUint64(value);
        *(static_cast<uint32_t*>(addr)) =
            static_cast<uint32_t>(read_amp_bytes_per_bit);
        return Status::OK();
      }}},
    {"enable_index_compression",
     {offsetof(struct HnswTableOptions, enable_index_compression),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"block_align",
     {offsetof(struct HnswTableOptions, block_align), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"pin_top_level_index_and_filter",
     {offsetof(struct HnswTableOptions, pin_top_level_index_and_filter),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {kOptNameMetadataCacheOpts,
     OptionTypeInfo::Struct(
         kOptNameMetadataCacheOpts, &metadata_cache_options_type_info,
         offsetof(struct HnswTableOptions, metadata_cache_options),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"block_cache",
     {offsetof(struct HnswTableOptions, block_cache), OptionType::kUnknown,
      OptionVerificationType::kNormal,
      (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize),
      // Parses the input value as a Cache
      [](const ConfigOptions& opts, const std::string&,
         const std::string& value, void* addr) {
        auto* cache = static_cast<std::shared_ptr<Cache>*>(addr);
        return Cache::CreateFromString(opts, value, cache);
      }}},
    {"block_cache_compressed",
     {0, OptionType::kUnknown, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone}},
    {"max_auto_readahead_size",
     {offsetof(struct HnswTableOptions, max_auto_readahead_size),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable}},
    {"prepopulate_block_cache",
     OptionTypeInfo::Enum<HnswTableOptions::PrepopulateBlockCache>(
         offsetof(struct HnswTableOptions, prepopulate_block_cache),
         &block_base_table_prepopulate_block_cache_string_map,
         OptionTypeFlags::kMutable)},
    {"initial_auto_readahead_size",
     {offsetof(struct HnswTableOptions, initial_auto_readahead_size),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable}},
    {"num_file_reads_for_auto_readahead",
     {offsetof(struct HnswTableOptions, num_file_reads_for_auto_readahead),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable}},
    // endregion
    {"dim",
     {offsetof(struct HnswTableOptions, dim), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"space_type",
     {offsetof(struct HnswTableOptions, space), OptionType::kUInt8T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"M",
     {offsetof(struct HnswTableOptions, M), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

Status HnswTableFactory::NewTableReader(
    const ReadOptions& ro, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  return HnswTable::Open(
      ro, table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader_options.block_protection_bytes_per_key,
      table_reader, table_reader_options.tail_size, table_reader_cache_res_mgr_,
      prefetch_index_and_filter_in_cache, table_reader_options.level,
      table_reader_options.immortal, table_reader_options.largest_seqno,
      table_reader_options.force_direct_prefetch, &tail_prefetch_stats_,
      table_reader_options.block_cache_tracer,
      table_reader_options.max_file_size_for_l0_meta_pin,
      table_reader_options.cur_db_session_id, table_reader_options.cur_file_num,
      table_reader_options.unique_id);
}

TableBuilder* HnswTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  return new HnswTableBuilder(table_options_, table_builder_options, file);
}

Status HnswTableFactory::ValidateOptions(
    const rocksdb::DBOptions& db_opts,

    const rocksdb::ColumnFamilyOptions& vcf_opts) const {
  if (table_options_.dim == 0) {
    return Status::InvalidArgument("dim must be positive");
  }
  if (table_options_.space != hnswlib::SpaceType::L2 &&
      table_options_.space != hnswlib::SpaceType::IP) {
    return Status::InvalidArgument("space must be L2 or IP");
  }
  if (table_options_.M == 0) {
    return Status::InvalidArgument("M must be positive");
  }
  if (table_options_.ef_construction == 0) {
    return Status::InvalidArgument("ef_construction must be positive");
  }
  if (table_options_.visit_list_pool_size == 0) {
    return Status::InvalidArgument("visit_list_pool_size must be positive");
  }
  return BlockBasedTableFactory::ValidateOptions(db_opts, vcf_opts);
}

std::string HnswTableFactory::GetPrintableOptions() const {
  std::string ret = BlockBasedTableFactory::GetPrintableOptions();
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  dim: %" ROCKSDB_PRIszt "\n",
           table_options_.dim);
  ret.append(buffer);
  switch (table_options_.space) {
    case hnswlib::SpaceType::L2:
      snprintf(buffer, kBufferSize, "  space: L2\n");
      break;
    case hnswlib::SpaceType::IP:
      snprintf(buffer, kBufferSize, "  space: IP\n");
      break;
    default:
      snprintf(buffer, kBufferSize, "  space: UNKNOWN\n");
      break;
  }
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  M: %" ROCKSDB_PRIszt "\n", table_options_.M);
  ret.append(buffer);
  return ret;
}

void HnswTableFactory::InitializeOptions() {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
  if (table_options_.no_block_cache) {
    table_options_.block_cache.reset();
  } else if (table_options_.block_cache == nullptr) {
    LRUCacheOptions co;
    // 32MB, the recommended minimum size for 64 shards, to reduce contention
    co.capacity = 32 << 20;
    table_options_.block_cache = NewLRUCache(co);
  }
  if (table_options_.block_size_deviation < 0 ||
      table_options_.block_size_deviation > 100) {
    table_options_.block_size_deviation = 0;
  }
  if (table_options_.block_restart_interval < 1) {
    table_options_.block_restart_interval = 1;
  }
  if (table_options_.index_block_restart_interval < 1) {
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      table_options_.index_block_restart_interval != 1) {
    // Currently kHashSearch is incompatible with
    // index_block_restart_interval_ > 1
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.partition_filters &&
      table_options_.index_type !=
          BlockBasedTableOptions::kTwoLevelIndexSearch) {
    // We do not support partitioned filters without partitioning indexes
    table_options_.partition_filters = false;
  }
  auto& options_overrides =
      table_options_.cache_usage_options.options_overrides;
  const auto options = table_options_.cache_usage_options.options;
  for (std::uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    CacheEntryRole role = static_cast<CacheEntryRole>(i);
    auto options_overrides_iter = options_overrides.find(role);
    if (options_overrides_iter == options_overrides.end()) {
      options_overrides.insert({role, options});
    } else if (options_overrides_iter->second.charged ==
               CacheEntryRoleOptions::Decision::kFallback) {
      options_overrides_iter->second.charged = options.charged;
    }
  }
}

HnswTableFactory::HnswTableFactory(HnswTableOptions table_option)
    : table_options_(std::move(table_option)) {
  InitializeOptions();
  RegisterOptions(&table_options_, &hnsw_table_type_info);
}

TableFactory* NewHnswTableFactory(const HnswTableOptions& options) {
  return new HnswTableFactory(options);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE