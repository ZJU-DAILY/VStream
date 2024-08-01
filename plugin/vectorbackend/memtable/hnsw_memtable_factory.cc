// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/4.
//

#include "hnsw_memtable_factory.h"

#include "../vectorindex/hnswlib/hnswlib.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
HnswOptions::HnswOptions() { space = hnswlib::SpaceType::L2; }

static std::unordered_map<std::string, OptionTypeInfo> hnsw_type_info = {
    {"dim",
        {offsetof(struct HnswOptions, dim), OptionType::kSizeT,
         OptionVerificationType::kNormal, OptionTypeFlags::kNone}
    },
    {"space",
     {offsetof(struct HnswOptions, space), OptionType::kUInt8T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"max_elements",
     {offsetof(struct HnswOptions, max_elements), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"M",
     {offsetof(struct HnswOptions, M), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"ef_construction",
     {offsetof(struct HnswOptions, ef_construction), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"random_seed",
     {offsetof(struct HnswOptions, random_seed), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"visit_list_pool_size",
     {offsetof(struct HnswOptions, visit_list_pool_size), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"allow_replace_deleted",
     {offsetof(struct HnswOptions, allow_replace_deleted), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

HnswMemTableFactory::HnswMemTableFactory(const HnswOptions& hnswOptions)
    : hnswOptions_(hnswOptions) {
  RegisterOptions(&hnswOptions_, &hnsw_type_info);
}

MemTableRepFactory* NewHnswMemTableFactory(const HnswOptions& options) {
  return new HnswMemTableFactory(options);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE