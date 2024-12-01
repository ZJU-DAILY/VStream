//
// Created by shb on 23-8-16.
//

#include "vector_options.h"

#include "vcf_options.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
VectorColumnFamilyOptions::VectorColumnFamilyOptions(
    const ColumnFamilyOptions& cf_opts,
    const ImmutableVectorCFOptions& immutable_opts,
    const MutableVectorCFOptions& mutable_opts)
    : ColumnFamilyOptions(cf_opts),
      dim(immutable_opts.dim),
      space(immutable_opts.space),
      max_elements(mutable_opts.max_elements),
      M(mutable_opts.M),
      ef_construction(mutable_opts.ef_construction),
      random_seed(mutable_opts.random_seed),
      visit_list_pool_size(mutable_opts.visit_list_pool_size),
      allow_replace_deleted(immutable_opts.allow_replace_deleted),
      termination_threshold(mutable_opts.termination_threshold),
      termination_weight(mutable_opts.termination_weight),
      termination_lower_bound(mutable_opts.termination_lower_bound) {}

void VectorColumnFamilyOptions::Dump(Logger* log) const {
  ROCKS_LOG_INFO(log,
                 "                             max_elements: %" ROCKSDB_PRIszt,
                 max_elements);
  ROCKS_LOG_INFO(
      log, "                                        M: %" ROCKSDB_PRIszt, M);
  ROCKS_LOG_INFO(log,
                 "                          ef_construction: %" ROCKSDB_PRIszt,
                 ef_construction);
  ROCKS_LOG_INFO(log,
                 "                              random_seed: %" ROCKSDB_PRIszt,
                 random_seed);
  ROCKS_LOG_INFO(log,
                 "                     visit_list_pool_size: %" ROCKSDB_PRIszt,
                 visit_list_pool_size);
  ROCKS_LOG_INFO(log, "                       termination_weight: %f",
                 termination_weight);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE