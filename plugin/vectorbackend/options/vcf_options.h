//
// Created by shb on 2023/8/11.
//

#pragma once

#include "vector_options.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
struct ImmutableVectorCFOptions {
 public:
  static const char* kName() { return "ImmutableVectorCFOptions"; }
  ImmutableVectorCFOptions()
      : ImmutableVectorCFOptions(VectorColumnFamilyOptions()) {}

  explicit ImmutableVectorCFOptions(const VectorColumnFamilyOptions& opts)
      : dim(opts.dim),
        space(opts.space),
        allow_replace_deleted(opts.allow_replace_deleted) {}

  size_t dim;
  hnswlib::SpaceType space;
  bool allow_replace_deleted = true;
};

struct ImmutableVectorOptions : public ImmutableDBOptions,
                                public ImmutableVectorCFOptions {
  explicit ImmutableVectorOptions() : ImmutableVectorOptions(VectorOptions()) {}
  explicit ImmutableVectorOptions(const Options& options)
      : ImmutableVectorOptions(options, options) {}
  explicit ImmutableVectorOptions(const ImmutableOptions& options)
      : ImmutableDBOptions(options) {}
  ImmutableVectorOptions(const VectorOptions& options)
      : ImmutableVectorOptions(
            reinterpret_cast<const DBOptions&>(options),
            reinterpret_cast<const VectorColumnFamilyOptions&>(options)) {}
  ImmutableVectorOptions(const DBOptions& db_options,
                         const VectorColumnFamilyOptions& cf_options)
      : ImmutableDBOptions(db_options), ImmutableVectorCFOptions(cf_options) {}
  ImmutableVectorOptions(const DBOptions& db_options,
                         const ImmutableVectorCFOptions& cf_options)
      : ImmutableDBOptions(db_options), ImmutableVectorCFOptions(cf_options) {}
  ImmutableVectorOptions(const ImmutableDBOptions& db_options,
                         const VectorColumnFamilyOptions& cf_options)
      : ImmutableDBOptions(db_options), ImmutableVectorCFOptions(cf_options) {}
  ImmutableVectorOptions(const ImmutableDBOptions& db_options,
                         const ImmutableVectorCFOptions& cf_options)
      : ImmutableDBOptions(db_options), ImmutableVectorCFOptions(cf_options) {}
};

struct MutableVectorCFOptions {
 public:
  static const char* kName() { return "MutableVectorCFOptions"; }
  MutableVectorCFOptions()
      : MutableVectorCFOptions(VectorColumnFamilyOptions()) {}

  MutableVectorCFOptions(const VectorColumnFamilyOptions& opts)
      : max_elements(opts.max_elements),
        M(opts.M),
        ef_construction(opts.ef_construction),
        random_seed(opts.random_seed),
        visit_list_pool_size(opts.visit_list_pool_size),
        termination_threshold(opts.termination_threshold),
        termination_weight(opts.termination_weight),
        termination_lower_bound(opts.termination_lower_bound) {}

  MutableVectorCFOptions(const MutableVectorCFOptions& opts)
      : max_elements(opts.max_elements),
        M(opts.M),
        ef_construction(opts.ef_construction),
        random_seed(opts.random_seed),
        visit_list_pool_size(opts.visit_list_pool_size),
        termination_threshold(opts.termination_threshold.load()),
        termination_weight(opts.termination_weight),
        termination_lower_bound(opts.termination_lower_bound) {}

  ~MutableVectorCFOptions() = default;

  MutableVectorCFOptions& operator=(const MutableVectorCFOptions& options) {
    if (this != const_cast<MutableVectorCFOptions*>(&options)) {
      this->max_elements = options.max_elements;
      this->M = options.M;
      this->ef_construction = options.ef_construction;
      this->random_seed = options.random_seed;
      this->visit_list_pool_size = options.visit_list_pool_size;
      this->termination_threshold.store(
          options.termination_threshold.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      this->termination_weight = options.termination_weight;
      this->termination_lower_bound = options.termination_lower_bound;
    }
    return *this;
  }

  size_t max_elements = 100000;
  size_t M = 16;
  size_t ef_construction = 200;
  size_t random_seed = 100;
  size_t visit_list_pool_size = 1;
  std::atomic<float> termination_threshold = 114.514f;
  float termination_weight = 0.1f;
  float termination_lower_bound = 0.3f;
};

Status GetStringFromVectorColumnFamilyOptions(
    std::string* opts_str, const VectorColumnFamilyOptions& vcf_options,
    const std::string& delimiter = ";  ");

Status GetStringFromVectorColumnFamilyOptions(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& vcf_options, std::string* opt_string);

Status GetStringFromMutableVectorCFOptions(
    const ConfigOptions& config_options,
    const MutableVectorCFOptions& mutable_opts, std::string* opt_string);

Status GetMutableVectorOptionsFromStrings(
    const MutableVectorCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableVectorCFOptions* new_options);

}  //  namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE