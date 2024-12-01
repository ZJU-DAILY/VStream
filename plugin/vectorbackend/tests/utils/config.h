#pragma once

#include <memory>

#include "float_vector_iterator.h"
#include "plugin/vectorbackend/memtable/hnsw_memtable_factory.h"
#include "plugin/vectorbackend/options/vector_options.h"
#include "plugin/vectorbackend/table/hnsw_table_factory.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswlib.h"
#include "toml.hpp"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class Config {
 public:
  static Config* parse(const std::string& config_file) {
    auto* config = new Config();
    auto conf = toml::parse_file(config_file);
    auto vcf_opts = VectorColumnFamilyOptions();
    auto vs_opts = VectorSearchOptions();

    // Parse VectorColumnFamilyOptions
    vcf_opts.dim = conf["dim"].value_or(128);
    vcf_opts.max_elements = conf["max_elements"].value_or(100000);
    vcf_opts.M = conf["M"].value_or(16);
    vcf_opts.ef_construction = conf["ef_construction"].value_or(200);
    vcf_opts.random_seed = conf["random_seed"].value_or(100);
    vcf_opts.visit_list_pool_size = conf["visit_list_pool_size"].value_or(1);
    vcf_opts.allow_replace_deleted =
        conf["allow_replace_deleted"].value_or(true);
    vcf_opts.termination_threshold =
        conf["termination_threshold"].value_or(0.0f);
    vcf_opts.termination_weight = conf["termination_weight"].value_or(0.0f);
    vcf_opts.termination_lower_bound =
        conf["termination_lower_bound"].value_or(0.3f);
    std::string_view space = conf["space"].value_or("L2");
    if (space == "L2") {
      vcf_opts.space = hnswlib::SpaceType::L2;
    } else if (space == "IP") {
      vcf_opts.space = hnswlib::SpaceType::IP;
    } else {
      throw std::runtime_error("Invalid space type");
    }

    HnswOptions hnsw_opts;
    hnsw_opts.dim = vcf_opts.dim;
    hnsw_opts.space = vcf_opts.space;
    hnsw_opts.max_elements = vcf_opts.max_elements;
    hnsw_opts.M = vcf_opts.M;
    hnsw_opts.ef_construction = vcf_opts.ef_construction;
    hnsw_opts.random_seed = vcf_opts.random_seed;
    hnsw_opts.visit_list_pool_size = vcf_opts.visit_list_pool_size;
    hnsw_opts.allow_replace_deleted = vcf_opts.allow_replace_deleted;
    vcf_opts.memtable_factory =
        std::make_shared<HnswMemTableFactory>(hnsw_opts);

    HnswTableOptions hnsw_table_opts{hnsw_opts};
    hnsw_table_opts.block_size = conf["block_size"].value_or(4096);
    vcf_opts.check_flush_compaction_key_order = false;
    vcf_opts.table_factory =
        std::make_shared<HnswTableFactory>(hnsw_table_opts);
    vcf_opts.flush_threshold = conf["flush_threshold"].value_or(2);
    vcf_opts.max_write_buffer_number = vcf_opts.flush_threshold * 2 + 1;

    config->vcf_opts_ = vcf_opts;

    // Parse VectorSearchOptions
    vs_opts.k = conf["k"].value_or(10);
    vs_opts.termination_factor = conf["termination_factor"].value_or(1.0f);

    config->vs_opts_ = vs_opts;

    // Parse flush interval
    config->flush_interval_ = conf["flush_interval"].value_or(1000);

    // Parse vector file
    config->base_file_ = conf["base_file"].value_or("");

    // Parse query file
    config->query_file_ = conf["query_file"].value_or("");

    return config;
  }

  [[nodiscard]] const VectorColumnFamilyOptions& vcf_opts() const {
    return vcf_opts_;
  }

  [[nodiscard]] const VectorSearchOptions& vs_opts() const { return vs_opts_; }

  [[nodiscard]] size_t flush_interval() const { return flush_interval_; }

  std::string base_file() const { return base_file_; }

  std::string query_file() const { return query_file_; }

  [[nodiscard]] FloatVectorIterator GetIterator(std::string file_path) {
    return FloatVectorIterator::fromFile(file_path);
  }

 private:
  Config() = default;
  VectorColumnFamilyOptions vcf_opts_;
  VectorSearchOptions vs_opts_;
  size_t flush_interval_{std::numeric_limits<size_t>::max()};
  std::string base_file_;
  std::string query_file_;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE