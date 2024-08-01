//
// Created by shb on 23-8-15.
//

#pragma once

#include "options/options_helper.h"
#include "rocksdb/options.h"
#include "vcf_options.h"
#include "vector_options.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
// Checks that the combination of DBOptions and VectorColumnFamilyOptions are
// valid
Status ValidateOptions(const DBOptions& db_opt,
                       const VectorColumnFamilyOptions& vcf_opts);

VectorColumnFamilyOptions BuildVectorColumnFamilyOptions(
    const VectorColumnFamilyOptions& ioptions,
    const MutableVectorCFOptions& mutable_vcf_options);

void UpdateVectorColumnFamilyOptions(const MutableVectorCFOptions& moptions,
                                     VectorColumnFamilyOptions* cf_opts);
void UpdateVectorColumnFamilyOptions(const ImmutableVectorCFOptions& ioptions,
                                     VectorColumnFamilyOptions* cf_opts);

std::unique_ptr<Configurable> VectorCFOptionsAsConfigurable(
    const MutableVectorCFOptions& opts);

std::unique_ptr<Configurable> VectorCFOptionsAsConfigurable(
    const VectorColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map = nullptr);

struct OptionsHelper : public ROCKSDB_NAMESPACE::OptionsHelper {
  static const std::string
      kVectorCFOptionsName /*= "VectorColumnFamilyOptions" */;
};

Status GetVectorColumnFamilyOptionsFromMap(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    VectorColumnFamilyOptions* new_options);

// Take a ConfigOptions `config_options`, a string representation of option
// names and values, apply them into the base_options, and return the new
// options as a result. The string has the following format:
//   "write_buffer_size=1024;max_write_buffer_number=2"
// Nested options config is also possible. For example, you can define
// BlockBasedTableOptions as part of the string for block-based table factory:
//   "write_buffer_size=1024;block_based_table_factory={block_size=4k};"
//   "max_write_buffer_num=2"
//
Status GetVectorColumnFamilyOptionsFromString(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& base_options, const std::string& opts_str,
    VectorColumnFamilyOptions* new_options);
}  //  namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE