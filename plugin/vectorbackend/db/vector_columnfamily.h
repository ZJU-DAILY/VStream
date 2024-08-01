//
// Created by shb on 23-11-9.
//

#pragma once

#include <utility>

#include "../options/vcf_options.h"
#include "../options/vector_options.h"
#include "../table/hnsw_table_factory.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
struct VectorCFDescriptor {
  std::string name;
  VectorColumnFamilyOptions options;
  VectorCFDescriptor() = default;
  VectorCFDescriptor(std::string _name, VectorColumnFamilyOptions _options)
      : name(std::move(_name)), options(std::move(_options)) {}
};

struct VectorColumnFamilyInfo {
  const std::string name;
  ImmutableVectorCFOptions immutable_vcf_options;
  MutableVectorCFOptions mutable_vcf_options;

  VectorColumnFamilyInfo(
      std::string _name,
      const ImmutableVectorCFOptions&& _immutable_vcf_options,
      const MutableVectorCFOptions&& _mutable_vcf_options)
      : name(std::move(_name)),
        immutable_vcf_options(_immutable_vcf_options),
        mutable_vcf_options(_mutable_vcf_options) {}
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE