#pragma once

#include <vector>

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class FloatVector {
 private:
  int id_;
  std::vector<float> value_;

 public:
  FloatVector(int id, std::vector<float> &&value)
      : id_(id), value_(std::move(value)) {}

  int id() const { return id_; }

  const std::vector<float> &value() const { return value_; }
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE