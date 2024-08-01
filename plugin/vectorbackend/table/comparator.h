//
// Created by shb on 23-9-8.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
template <typename T>
int compare(T a, T b) {
  return a < b ? -1 : a > b ? 1 : 0;
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE