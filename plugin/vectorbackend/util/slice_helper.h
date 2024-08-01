//
// Created by shb on 23-8-26.
//

#pragma once

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
inline unsigned long getIdFromInternalKey(const Slice& internal_key) {
  return *reinterpret_cast<const unsigned long*>(internal_key.data());
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE