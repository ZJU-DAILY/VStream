// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/10.
//

#include "hnsw_block_builder.h"

#include "../compression/compression.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswalg.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
HnswBlockBuilder::HnswBlockBuilder(int block_restart_interval, size_t dim,
                                   size_t M)
    : BlockBuilder(block_restart_interval) {
  assert(block_restart_interval_ >= 1);
  restarts_.clear();
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  offsetData_ = M * 2 * sizeof(hnswlib::tableint) +
                sizeof(hnswlib::linklistsizeint) + sizeof(hnswlib::labeltype) +
                sizeof(SequenceNumber) + sizeof(uint64_t);
  compressor = new ::compression::GorillaVectorCompressor(dim);
}

void HnswBlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  counter_ = 0;
#ifndef NDEBUG
  add_with_last_key_called_ = false;  // add_with_first_value_called_
#endif
}

void HnswBlockBuilder::SwapAndReset(std::string& buffer) {
  std::swap(buffer_, buffer);
  Reset();
}

inline size_t HnswBlockBuilder::EstimateSizeAfterKV(const Slice& key,
                                                    const Slice& value) const {
  size_t estimate = estimate_;
  estimate += key.size();
  estimate += (value.size() >> 1) + (value.size() >> 2);

  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t);
  }

  return estimate;
}

Slice HnswBlockBuilder::Finish() {
  // Append restart array
  for (unsigned int restart : restarts_) {
    PutFixed32(&buffer_, restart);
  }
  auto num_restarts = static_cast<uint32_t>(restarts_.size());

  PutFixed32(&buffer_, num_restarts);
  return {buffer_};
}

void HnswBlockBuilder::Add(const Slice& key, const Slice& value) {
#ifndef NDEBUG
  add_with_last_key_called_ = false;  // add_with_first_value_called_
#endif
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  size_t buffer_size = buffer_.size();
  if (counter_ == 0 || !use_delta_encoding_) {
    restarts_.push_back(static_cast<uint32_t>(buffer_size));
    estimate_ += sizeof(uint32_t);
    buffer_.append(key.data(), sizeof(uint32_t));
    buffer_.append(value.data(), offsetData_);
    compressor->AddFirstVector(
        reinterpret_cast<const float*>(value.data() + offsetData_), buffer_);
  } else {
    buffer_.append(key.data(), sizeof(uint32_t));
    buffer_.append(value.data(), offsetData_);
     if (counter_ == 1) {
       compressor->AddSecondVector(
           reinterpret_cast<const float*>(value.data() + offsetData_), buffer_);
     } else {
      compressor->AddNextVector(
          reinterpret_cast<const float*>(value.data() + offsetData_), buffer_);
     }
  }

  counter_ = (++counter_) % block_restart_interval_;
  estimate_ += buffer_.size() - buffer_size;
}

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
