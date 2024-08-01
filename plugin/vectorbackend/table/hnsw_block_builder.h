// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//
// Created by shb on 2023/8/10.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "table/block_based/block_builder.h"

namespace compression {
class GorillaVectorCompressor;
}

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class HnswBlockBuilder : public BlockBuilder {
 public:
  HnswBlockBuilder(const HnswBlockBuilder&) = delete;
  void operator=(const HnswBlockBuilder&) = delete;
  ~HnswBlockBuilder() override { delete compressor; }

  HnswBlockBuilder(int block_restart_interval, size_t dim, size_t M);

  void Reset() override;

  void SwapAndReset(std::string& buffer) override;

  void Add(const Slice& key, const Slice& value);

  Slice Finish() override;
  inline size_t CurrentSizeEstimate() const override { return estimate_; }
  inline size_t EstimateSizeAfterKV(const Slice& key,
                                    const Slice& value) const override;

 private:
  ::compression::GorillaVectorCompressor* compressor;
  uint32_t offsetData_;
};

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
