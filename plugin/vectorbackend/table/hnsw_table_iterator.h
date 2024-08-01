//
// Created by shb on 23-8-31.
//

#pragma once

#include "comparator.h"
#include "hnsw_block.h"
#include "hnsw_block_prefetcher.h"
#include "table/block_based/block_based_table_iterator.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class HnswTable;
// Iterates over the contents of HnswTable.
class HnswTableIterator : public InternalIteratorBase<Slice> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
  // @param read_options Must outlive this iterator.
 public:
  HnswTableIterator(const HnswTable* table, const ReadOptions& read_options,
                    std::unique_ptr<HnswIndexBlockIter>&& index_iter,
                    TableReaderCaller caller,
                    size_t compaction_readahead_size = 0,
                    bool allow_unprepared_value = false);

  ~HnswTableIterator() override = default;

  void Seek(const Slice& target) override {
    Seek(DecodeFixed32(target.data()));
  }
  void Seek(uint32_t target_internal_id,
            Cache::Priority priority = Cache::Priority::LOW) {
    SeekImpl(target_internal_id, true, priority);
  }
  void SeekForPrev(const Slice& target) override {
    SeekForPrev(DecodeFixed32(target.data()));
  }
  void SeekForPrev(uint32_t target_internal_id);
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;
  bool Valid() const override {
    return !is_out_of_bound_ &&
           (is_at_first_key_from_index_ ||
            (block_iter_points_to_real_block_ && block_iter_.Valid()));
  }
  Slice key() const override;
  Slice user_key() const override;
  uint32_t current_internal_id() const;
  bool PrepareValue() override {
    assert(Valid());

    if (!is_at_first_key_from_index_) {
      return true;
    }

    return const_cast<HnswTableIterator*>(this)->MaterializeCurrentBlock();
  }
  Slice value() const override;

  Entry* EntryValue() const;
  Status status() const override;

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (is_out_of_bound_) {
      return IterBoundCheck::kOutOfBound;
    } else if (block_upper_bound_check_ ==
               BlockUpperBound::kUpperBoundBeyondCurBlock) {
      assert(!is_out_of_bound_);
      return IterBoundCheck::kInbound;
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override;
  bool IsValuePinned() const override {
    assert(!is_at_first_key_from_index_);
    assert(Valid());

    // BlockIter::IsValuePinned() is always true. No need to check
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           block_iter_points_to_real_block_;
  }

  void ResetDataIter() {
    if (block_iter_points_to_real_block_) {
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        block_iter_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
      block_iter_.Invalidate(Status::OK());
      block_iter_points_to_real_block_ = false;
    }
    block_upper_bound_check_ = BlockUpperBound::kUnknown;
  }

  void SavePrevIndexValue();

  void GetReadaheadState(ReadaheadFileInfo* readahead_file_info) override;

  void SetReadaheadState(ReadaheadFileInfo* readahead_file_info) override;

  std::unique_ptr<HnswIndexBlockIter> index_iter_;

 private:
  // This enum indicates whether the upper bound falls into current block
  // or beyond.
  //   +-------------+
  //   |  cur block  |       <-- (1)
  //   +-------------+
  //                         <-- (2)
  //  --- <boundary key> ---
  //                         <-- (3)
  //   +-------------+
  //   |  next block |       <-- (4)
  //        ......
  //
  // When the block is smaller than <boundary key>, kUpperBoundInCurBlock
  // is the value to use. The examples are (1) or (2) in the graph. It means
  // all keys in the next block or beyond will be out of bound. Keys within
  // the current block may or may not be out of bound.
  // When the block is larger or equal to <boundary key>,
  // kUpperBoundBeyondCurBlock is to be used. The examples are (3) and (4)
  // in the graph. It means that all keys in the current block is within the
  // upper bound and keys in the next block may or may not be within the uppder
  // bound.
  // If the boundary key hasn't been checked against the upper bound,
  // kUnknown can be used.
  enum class BlockUpperBound : uint8_t {
    kUpperBoundInCurBlock,
    kUpperBoundBeyondCurBlock,
    kUnknown,
  };

  // State bits for collecting stats on seeks and whether they returned useful
  // results.
  enum SeekStatState : uint8_t {
    kNone = 0,
    // Already recorded that a data block was accessed since the last seek.
    kDataBlockReadSinceLastSeek = 1 << 1,
    // Have not yet recorded that a value() was accessed.
    kReportOnUseful = 1 << 2,
  };

  const HnswTable* table_;
  const ReadOptions read_options_;
  UserComparatorWrapper user_comparator_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  GorillaCompressedDataBlockIter block_iter_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;

  HnswBlockPrefetcher block_prefetcher_;

  const bool allow_unprepared_value_;
  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  // See InternalIteratorBase::IsOutOfBound().
  bool is_out_of_bound_ = false;
  // How current data block's boundary key with the next block is compared with
  // iterate upper bound.
  BlockUpperBound block_upper_bound_check_ = BlockUpperBound::kUnknown;
  // True if we're standing at the first key of a block, and we haven't loaded
  // that block yet. A call to PrepareValue() will trigger loading the block.
  bool is_at_first_key_from_index_ = false;

  bool async_read_in_progress_;

  mutable SeekStatState seek_stat_state_ = SeekStatState::kNone;
  bool is_last_level_;

  // If `target` is null, seek to first.
  void SeekImpl(const uint32_t target_internal_id, bool async_prefetch,
                Cache::Priority priority = Cache::Priority::LOW);

  void InitDataBlock();
  void AsyncInitDataBlock(bool is_first_pass);
  bool MaterializeCurrentBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();
  void CheckOutOfBound();

  // Check if data block is fully within iterate_upper_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update data_block_within_upper_bound_ accordingly.
  void CheckDataBlockWithinUpperBound();
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
