//
// Created by shb on 23-8-31.
//

#include "hnsw_table_iterator.h"

#include "hnsw_table_reader.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
HnswTableIterator::HnswTableIterator(
    const HnswTable* table, const ReadOptions& read_options,
    std::unique_ptr<HnswIndexBlockIter>&& index_iter, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value)
    : index_iter_(std::move(index_iter)),
      table_(table),
      read_options_(read_options),
      pinned_iters_mgr_(nullptr),
      lookup_context_(caller),
      block_prefetcher_(
          compaction_readahead_size,
          table_->get_rep()->table_options.initial_auto_readahead_size),
      allow_unprepared_value_(allow_unprepared_value),
      block_iter_points_to_real_block_(false),
      async_read_in_progress_(false),
      is_last_level_(table->IsLastLevel()) {}

Slice HnswTableIterator::key() const {
  assert(Valid());
  if (is_at_first_key_from_index_) {
    return index_iter_->key();
  } else {
    return block_iter_.key();
  }
}

Slice HnswTableIterator::user_key() const {
  assert(Valid());
  if (is_at_first_key_from_index_) {
    return index_iter_->user_key();
  } else {
    return block_iter_.user_key();
  }
}

uint32_t HnswTableIterator::current_internal_id() const {
  assert(Valid());
  if (is_at_first_key_from_index_) {
    return index_iter_->current_internal_id();
  }
  return block_iter_.current_internal_id();
}

Slice HnswTableIterator::value() const {
  // PrepareValue() must have been called.
  assert(!is_at_first_key_from_index_);
  assert(Valid());

  if (seek_stat_state_ & kReportOnUseful) {
    RecordTick(table_->GetStatistics(),
               is_last_level_ ? LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER
                              : NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER);
    seek_stat_state_ = kDataBlockReadSinceLastSeek;
  }

  return {reinterpret_cast<const char*>(block_iter_.value()->vector_),
          block_iter_.size()};
}

Entry* HnswTableIterator::EntryValue() const {
  // PrepareValue() must have been called.
  assert(!is_at_first_key_from_index_);
  // assert(Valid());

  if (seek_stat_state_ & kReportOnUseful) {
    RecordTick(table_->GetStatistics(),
               is_last_level_ ? LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER
                              : NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER);
    seek_stat_state_ = kDataBlockReadSinceLastSeek;
  }

  return block_iter_.value();
}

Status HnswTableIterator::status() const {
  // Prefix index set status to NotFound when the prefix does not exist
  if (!index_iter_->status().ok() && !index_iter_->status().IsNotFound()) {
    return index_iter_->status();
  } else if (block_iter_points_to_real_block_) {
    return block_iter_.status();
  } else if (async_read_in_progress_) {
    return Status::TryAgain();
  } else {
    return Status::OK();
  }
}

bool HnswTableIterator::IsKeyPinned() const {
  // Our key comes either from block_iter_'s current key
  // or index_iter_'s current *value*.
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         ((is_at_first_key_from_index_ && index_iter_->IsValuePinned()) ||
          (block_iter_points_to_real_block_ && block_iter_.IsKeyPinned()));
}

void HnswTableIterator::SavePrevIndexValue() {
  if (block_iter_points_to_real_block_) {
    // Reseek. If they end up with the same data block, we shouldn't re-fetch
    // the same data block.
    prev_block_offset_ = index_iter_->value().offset();
  }
}

void HnswTableIterator::GetReadaheadState(
    ReadaheadFileInfo* readahead_file_info) {
  if (block_prefetcher_.prefetch_buffer() != nullptr &&
      read_options_.adaptive_readahead) {
    block_prefetcher_.prefetch_buffer()->GetReadaheadState(
        &(readahead_file_info->data_block_readahead_info));
    if (index_iter_) {
      index_iter_->GetReadaheadState(readahead_file_info);
    }
  }
}

void HnswTableIterator::SetReadaheadState(
    ReadaheadFileInfo* readahead_file_info) {
  if (read_options_.adaptive_readahead) {
    block_prefetcher_.SetReadaheadState(
        &(readahead_file_info->data_block_readahead_info));
    if (index_iter_) {
      index_iter_->SetReadaheadState(readahead_file_info);
    }
  }
}

void HnswTableIterator::SeekToFirst() { SeekImpl(0, false); }

void HnswTableIterator::SeekImpl(const uint32_t target, bool async_prefetch,
                                 Cache::Priority priority) {
  bool is_first_pass = true;
  if (async_read_in_progress_) {
    AsyncInitDataBlock(false);
    is_first_pass = false;
  }

  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;

  bool need_seek_index = true;
  if (block_iter_points_to_real_block_ && block_iter_.Valid()) {
    // Reseek.
    prev_block_offset_ = index_iter_->value().offset();

    // We can avoid an index seek if:
    // 1. The new seek key is larger than the current key
    // 2. The new seek key is within the upper bound of the block
    // Since we don't necessarily know the internal key for either
    // the current key or the upper bound, we check user keys and
    // exclude the equality case. Considering internal keys can
    // improve for the boundary cases, but it would complicate the
    // code.
    if (target > block_iter_.current_internal_id() &&
        target < index_iter_->current_internal_id()) {
      need_seek_index = false;
    }
  }

  if (need_seek_index) {
    index_iter_->Seek(target);

    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  BlockHandle handle = index_iter_->value();
  const bool same_block =
      block_iter_points_to_real_block_ && handle.offset() == prev_block_offset_;

  if (!same_block &&
      (!target || target <= index_iter_->current_internal_id()) &&
      allow_unprepared_value_) {
    // Index contains the first key of the block, and it's >= target.
    // We can defer reading the block.
    is_at_first_key_from_index_ = true;
    // ResetDataIter() will invalidate block_iter_. Thus, there is no need to
    // call CheckDataBlockWithinUpperBound() to check for iterate_upper_bound
    // as that will be done later when the data block is actually read.
    ResetDataIter();
  } else {
    // Need to use the data block.
    if (!same_block) {
      if (read_options_.async_io && async_prefetch) {
        if (is_first_pass) {
          AsyncInitDataBlock(is_first_pass);
        }
        if (async_read_in_progress_) {
          // Status::TryAgain indicates asynchronous request for retrieval of
          // data blocks has been submitted. So it should return at this point
          // and Seek should be called again to retrieve the requested block and
          // execute the remaining code.
          return;
        }
      } else {
        InitDataBlock();
      }
    } else {
      // When the user does a reseek, the iterate_upper_bound might have
      // changed. CheckDataBlockWithinUpperBound() needs to be called
      // explicitly if the reseek ends up in the same data block.
      // If the reseek ends up in a different block, InitDataBlock() will do
      // the iterator upper bound check.
      CheckDataBlockWithinUpperBound();
    }

    block_iter_.Seek(target, priority);
    FindKeyForward();
  }

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || target <= current_internal_id());
  }
}

void HnswTableIterator::SeekForPrev(uint32_t target_internal_id) {
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;

  SavePrevIndexValue();

  // Call Seek() rather than SeekForPrev() in the index block, because the
  // target data block will likely to contain the position for `target`, the
  // same as Seek(), rather than than before.
  // For example, if we have three data blocks, each containing two keys:
  //   [2, 4]  [6, 8] [10, 12]
  //  (the keys in the index block would be [4, 8, 12])
  // and the user calls SeekForPrev(7), we need to go to the second block,
  // just like if they call Seek(7).
  // The only case where the block is difference is when they seek to a position
  // in the boundary. For example, if they SeekForPrev(5), we should go to the
  // first block, rather than the second. However, we don't have the information
  // to distinguish the two unless we read the second block. In this case, we'll
  // end up with reading two blocks.
  index_iter_->Seek(target_internal_id);

  if (!index_iter_->Valid()) {
    auto seek_status = index_iter_->status();
    // Check for IO error
    if (!seek_status.IsNotFound() && !seek_status.ok()) {
      ResetDataIter();
      return;
    }

    // With prefix index, Seek() returns NotFound if the prefix doesn't exist
    if (seek_status.IsNotFound()) {
      // Any key less than the target is fine for prefix seek
      ResetDataIter();
      return;
    } else {
      index_iter_->SeekToLast();
    }
    // Check for IO error
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  InitDataBlock();

  block_iter_.SeekForPrevImpl(target_internal_id);

  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
  assert(!block_iter_.Valid() ||
         target_internal_id >= block_iter_.current_internal_id());
}

void HnswTableIterator::SeekToLast() {
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
}

void HnswTableIterator::Next() {
  if (is_at_first_key_from_index_ && !MaterializeCurrentBlock()) {
    return;
  }
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  FindKeyForward();
  CheckOutOfBound();
}

bool HnswTableIterator::NextAndGetResult(IterateResult* result) {
  Next();
  bool is_valid = Valid();
  if (is_valid) {
    result->key = key();
    result->bound_check_result = UpperBoundCheckResult();
    result->value_prepared = !is_at_first_key_from_index_;
  }
  return is_valid;
}

void HnswTableIterator::Prev() {
  if (is_at_first_key_from_index_) {
    is_at_first_key_from_index_ = false;

    index_iter_->Prev();
    if (!index_iter_->Valid()) {
      return;
    }

    InitDataBlock();
    block_iter_.SeekToLast();
  } else {
    assert(block_iter_points_to_real_block_);
    block_iter_.Prev();
  }

  FindKeyBackward();
}

void HnswTableIterator::InitDataBlock() {
  BlockHandle data_block_handle = index_iter_->value();
  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }
    auto* rep = table_->get_rep();

    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(
        rep, data_block_handle, read_options_.readahead_size, is_for_compaction,
        /*no_sequential_checking=*/false, read_options_.rate_limiter_priority);
    Status s;
    table_->NewDataBlockIterator<GorillaCompressedDataBlockIter>(
        read_options_, data_block_handle, &block_iter_, BlockType::kData,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
    block_iter_points_to_real_block_ = true;
    CheckDataBlockWithinUpperBound();
    if (!is_for_compaction &&
        (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
      RecordTick(table_->GetStatistics(), is_last_level_
                                              ? LAST_LEVEL_SEEK_DATA
                                              : NON_LAST_LEVEL_SEEK_DATA);
      seek_stat_state_ = static_cast<SeekStatState>(
          seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
    }
  }
}

void HnswTableIterator::AsyncInitDataBlock(bool is_first_pass) {
  BlockHandle data_block_handle = index_iter_->value();
  bool is_for_compaction =
      lookup_context_.caller == TableReaderCaller::kCompaction;
  if (is_first_pass) {
    if (!block_iter_points_to_real_block_ ||
        data_block_handle.offset() != prev_block_offset_ ||
        // if previous attempt of reading the block missed cache, try again
        block_iter_.status().IsIncomplete()) {
      if (block_iter_points_to_real_block_) {
        ResetDataIter();
      }
      auto* rep = table_->get_rep();
      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      // In case of async_io with Implicit readahead, block_prefetcher_ will
      // always the create the prefetch buffer by setting no_sequential_checking
      // = true.
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction, /*no_sequential_checking=*/read_options_.async_io,
          read_options_.rate_limiter_priority);

      Status s;
      table_->NewDataBlockIterator<GorillaCompressedDataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/true, s);

      if (s.IsTryAgain()) {
        async_read_in_progress_ = true;
        return;
      }
    }
  } else {
    // Second pass will call the Poll to get the data block which has been
    // requested asynchronously.
    Status s;
    table_->NewDataBlockIterator<GorillaCompressedDataBlockIter>(
        read_options_, data_block_handle, &block_iter_, BlockType::kData,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
  }
  block_iter_points_to_real_block_ = true;
  CheckDataBlockWithinUpperBound();

  if (!is_for_compaction &&
      (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_DATA
                                            : NON_LAST_LEVEL_SEEK_DATA);
    seek_stat_state_ = static_cast<SeekStatState>(
        seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
  }
  async_read_in_progress_ = false;
}

bool HnswTableIterator::MaterializeCurrentBlock() {
  assert(is_at_first_key_from_index_);
  assert(!block_iter_points_to_real_block_);
  assert(index_iter_->Valid());

  is_at_first_key_from_index_ = false;
  InitDataBlock();
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.status().ok()) {
    return false;
  }

  block_iter_.SeekToFirst();

  if (!block_iter_.Valid()) {
    block_iter_.Invalidate(Status::Corruption(
        "first key in index doesn't match first key in block"));
    return false;
  }

  return true;
}

void HnswTableIterator::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

  assert(!is_out_of_bound_);
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    // FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void HnswTableIterator::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    // Whether next data block is out of upper bound, if there is one.
    const bool next_block_is_out_of_bound =
        read_options_.iterate_upper_bound != nullptr &&
        block_iter_points_to_real_block_ &&
        block_upper_bound_check_ == BlockUpperBound::kUpperBoundInCurBlock;
    assert(!next_block_is_out_of_bound);
    ResetDataIter();
    index_iter_->Next();
    if (next_block_is_out_of_bound) {
      // The next block is out of bound. No need to read it.
      TEST_SYNC_POINT_CALLBACK("HnswTableIterator:out_of_bound", nullptr);
      // We need to make sure this is not the last data block before setting
      // is_out_of_bound_, since the index key for the last data block can be
      // larger than smallest key of the next file on the same level.
      if (index_iter_->Valid()) {
        is_out_of_bound_ = true;
      }
      return;
    }

    if (!index_iter_->Valid()) {
      return;
    }

    BlockHandle handle = index_iter_->value();

    InitDataBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

void HnswTableIterator::FindKeyBackward() {
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetDataIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitDataBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }

  // We could have check lower bound here too, but we opt not to do it for
  // code simplicity.
}

void HnswTableIterator::CheckOutOfBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_upper_bound_check_ != BlockUpperBound::kUpperBoundBeyondCurBlock &&
      Valid()) {
    is_out_of_bound_ =
        DecodeFixed32(read_options_.iterate_upper_bound->data()) <=
        current_internal_id();
  }
}

void HnswTableIterator::CheckDataBlockWithinUpperBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_iter_points_to_real_block_) {
    block_upper_bound_check_ =
        (DecodeFixed32(read_options_.iterate_upper_bound->data()) >
         current_internal_id())
            ? BlockUpperBound::kUpperBoundBeyondCurBlock
            : BlockUpperBound::kUpperBoundInCurBlock;
  }
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE