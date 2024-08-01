//
// Created by shb on 23-9-1.
//

#include "hnsw_block.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
void GorillaCompressedDataBlockIter::NextImpl() {
#ifndef NDEBUG
  if (TEST_Corrupt_Callback("GorillaCompressedDataBlockIter::NextImpl")) return;
#endif
  ParseNextDataKey();
  ++cur_entry_idx_;
}

void GorillaCompressedDataBlockIter::PrevImpl() {
  assert(Valid());

  assert(prev_entries_idx_ == -1 ||
         static_cast<size_t>(prev_entries_idx_) < prev_entries_.size());
  --cur_entry_idx_;
  // Check if we can use cached prev_entries_
  if (prev_entries_idx_ > 0 &&
      prev_entries_[prev_entries_idx_].offset == current_) {
    // Read cached CachedPrevEntry
    prev_entries_idx_--;

    value_ = const_cast<Entry *>(prev_entries_[prev_entries_idx_].value);
    return;
  }

  // Clear prev entries cache
  prev_entries_idx_ = -1;
  prev_entries_.clear();

  // Scan backwards to a restart point before current_
  const uint32_t original = current_;
  while (GetRestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      // No more entries
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return;
    }
    restart_index_--;
  }

  SeekToRestartPoint(restart_index_);

  do {
    if (!ParseNextDataKey()) {
      break;
    }
    prev_entries_.emplace_back(current_, value());
    // Loop until end of current entry hits the start of original entry
  } while (NextEntryOffset() < original);
  prev_entries_idx_ = static_cast<int32_t>(prev_entries_.size()) - 1;
}

void GorillaCompressedDataBlockIter::SeekImpl(uint32_t target_internal_id,
                                              Cache::Priority priority) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target_internal_id, &index);

  if (!ok) {
    return;
  }
  FindKeyAfterBinarySeek(target_internal_id, index, priority);
}

void GorillaCompressedDataBlockIter::SeekForPrevImpl(
    uint32_t target_internal_id) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target_internal_id, &index);

  if (!ok) {
    return;
  }
  FindKeyAfterBinarySeek(target_internal_id, index);

  if (!Valid()) {
    if (status_.ok()) {
      SeekToLastImpl();
    }
  } else {
    while (Valid() && CompareCurrentKey(target_internal_id) > 0) {
      PrevImpl();
    }
  }
}

void GorillaCompressedDataBlockIter::SeekToLastImpl() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(num_restarts_ - 1);
  cur_entry_idx_ = (num_restarts_ - 1) * block_restart_interval_;
  while (ParseNextDataKey() && NextEntryOffset() < restarts_) {
    // Keep skipping
    ++cur_entry_idx_;
  }
}

void GorillaCompressedDataBlockIter::SeekToFirstImpl() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(0);
  ParseNextDataKey();
  cur_entry_idx_ = 1;
}

void GorillaCompressedDataBlockIter::FindKeyAfterBinarySeek(
    uint32_t target_internal_id, uint32_t index, Cache::Priority priority) {
  // SeekToRestartPoint() only does the lookup in the restart block. We need
  // to follow it up with NextImpl() to position the iterator at the restart
  // key.
  SeekToRestartPoint(index);
  cur_entry_idx_ = static_cast<int32_t>(index * block_restart_interval_);
  // NextImpl();

  // Linear search (within restart block) for first key >= target
  uint32_t max_offset;
  if (index + 1 < num_restarts_) {
    // We are in a non-last restart interval. Since `BinarySeek()` guarantees
    // the next restart key is strictly greater than `target`, we can
    // terminate upon reaching it without any additional key comparison.
    max_offset = GetRestartPoint(index + 1);
  } else {
    // We are in the last restart interval. The while-loop will terminate by
    // `Valid()` returning false upon advancing past the block's last key.
    max_offset = std::numeric_limits<uint32_t>::max();
  }
  Entry *result_value;
  while (true) {
    NextImpl();

    if (!Valid()) {
      // TODO(cbi): per key-value checksum will not be verified in UpdateKey()
      //  since Valid() will returns false.
      break;
    }

    std::string cache_key =
        table_name_ + "#" + std::to_string(stored_internal_id_);
    auto cache_handle = block_cache_->Lookup(cache_key);
    if (cache_handle == nullptr) {
      Entry::TypedHandle *new_cache_handle;
      block_cache_->Insert(cache_key, value_,
                           sizeof(*value_) + offsetData_ + sizeof(float) * dim_,
                           &new_cache_handle, priority);
      value_->cache_handle_ = new_cache_handle;
    } else {
      delete value_;
      value_ = block_cache_->Value(cache_handle);
      assert(value_->cache_handle_ == cache_handle);
    }

    if (current_ == max_offset) {
      assert(CompareCurrentKey(target_internal_id) > 0);
      break;
    } else if (CompareCurrentKey(target_internal_id) == 0) {
      break;
    } else {
      block_cache_->Release(value_->cache_handle_);
    }
  }
}

bool GorillaCompressedDataBlockIter::BinarySeek(uint32_t target_internal_id,
                                                uint32_t *index) {
  if (restarts_ == 0) {
    return false;
  }

  // Loop invariants:
  // - Restart key at index `left` is less than or equal to the target key. The
  //   sentinel index `-1` is considered to have a key that is less than all
  //   keys.
  // - Any restart keys after index `right` are strictly greater than the target
  //   key.
  int64_t left = -1, right = num_restarts_ - 1;
  while (left != right) {
    // The `mid` is computed by rounding up, so it lands in (`left`, `right`].
    int64_t mid = left + (right - left + 1) / 2;
    uint32_t region_offset = GetRestartPoint(static_cast<uint32_t>(mid));

    uint32_t mid_key =
        *reinterpret_cast<const uint32_t *>(data_ + region_offset);

    if (mid_key < target_internal_id) {
      // Key at "mid" is smaller than "target". Therefore, all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (mid_key > target_internal_id) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  if (left == -1) {
    // All keys in the block were strictly greater than `target`. So the very
    // first key in the block is the final seek result.
    *index = 0;
  } else {
    *index = static_cast<uint32_t>(left);
  }
  return true;
}

bool GorillaCompressedDataBlockIter::ParseNextKey() {
  current_ = NextEntryOffset();
  const char *p = data_ + current_;
  const char *limit = data_ + restarts_;

  if (p >= limit) {
    // No more entries to return.  Mark as invalid.
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  char *data;
  float *vector;
  // Decode next entry
  uint32_t idx_of_restart_interval = cur_entry_idx_ % block_restart_interval_;
  if (idx_of_restart_interval == 0) {
    cur_offset_ = p;
    stored_internal_id_ = *reinterpret_cast<const uint32_t *>(p);
    p += 4;
    data = new char[offsetData_];
    memcpy(data, p, offsetData_);
    next_offset_ = reinterpret_cast<const char *>(
        decompressor_->ReadFirstVector(p + offsetData_, vector));
  } else {
    cur_offset_ = p;
    stored_internal_id_ = *reinterpret_cast<const uint32_t *>(p);
    p += 4;
    data = new char[offsetData_];
    memcpy(data, p, offsetData_);
     if (idx_of_restart_interval == 1) {
       next_offset_ = reinterpret_cast<const char *>(
           decompressor_->ReadSecondVector(p + offsetData_, vector));
     } else {
      next_offset_ = reinterpret_cast<const char *>(
          decompressor_->ReadNextVector(p + offsetData_, vector));
     }
  }

  value_ = new Entry(data, vector);
  return true;
}

bool GorillaCompressedDataBlockIter::ParseNextDataKey() {
  if (ParseNextKey()) {
    /*#ifndef NDEBUG
        if (global_seqno_ != kDisableGlobalSequenceNumber) {
          // If we are reading a file with a global sequence number we should
          // expect that all encoded sequence numbers are zeros and any value
          // type is kTypeValue, kTypeMerge, kTypeDeletion,
          // kTypeDeletionWithTimestamp, or kTypeRangeDeletion.
          SequenceNumber seqno;
          ValueType value_type;
          UnPackSequenceAndType(stored_sequence_no_, &seqno, &value_type);
          assert(value_type == ValueType::kTypeValue ||
                 value_type == ValueType::kTypeMerge ||
                 value_type == ValueType::kTypeDeletion ||
                 value_type == ValueType::kTypeDeletionWithTimestamp ||
                 value_type == ValueType::kTypeRangeDeletion);
          assert(seqno == 0);
        }
    #endif  // NDEBUG
     */
    return true;
  } else {
    return false;
  }
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
