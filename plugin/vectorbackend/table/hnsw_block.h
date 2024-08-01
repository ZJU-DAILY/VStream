//
// Created by shb on 23-8-30.
//

#pragma once

#include <memory>

#include "../compression/compression.h"
#include "../vectorbackend/vectorbackend_namespace.h"
#include "hnsw_block_cache.h"
#include "table/block_based/block.h"

namespace hnswlib {
typedef size_t labeltype;
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;
}  // namespace hnswlib

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

class HnswIndexBlockIter final : public InternalIteratorBase<BlockHandle> {
 public:
  void Initialize(const char* data, uint32_t data_size,
                  bool block_contents_pinned) {
    assert(data_ == nullptr);  // Ensure it is called only once
    assert(data_size % (3 * sizeof(uint32_t)) == 0);
    data_ = data;
    data_size_ = data_size;
    num_entries_ = data_size / (3 * sizeof(uint32_t));
    current_ = data_size_;
    block_contents_pinned_ = block_contents_pinned;
    cache_handle_ = nullptr;
  }

  void CorruptionError(const std::string& error_msg = "bad entry in block") {
    current_ = data_size_;
    status_ = Status::Corruption(error_msg);
    raw_entry_.clear();
  }

  // Makes Valid() return false, status() return `s`, and Seek()/Prev()/etc do
  // nothing. Calls cleanup functions.
  void Invalidate(const Status& s) {
    // Assert that the HnswIndexBlockIter is never deleted while Pinning is
    // Enabled.
    assert(!pinned_iters_mgr_ || !pinned_iters_mgr_->PinningEnabled());

    data_ = nullptr;
    current_ = data_size_;
    status_ = s;

    // call cleanup callbacks.
    Cleanable::Reset();
  }

  bool Valid() const override {
    // When status_ is not ok, iter should be invalid.
    assert(status_.ok() || current_ >= data_size_);
    return current_ < data_size_;
  }

  inline bool ParseNextIndexKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + data_size_;

    if (p >= limit) {
      // No more entries to return. Mark as invalid.
      current_ = data_size_;
      return false;
    }
    // Decode next entry
    raw_entry_ = Slice(p, sizeof(uint32_t) * 3);
    return true;
  }

  void SeekToFirst() override {
#ifndef NDEBUG
    if (TEST_Corrupt_Callback("HnswIndexBlockIter::SeekToFirstImpl")) return;
#endif
    if (data_ == nullptr) {  // Not init yet
      return;
    }
    status_ = Status::OK();
    current_ = 0;
    raw_entry_ = Slice(data_ + current_, sizeof(uint32_t) * 3);
  }

  void SeekToLast() override {
    if (data_ == nullptr) {  // Not init yet
      return;
    }
    status_ = Status::OK();
    current_ = data_size_ - 3 * sizeof(uint32_t);
    raw_entry_ = Slice(data_ + current_, sizeof(uint32_t) * 3);
  }

  void Seek(const Slice& target) override { Seek(DecodeFixed32(target.data_)); }

  void Seek(const uint32_t target) {
#ifndef NDEBUG
    if (TEST_Corrupt_Callback("HnswIndexBlockIter::SeekImpl")) return;
#endif
    TEST_SYNC_POINT("HnswIndexBlockIter::Seek:0");
    PERF_TIMER_GUARD(block_seek_nanos);
    if (data_ == nullptr) {  // Not init yet
      return;
    }
    status_ = Status::OK();
    uint32_t index;

    // Loop invariants:
    // - Restart key at index `left` is less than or equal to the target key.
    // The
    //   sentinel index `-1` is considered to have a key that is less than all
    //   keys.
    // - Any restart keys after index `right` are strictly greater than the
    // target
    //   key.
    int64_t left = 0, right = num_entries_ - 1;
    while (left != right) {
      int64_t mid = left + (right - left) / 2;
      uint32_t mid_key = DecodeFixed32(data_ + static_cast<uint32_t>(mid) *
                                                   sizeof(uint32_t) * 3);
      if (mid_key < target) {
        // Key at "mid" is smaller than "target". Therefore all
        // blocks before "mid" are uninteresting.
        left = mid + 1;
      } else if (mid_key > target) {
        // Key at "mid" is >= "target". Therefore all blocks at or
        // after "mid" are uninteresting.
        right = mid;
      } else {
        left = right = mid;
      }
    }

    if (left == -1) {
      // All keys in the block were strictly greater than `target`. So the very
      // first key in the block is the final seek result.
      index = 0;
    } else {
      index = static_cast<uint32_t>(left);
    }

    current_ = static_cast<uint32_t>(index) * sizeof(uint32_t) * 3;
    raw_entry_ = Slice(data_ + current_, sizeof(uint32_t) * 3);
  }

  void SeekForPrev(const Slice& /* target */) { SeekForPrev(0); }

  void SeekForPrev(const uint32_t /* target */) {
    assert(false);
    current_ = data_size_;
    status_ = Status::InvalidArgument(
        "RocksDB internal error: should never call SeekForPrev() on index "
        "blocks");
    raw_entry_.clear();
  }

  void Next() override { ParseNextIndexKey(); }

  bool NextAndGetResult(IterateResult* result) override {
    // This does not need to call `UpdateKey()` as the parent class only has
    // access to the `UpdateKey()`-invoking functions.
    return InternalIteratorBase<BlockHandle>::NextAndGetResult(result);
  }

  void Prev() override {
    assert(Valid());
    if (current_ == 0) {
      // No more entries
      current_ = data_size_;
      return;
    }

    current_ -= 3 * sizeof(uint32_t);
    raw_entry_ = Slice(data_ + current_, 0);
    ParseNextIndexKey();
  }

  Status status() const override { return status_; }

  Slice key() const override {
    assert(Valid());
    return {raw_entry_.data_, sizeof(uint32_t)};
  }

#ifndef NDEBUG
  ~HnswIndexBlockIter() override {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    status_.PermitUncheckedError();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;

  bool TEST_Corrupt_Callback(const std::string& sync_point) {
    bool corrupt = false;
    TEST_SYNC_POINT_CALLBACK(sync_point, static_cast<void*>(&corrupt));

    if (corrupt) {
      CorruptionError();
    }
    return corrupt;
  }
#endif

  Slice user_key() const override {
    assert(Valid());
    return {raw_entry_.data_, sizeof(uint32_t)};
  }

  uint32_t current_internal_id() const {
    assert(Valid());
    return DecodeFixed32(raw_entry_.data_);
  }

  BlockHandle value() const {
    assert(Valid());
    return {DecodeFixed32(raw_entry_.data_ + sizeof(uint32_t)),
            DecodeFixed32(raw_entry_.data_ + 2 * sizeof(uint32_t))};
  }

  bool IsKeyPinned() const override { return block_contents_pinned_; }

  bool IsValuePinned() const override { return block_contents_pinned_; }

  size_t TEST_CurrentEntrySize() { return 3 * sizeof(uint32_t); }

  uint32_t ValueOffset() const {
    return static_cast<uint32_t>(raw_entry_.data() + sizeof(uint32_t) - data_);
  }

  void SetCacheHandle(Cache::Handle* handle) { cache_handle_ = handle; }

  Cache::Handle* cache_handle() { return cache_handle_; }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    // NOTE: We don't support blocks bigger than 2GB
    return static_cast<uint32_t>((raw_entry_.data() + raw_entry_.size()) -
                                 data_);
  }

 private:
  // Store the cache handle, if the block is cached. We need this since the
  // only other place the handle is stored is as an argument to the Cleanable
  // function callback, which is hard to retrieve. When multiple value
  // PinnableSlices reference the block, they need the cache handle in order
  // to bump up the ref count
  Cache::Handle* cache_handle_;

  const char* data_ = nullptr;  // underlying block contents

  uint32_t data_size_;
  uint32_t num_entries_;
  // current_ is offset in data_ of current entry.  >= data_size_ if !Valid
  uint32_t current_;
  // Raw key from block.
  Slice raw_entry_;
  Status status_;
  // Whether the block data is guaranteed to outlive this iterator, and
  // as long as the cleanup functions are transferred to another class,
  // e.g. PinnableSlice, the pointer to the bytes will still be valid.
  bool block_contents_pinned_;
};

struct Entry {
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kBlobValue;
  using SharedCache = BasicTypedSharedCacheInterface<Entry>;
  using TypedHandle = SharedCache::TypedHandle;

  char* data_;
  float* vector_;
  TypedHandle* cache_handle_ = nullptr;

  Entry(char* data, float* vector_) : data_(data), vector_(vector_) {}
  ~Entry() {
    delete[] data_;
    delete[] vector_;
  }
};

class GorillaCompressedDataBlockIter : public InternalIteratorBase<Entry*> {
 public:
  GorillaCompressedDataBlockIter()
      : InternalIteratorBase(),
        read_amp_bitmap_(nullptr),
        last_bitmap_offset_(0) {}

  void Initialize(const char* data, uint32_t restarts, uint32_t num_restarts,
                  BlockReadAmpBitmap* read_amp_bitmap,
                  bool block_contents_pinned, size_t dim, size_t M,
                  size_t block_restart_interval, const std::string& table_name,
                  std::shared_ptr<Cache> block_cache) {
    assert(data_ == nullptr);  // Ensure it is called only once
    assert(num_restarts > 0);  // Ensure the param is valid
    data_ = data;
    restarts_ = restarts;
    num_restarts_ = num_restarts;
    current_ = restarts_;
    restart_index_ = num_restarts_;
    block_contents_pinned_ = block_contents_pinned;
    cache_handle_ = nullptr;
    cur_entry_idx_ = 0;
    read_amp_bitmap_ = read_amp_bitmap;
    last_bitmap_offset_ = current_ + 1;
    dim_ = dim;
    decompressor_ =
        std::make_unique<::compression::GorillaVectorDecompressor>(dim);
    offsetData_ =
        M * 2 * sizeof(hnswlib::tableint) + sizeof(hnswlib::linklistsizeint) +
        sizeof(hnswlib::labeltype) + sizeof(SequenceNumber) + sizeof(uint64_t);
    block_restart_interval_ = GetRestartInterval(block_restart_interval);
    assert(block_restart_interval_ > 0 || num_restarts == 1);
    table_name_ = table_name;
    block_cache_ = std::make_shared<
        BasicTypedSharedCacheInterface<Entry, CacheEntryRole::kBlobValue>>(
        block_cache);
  }

  void CorruptionError(const std::string& error_msg = "bad entry in block") {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption(error_msg);
    value_ = nullptr;
  }

  size_t size() const { return dim_ * sizeof(float); }

  bool Valid() const override {
    // When status_ is not ok, iter should be invalid.
    assert(status_.ok() || current_ >= restarts_);
    return current_ < restarts_;
  }

  inline bool SeekForGet(const Slice& target) {
#ifndef NDEBUG
    if (TEST_Corrupt_Callback("GorillaCompressedDataBlockIter::SeekForGet"))
      return true;
#endif
    SeekImpl(target);
  }

  void SeekToFirst() final {
#ifndef NDEBUG
    if (TEST_Corrupt_Callback("GorillaCompressedDataBlockIter::SeekToFirst"))
      return;
#endif
    SeekToFirstImpl();
  }

  void SeekToLast() final { SeekToLastImpl(); }

  void Seek(const Slice& target) final { SeekImpl(target); }

  void Seek(const uint32_t target_internal_id,
            Cache::Priority priority = Cache::Priority::LOW) {
    SeekImpl(target_internal_id, priority);
  }

  void SeekForPrev(const Slice& target) final { SeekForPrevImpl(target); }

  void Next() final { NextImpl(); }

  Status status() const override { return status_; }

  Slice key() const override {
    assert(Valid());
    return {cur_offset_, 4};
  }

  // Returns the restart interval of this block.
  // Returns 0 if num_restarts_ <= 1 or if the BlockIter is not initialized.
  uint32_t GetRestartInterval(size_t block_restart_interval) {
    if (UNLIKELY(num_restarts_ <= 0 || data_ == nullptr)) {
      return 0;
    }
    return block_restart_interval;
  }

  uint32_t current_internal_id() const {
    assert(Valid());
    return stored_internal_id_;
  }

#ifndef NDEBUG
  ~GorillaCompressedDataBlockIter() override {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    status_.PermitUncheckedError();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;

  bool TEST_Corrupt_Callback(const std::string& sync_point) {
    bool corrupt = false;
    TEST_SYNC_POINT_CALLBACK(sync_point, static_cast<void*>(&corrupt));

    if (corrupt) {
      CorruptionError();
    }
    return corrupt;
  }
#endif

  bool IsKeyPinned() const override { return block_contents_pinned_; }

  bool IsValuePinned() const override { return block_contents_pinned_; }

  size_t TEST_CurrentEntrySize() { return NextEntryOffset() - current_; }

  uint32_t ValueOffset() const { return cur_offset_ - data_; }

  void SetCacheHandle(Cache::Handle* handle) { cache_handle_ = handle; }

  Cache::Handle* cache_handle() { return cache_handle_; }

  void Prev() override final { PrevImpl(); }

  bool NextAndGetResult(IterateResult* result) override final {
    // This does not need to call `UpdateKey()` as the parent class only has
    // access to the `UpdateKey()`-invoking functions.
    return InternalIteratorBase<Entry*>::NextAndGetResult(result);
  }

  Entry* value() const {
    // assert(Valid());
    if (read_amp_bitmap_ && current_ < restarts_ &&
        current_ != last_bitmap_offset_) {
      read_amp_bitmap_->Mark(current_ /* current entry offset */,
                             NextEntryOffset() - 1);
      last_bitmap_offset_ = current_;
    }
    return value_;
  }

  std::string ReadableValueString() const {
    Entry* entry_value = value();
    float* vector = entry_value->vector_;
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < dim_; ++i) {
      if (i != 0) {
        ss << ", ";
      }
      ss << vector[i];
    }
    ss << "]";
  }

  // Makes Valid() return false, status() return `s`, and Seek()/Prev()/etc do
  // nothing. Calls cleanup functions.
  void Invalidate(const Status& s) {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ || !pinned_iters_mgr_->PinningEnabled());

    data_ = nullptr;
    current_ = restarts_;
    status_ = s;

    // Call cleanup callbacks.
    Cleanable::Reset();
    // Clear prev entries cache.
    prev_entries_.clear();
    prev_entries_idx_ = -1;
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const { return next_offset_ - data_; }

  uint32_t GetRestartPoint(uint32_t index) const {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of raw_entry_, so set raw_entry_
    // accordingly
    uint32_t offset = GetRestartPoint(index);
    cur_offset_ = const_cast<char*>(data_ + offset);
    next_offset_ = cur_offset_;
  }

  void SeekForPrevImpl(uint32_t target_internal_id);

 private:
  friend Block;
  inline bool BinarySeek(const Slice& target, uint32_t* index) {
    BinarySeek(DecodeFixed32(target.data()), index);
  }
  inline bool BinarySeek(uint32_t target_internal_id, uint32_t* index);
  // Find the first key in restart interval `index` that is >= `target`.
  // If there is no such key, iterator is positioned at the first key in
  // restart interval `index + 1`.
  // Per key-value checksum verification is done for all keys scanned
  // up to but not including the last key (the key that current_ points to
  // when this function returns). This key's checksum is verified in
  // UpdateKey().
  void FindKeyAfterBinarySeek(const Slice& target, uint32_t index) {
    FindKeyAfterBinarySeek(DecodeFixed32(target.data()), index);
  }
  void FindKeyAfterBinarySeek(uint32_t target_internal_id, uint32_t index,
                              Cache::Priority priority = Cache::Priority::LOW);

  inline bool ParseNextDataKey();
  void SeekToFirstImpl();
  void SeekToLastImpl();
  void SeekImpl(const Slice& target) { SeekImpl(DecodeFixed32(target.data())); }
  void SeekImpl(uint32_t target_internal_id,
                Cache::Priority priority = Cache::Priority::LOW);
  void SeekForPrevImpl(const Slice& target) {
    SeekForPrevImpl(DecodeFixed32(target.data()));
  }
  void NextImpl();
  void PrevImpl();

  // Stores whether the current key has a shared bytes with prev key in
  // *is_shared.
  // Sets raw_key_, raw_entry_ to the current parsed key and value.
  // Sets restart_index_ to point to the restart interval that contains
  // the current key.
  inline bool ParseNextKey();

  // Returns the result of `Comparator::Compare()`, where the appropriate
  // comparator is used for the block contents, the LHS argument is the current
  // key with global seqno applied, and the RHS argument is `other`.
  inline int CompareCurrentKey(const Slice& other) {
    return CompareCurrentKey(DecodeFixed32(other.data()));
  }

  inline int CompareCurrentKey(uint32_t other) const {
    return (stored_internal_id_ < other)
               ? -1
               : ((stored_internal_id_ > other) ? 1 : 0);
  }

  std::unique_ptr<InternalKeyComparator> icmp_;
  const char* data_ = nullptr;  // underlying block contents
  size_t dim_;
  std::unique_ptr<::compression::GorillaVectorDecompressor> decompressor_;
  std::string table_name_;
  std::shared_ptr<
      BasicTypedSharedCacheInterface<Entry, CacheEntryRole::kBlobValue>>
      block_cache_;
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // Index of restart block in which current_ or current_-1 falls
  uint32_t restart_index_;
  uint32_t restarts_;  // Offset of restart array (list of fixed32)
  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t stored_internal_id_;
  Entry* value_;
  const char* cur_offset_;
  const char* next_offset_;
  Status status_;

  int32_t cur_entry_idx_;
  uint32_t block_restart_interval_;

  uint32_t offsetData_;

  // Whether the block data is guaranteed to outlive this iterator, and
  // as long as the cleanup functions are transferred to another class,
  // e.g. PinnableSlice, the pointer to the bytes will still be valid.
  bool block_contents_pinned_;

  // Store the cache handle, if the block is cached. We need this since the
  // only other place the handle is stored is as an argument to the Cleanable
  // function callback, which is hard to retrieve. When multiple value
  // PinnableSlices reference the block, they need the cache handle in order
  // to bump up the ref count
  Cache::Handle* cache_handle_;

  // read-amp bitmap
  BlockReadAmpBitmap* read_amp_bitmap_;
  // last `current_` value we report to read-amp bitmp
  mutable uint32_t last_bitmap_offset_;
  struct CachedPrevEntry {
    explicit CachedPrevEntry(uint32_t _offset, const Entry* _value)
        : offset(_offset), value(_value) {}

    // offset of entry in block
    uint32_t offset;
    // decoded value
    const Entry* value;
  };
  std::vector<CachedPrevEntry> prev_entries_;
  int32_t prev_entries_idx_ = -1;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE