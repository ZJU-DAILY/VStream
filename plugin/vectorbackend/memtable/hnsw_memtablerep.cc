// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "hnsw_memtablerep.h"

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "memory/arena.h"
#include "plugin/vectorbackend/memtable/hnsw_memtable_factory.h"
#include "plugin/vectorbackend/options/vector_options.h"
#include "plugin/vectorbackend/util/slice_helper.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswalg.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/space_ip.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/space_l2.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
using dist_t = float;
std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> MemTableRep::GetVectorIndex()
    const {
  return nullptr;
}
}  // namespace ROCKSDB_NAMESPACE

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
HnswMemTableRep::HnswMemTableRep(Allocator* allocator,
                                 const HnswOptions& hnswOptions)
    : MemTableRep(allocator), hnswOptions_(hnswOptions) {
  index_ = std::make_shared<hnswlib::HierarchicalNSW<dist_t>>(
      hnswOptions.dim, hnswOptions.space, allocator, hnswOptions.max_elements,
      hnswOptions.M, hnswOptions.ef_construction, hnswOptions.random_seed,
      hnswOptions.visit_list_pool_size, hnswOptions.allow_replace_deleted);
}

void HnswMemTableRep::MarkReadOnly() {
  index_->readonly_ = true;
  index_->label_op_locks_.clear();
}

void HnswMemTableRep::InsertImpl(KeyHandle handle, bool concurrent) {
  // KeyHandle:
  //  VarintLength(internal_key_size)
  //  internal_key
  //  VarintLength(val_size)
  //  val
  //  moptions_.protection_bytes_per_key
  const char* data = reinterpret_cast<const char*>(handle);
  uint32_t internal_key_size;
  data = GetVarint32Ptr(data, data + 5, &internal_key_size);
  assert(internal_key_size == sizeof(labeltype) + sizeof(SequenceNumber));

  labeltype label;
  memcpy(&label, data, sizeof(labeltype));

  // SequenceNumber seq;
  uint64_t packed;
  memcpy(&packed, data + sizeof(labeltype), sizeof(SequenceNumber));

  uint64_t seq;
  ValueType type;
  UnPackSequenceAndType(packed, &seq, &type);

  switch (type) {
    case kTypeValue:
      // value
      uint32_t val_size;
      data += sizeof(labeltype) + sizeof(SequenceNumber);
      data = GetVarint32Ptr(data, data + 5, &val_size);
      assert(val_size == index_->data_size_ + sizeof(uint64_t));
      uint64_t ts;
      memcpy(&ts, data, sizeof(uint64_t));
      data += sizeof(uint64_t);
      index_->addPoint((void*)(data), label, seq, ts,
                       hnswOptions_.allow_replace_deleted, true, concurrent);
      break;
    case kTypeDeletion:
      index_->markDelete(label, seq, concurrent);
      break;
    default:
      throw std::runtime_error("Unknown type");
  }

  delete[] handle;
}

KeyHandle HnswMemTableRep::Allocate(const size_t len, char** buf) {
  *buf = new char[len];
  return static_cast<KeyHandle>(*buf);
}

bool HnswMemTableRep::ShouldFlushNow() const {
  return index_->cur_element_count + 10 >= index_->max_elements_;
}

size_t HnswMemTableRep::ApproximateMemoryUsage() {
  // todo: only calculate the memory allocated without the allocator
  return index_->ApproximateMemoryUsage();
}

void HnswMemTableRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  throw std::runtime_error("unsupported operation");
}

void HnswMemTableRep::Search(const LookupKey& key, std::string* value,
                             Status* s, const ReadOptions& read_opts,
                             bool /*immutable_memtable*/) {
  if (read_opts.GetType() == ReadOptions::kVectorSearch) {
    size_t k = static_cast<const VectorSearchOptions*>(&read_opts)->k;
    uint64_t ts = static_cast<const VectorSearchOptions*>(&read_opts)->ts;
    const char* internal_key = key.internal_key().data();
    SequenceNumber seq;
    memcpy(&seq, internal_key + key.internal_key().size() - sizeof(SequenceNumber), sizeof(SequenceNumber));
    seq >>= 8;
    hnswlib::FilterSeqTs filterFunctor = hnswlib::FilterSeqTs(seq, ts);
    // Search in the hnsw index is lock-free
    auto result = index_->searchKnn(internal_key, k, &filterFunctor);
    if (value) {
      if (!value->empty()) {
        // 归并
        const char* data = value->data();
        uint16_t size;
        memcpy(&size, data, sizeof(size));
        data = data + sizeof(size);
        for (uint32_t i = 0; i < size; ++i) {
          hnswlib::ResultItem<dist_t> item;
          memcpy(&(item.dist), data, sizeof(dist_t));
          data = data + sizeof(dist_t);
          memcpy(&(item.label), data, sizeof(labeltype));
          data = data + sizeof(labeltype);
          memcpy(&(item.version), data, sizeof(SequenceNumber));
          data = data + sizeof(SequenceNumber);
          memcpy(&(item.file_number), data, sizeof(uint64_t));
          data = data + sizeof(uint64_t);
          result.push(item);
          if (result.size() > k) {
            result.pop();
          }
        }
      } else {
        value->resize(sizeof(uint16_t) +
                      k * (sizeof(dist_t) + sizeof(labeltype) +
                           sizeof(SequenceNumber) + sizeof(uint64_t)));
      }
      uint16_t result_size = result.size();
      char* data = value->data();
      memcpy(data, &result_size, sizeof(uint16_t));
      data += sizeof(uint16_t);
      while (!result.empty()) {
        auto item = result.top();
        result.pop();
        memcpy(data, &(item.dist), sizeof(dist_t));
        data += sizeof(dist_t);
        memcpy(data, &(item.label), sizeof(labeltype));
        data += sizeof(labeltype);
        memcpy(data, &(item.version), sizeof(SequenceNumber));
        data += sizeof(SequenceNumber);
        memcpy(data, &(item.file_number), sizeof(uint64_t));
        data += sizeof(uint64_t);
      }
      // value->resize(data - value->data());
    }
    *s = Status::OK();
  } else {
    *s = Status::InvalidArgument("read options is not VectorSearchOptions");
  }
}

bool HnswMemTableRep::Contains(const char* key) const {
  uint32_t internal_key_size;
  key = GetVarint32Ptr(key, key + 5, &internal_key_size);
  assert(internal_key_size == sizeof(labeltype) + sizeof(SequenceNumber));

  labeltype label;
  memcpy(&label, key, sizeof(labeltype));
  return index_->contains(label);
}

MemTableRep::Iterator* HnswMemTableRep::GetIterator(Arena* arena) {
  void* mem = arena ? arena->AllocateAligned(sizeof(Iterator)) :
                    operator new(sizeof(Iterator));
  return new (mem) Iterator(index_);
}

HnswMemTableRep::Iterator::Iterator(
    std::shared_ptr<hnswlib::HierarchicalNSW<dist_t>> index)
    : index_(std::move(index)) {
  buf = new char[10 + sizeof(hnswlib::tableint) + sizeof(SequenceNumber) + 10 +
                 index_->size_data_per_element_];
  current = 0;
}

HnswMemTableRep::Iterator::~Iterator() { delete[] buf; }

bool HnswMemTableRep::Iterator::Valid() const {
  return current >= 0 && current < index_->cur_element_count;
}

const char* HnswMemTableRep::Iterator::key() const {
  hnswlib::tableint internal_id = current;
  uint64_t version_with_type = index_->getVersionWithType(internal_id);

  // KeyHandle:
  //  VarintLength(internal_key_size)
  //  internal_key
  //  VarintLength(val_size)
  //  val
  //  moptions_.protection_bytes_per_key
  char* buf_ptr =
      EncodeVarint32(buf, sizeof(uint64_t) + sizeof(hnswlib::tableint));
  EncodeFixed32(buf_ptr, internal_id);
  buf_ptr += sizeof(hnswlib::tableint);
  EncodeFixed64(buf_ptr, version_with_type);  // TODO: 8 bytes wasted
  buf_ptr += sizeof(uint64_t);
  buf_ptr = EncodeVarint32(buf_ptr, index_->size_data_per_element_);
  memcpy(buf_ptr,
         index_->data_level0_memory_ +
             internal_id * index_->size_data_per_element_,
         index_->size_data_per_element_);
  return buf;
}

void HnswMemTableRep::Iterator::Next() { current++; }

void HnswMemTableRep::Iterator::Prev() { current--; }

void HnswMemTableRep::Iterator::Seek(const Slice& internal_key,
                                     const char* memtable_key) {
  current = *reinterpret_cast<const uint32_t*>(internal_key.data());
}

void HnswMemTableRep::Iterator::SeekForPrev(const Slice& internal_key,
                                            const char* memtable_key) {
  current = *reinterpret_cast<const uint32_t*>(internal_key.data());
}

void HnswMemTableRep::Iterator::SeekToFirst() { current = 0; }

void HnswMemTableRep::Iterator::SeekToLast() {
  current = index_->cur_element_count - 1;
}

MemTableRep* HnswMemTableFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& /*compare*/, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new HnswMemTableRep(allocator, hnswOptions_);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
