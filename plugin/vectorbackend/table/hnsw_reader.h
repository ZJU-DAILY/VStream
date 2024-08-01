//
// Created by shb on 23-8-30.
//

#pragma once

#include <queue>

#include "hnsw_block.h"
#include "hnsw_block_cache.h"
#include "memory/arena.h"
#include "table/block_based/cachable_entry.h"

namespace hnswlib {
typedef size_t labeltype;
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;

template <typename dist_t>
class AlgorithmInterface;

template <typename dist_t>
struct ResultItem;

template <typename dist_t>
struct CompareByFirst;

template <typename dist_t>
struct CompareWithVersion;

template <typename dist_t>
using KnnResult =
    std::priority_queue<ResultItem<dist_t>, std::vector<ResultItem<dist_t>>,
                        CompareWithVersion<dist_t>>;

template <typename MTYPE>
using DISTFUNC = MTYPE (*)(const void*, const void*, const size_t);

class VisitedListPool;
}  // namespace hnswlib

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class HnswTable;
struct HnswTableOptions;
class HnswTableIterator;

using dist_t = float;
class HnswReader : public Cleanable {
 public:
  HnswReader(HnswTable* table, const ReadOptions& ro,
             CachableEntry<Block_kHnsw>* hnsw_block,
             const HnswTableOptions& table_options,
             size_t compaction_readahead_size, bool allow_unprepared_value);

  static Status Create(HnswTable* table, const ReadOptions& ro,
                       CachableEntry<Block_kHnsw>* hnsw_block,
                       std::unique_ptr<HnswReader>* hnsw_reader,
                       size_t compaction_readahead_size = 0,
                       bool allow_unprepared_value = false);

  inline const float* getDataByInternalId(hnswlib::tableint internal_id) const;

  inline Entry* getEntryByInternalId(
      hnswlib::tableint internal_id,
      Cache::Priority priority = Cache::Priority::BOTTOM) const;

  inline bool isMarkedDeleted(const char* link_list0) const {
    unsigned char* ll_cur = (unsigned char*)link_list0 + value_type_offset_;
    return *ll_cur == ValueType::kTypeDeletion;
  }

  size_t ApproximateMemoryUsage() const {
    return sizeof(*this) + arena.ApproximateMemoryUsage();
  }

  std::priority_queue<std::pair<dist_t, hnswlib::tableint>,
                      std::vector<std::pair<dist_t, hnswlib::tableint>>,
                      hnswlib::CompareByFirst<dist_t>>
  searchBaseLayerST(hnswlib::tableint ep_id, const void* data_point, size_t ef,
                    const SequenceNumber* snapshot, uint64_t timestamp) const;

  hnswlib::KnnResult<dist_t> searchKnn(const void* query_data, size_t k,
                                       uint64_t ts,
                                       const SequenceNumber* snapshot,
                                       uint64_t file_number) const;

  ~HnswReader();

  inline Entry* get_linklist0(hnswlib::tableint internal_id) const;

  inline hnswlib::linklistsizeint* get_linklist(hnswlib::tableint internal_id,
                                                unsigned int level) const;

  static inline unsigned short int getListCount(const void* ptr) {
    return *reinterpret_cast<const unsigned short int*>(ptr);
  }

  inline SequenceNumber getVersion(const void* link_list) const {
    SequenceNumber version;
    memcpy(&version, ((unsigned char*)link_list) + version_offset_,
           sizeof(SequenceNumber) - sizeof(ValueType));
    return version >> 8;
  }

  inline uint64_t getTimestamp(const void* link_list) const {
    uint64_t ts;
    memcpy(&ts, ((unsigned char*)link_list) + timestamp_offset,
           sizeof(uint64_t));
    return ts;
  }

  inline hnswlib::labeltype getExternalLabel(const void* data_level0) const;

  inline hnswlib::ResultItem<dist_t> getResultItem(
      const std::pair<dist_t, hnswlib::tableint>& rez,
      uint64_t file_number) const;

 private:
  HnswTableIterator* table_iterator_;
  Arena arena;
  std::shared_ptr<Cache> block_cache;

  size_t cur_element_count{0};  // current number of elements
  size_t size_links_per_element_{0};
  size_t M_{0};
  size_t maxM_{0};
  size_t maxM0_{0};
  size_t ef_construction_{0};
  size_t ef_{10};
  size_t offsetData_;
  unsigned int maxlevel_{0};
  hnswlib::tableint enterpoint_node_{0};

  char** linkLists_;

  size_t label_offset_{0}, version_offset_{0}, value_type_offset_{0},
      timestamp_offset{0};

  size_t visit_list_pool_size_{1};
  hnswlib::VisitedListPool* visited_list_pool_;

  hnswlib::DISTFUNC<dist_t> fstdistfunc_;
  size_t dist_func_param_;
  std::string table_name_;

  bool has_deletions{true};
  //  mutable std::atomic<size_t> metric_distance_computations{0};
  //  mutable std::atomic<long> metric_hops{0};
  //  size_t approximate_init_memory_usage;
  float calDist(const void* query_data, hnswlib::tableint internal_id,
                Cache::Priority priority = Cache::Priority::BOTTOM) const;
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
