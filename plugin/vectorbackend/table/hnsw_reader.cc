//
// Created by shb on 23-8-30.
//

#include "hnsw_reader.h"

#include "hnsw_table_factory.h"
#include "hnsw_table_iterator.h"
#include "hnsw_table_reader.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswalg.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/space_ip.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/space_l2.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/visited_list_pool.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
using namespace hnswlib;
using ResultItem = ResultItem<dist_t>;
using KnnResult = KnnResult<dist_t>;

template <typename T>
static const char* readBinaryPOD(const char* data, T& podRef) {
  podRef = *reinterpret_cast<const T*>(data);
  return data + sizeof(T);
}

HnswReader::HnswReader(HnswTable* table, const ReadOptions& ro,
                       CachableEntry<Block_kHnsw>* hnsw_block,
                       const HnswTableOptions& table_options,
                       size_t compaction_readahead_size,
                       bool /* allow_unprepared_value */)
    : block_cache(table_options.block_cache),
      table_name_(
          table->get_rep()->base_cache_key.CommonPrefixSlice().ToString()) {
  switch (table_options.space) {
    case L2: {
      L2Space space(table_options.dim);
      fstdistfunc_ = space.get_dist_func();
      dist_func_param_ = space.get_dist_func_param();
      break;
    }
    case IP: {
      InnerProductSpace space(table_options.dim);
      fstdistfunc_ = space.get_dist_func();
      dist_func_param_ = space.get_dist_func_param();
      break;
    }
  }

  const char* data = hnsw_block->GetValue()->data();
  data = readBinaryPOD(data, cur_element_count);
  data = readBinaryPOD(data, maxlevel_);
  data = readBinaryPOD(data, enterpoint_node_);
  data = readBinaryPOD(data, M_);
  data = readBinaryPOD(data, ef_construction_);
  data = readBinaryPOD(data, visit_list_pool_size_);
  data = readBinaryPOD(data, has_deletions);

  maxM_ = M_;
  maxM0_ = M_ * 2;

  size_t size_links_level0_ =
      maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
  version_offset_ = size_links_level0_ + sizeof(labeltype);
  value_type_offset_ = version_offset_ + 1;
  label_offset_ = size_links_level0_;
  timestamp_offset = version_offset_ + sizeof(uint64_t);

  size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
  visited_list_pool_ =
      new VisitedListPool(visit_list_pool_size_, cur_element_count, &arena);

  table_iterator_ = reinterpret_cast<HnswTableIterator*>(table->NewIterator(
      ro, nullptr, &arena, true, TableReaderCaller::kUserIterator,
      compaction_readahead_size, false));

  linkLists_ = (char**)arena.AllocateAligned(sizeof(void*) * cur_element_count);
  if (linkLists_ == nullptr) {
    throw std::runtime_error("Not enough memory to allocate linkLists");
  }
  for (size_t i = 0; i < cur_element_count; ++i) {
    unsigned int linkListSize;
    data = readBinaryPOD(data, linkListSize);
    if (linkListSize == 0) {
      linkLists_[i] = nullptr;
    } else {
      linkLists_[i] = (char*)arena.AllocateAligned(linkListSize);
      if (linkLists_[i] == nullptr) {
        throw std::runtime_error("Not enough memory to allocate linkList");
      }
      memcpy(linkLists_[i], data, linkListSize);
      data += linkListSize;
    }
  }

  offsetData_ = M_ * 2 * sizeof(hnswlib::tableint) +
                sizeof(hnswlib::linklistsizeint) + sizeof(hnswlib::labeltype) +
                sizeof(SequenceNumber);

  //  approximate_init_memory_usage =
  //      15 * sizeof(size_t) + sizeof(int) + sizeof(tableint) +
  //      sizeof(has_deletions) + sizeof(dist_func_param_) +
  //      sizeof(fstdistfunc_) + sizeof(arena) + sizeof(table_iterator_) +
  //      sizeof(hnsw_block_) + arena.ApproximateMemoryUsage();
}

Status HnswReader::Create(HnswTable* table, const ReadOptions& ro,
                          CachableEntry<Block_kHnsw>* hnsw_block,
                          std::unique_ptr<HnswReader>* hnsw_reader,
                          size_t compaction_readahead_size,
                          bool allow_unprepared_value) {
  assert(table != nullptr);
  assert(table->get_rep());
  assert(hnsw_reader != nullptr);

  const HnswTable::Rep* const rep = table->get_rep();
  assert(rep != nullptr);

  *hnsw_reader = std::move(std::make_unique<HnswReader>(
      table, ro, hnsw_block, rep->table_options, compaction_readahead_size,
      allow_unprepared_value));

  return Status::OK();
}

inline float HnswReader::calDist(const void* query_data,
                                 hnswlib::tableint internal_id,
                                 Cache::Priority priority) const {
  Entry* entry = getEntryByInternalId(internal_id, priority);
  dist_t d = fstdistfunc_(query_data, entry->vector_, dist_func_param_);
  block_cache->Release(entry->cache_handle_);
  return d;
}

inline const float* HnswReader::getDataByInternalId(
    tableint internal_id) const {
  return getEntryByInternalId(internal_id)->vector_;
}

inline Entry* HnswReader::getEntryByInternalId(tableint internal_id,
                                               Cache::Priority priority) const {
  std::string cache_key = table_name_ + "#" + std::to_string(internal_id);
  auto cache_handle =
      block_cache->Lookup(cache_key, nullptr, nullptr, priority);
  if (cache_handle != nullptr) {
    auto* value = static_cast<Entry*>(block_cache->Value(cache_handle));
    assert(value->cache_handle_ == cache_handle);
    return value;
  }

  table_iterator_->Seek(internal_id, priority);
  return table_iterator_->EntryValue();
}

inline Entry* HnswReader::get_linklist0(hnswlib::tableint internal_id) const {
  return getEntryByInternalId(internal_id);
}

inline linklistsizeint* HnswReader::get_linklist(tableint internal_id,
                                                 unsigned int level) const {
  return (linklistsizeint*)(linkLists_[internal_id] +
                            (level - 1) * size_links_per_element_);
}

inline labeltype HnswReader::getExternalLabel(const void* data_level0) const {
  labeltype return_label;
  memcpy(&return_label, (((unsigned char*)data_level0) + label_offset_),
         sizeof(labeltype));
  return return_label;
}

inline ResultItem HnswReader::getResultItem(
    const std::pair<dist_t, tableint>& rez, uint64_t file_number) const {
  Entry* linklist0_ptr = get_linklist0(rez.second);
  const char* data_level0 = linklist0_ptr->data_;
  ResultItem item{rez.first, getExternalLabel(data_level0),
                  getVersion(data_level0), file_number};
  block_cache->Release(linklist0_ptr->cache_handle_);
  return item;
}

std::priority_queue<std::pair<dist_t, tableint>,
                    std::vector<std::pair<dist_t, tableint>>,
                    CompareByFirst<dist_t>>
HnswReader::searchBaseLayerST(tableint ep_id, const void* data_point, size_t ef,
                              const SequenceNumber* snapshot,
                              uint64_t timestamp) const {
  VisitedList* vl = visited_list_pool_->getFreeVisitedList();
  vl_type* visited_array = vl->mass;
  vl_type visited_array_tag = vl->curV;

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
      top_candidates;
  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
      candidate_set;

  dist_t lowerBound;
  Entry* ep_entry = getEntryByInternalId(ep_id);
  if ((!has_deletions || !isMarkedDeleted(ep_entry->data_)) &&
      ((!snapshot) || getVersion(ep_entry->data_) <= *snapshot) &&
      (getTimestamp(ep_entry->data_) >= timestamp)) {
    dist_t dist = fstdistfunc_(data_point, ep_entry->vector_, dist_func_param_);
    lowerBound = dist;
    top_candidates.emplace(dist, ep_id);
    candidate_set.emplace(-dist, ep_id);
  } else {
    lowerBound = std::numeric_limits<dist_t>::max();
    candidate_set.emplace(-lowerBound, ep_id);
  }

  block_cache->Release(ep_entry->cache_handle_);

  visited_array[ep_id] = visited_array_tag;

  while (!candidate_set.empty()) {
    std::pair<dist_t, tableint> current_node_pair = candidate_set.top();

    if ((-current_node_pair.first) > lowerBound &&
        (top_candidates.size() == ef || (!snapshot && !has_deletions))) {
      break;
    }
    candidate_set.pop();

    tableint current_node_id = current_node_pair.second;
    Entry* linklist0_ptr = get_linklist0(current_node_id);
    int* data = (int*)linklist0_ptr->data_;
    size_t size = getListCount(data);
    //                bool cur_node_deleted =
    //                isMarkedDeleted(current_node_id);

#ifdef USE_SSE
    _mm_prefetch((void*)((char*)visited_array + *(data + 1)), _MM_HINT_T0);
    _mm_prefetch((void*)((char*)visited_array + *(data + 1) + 64), _MM_HINT_T0);
    _mm_prefetch((void*)(data + 2), _MM_HINT_T0);
#endif

    for (size_t j = 1; j <= size; j++) {
      int candidate_id = *(data + j);
      //                    if (candateid_id == 0) continue;
      if (visited_array[candidate_id] != visited_array_tag) {
        visited_array[candidate_id] = visited_array_tag;

        Entry* currObj1 = getEntryByInternalId(candidate_id);

        auto currObj1_data = currObj1->data_;

        dist_t dist =
            fstdistfunc_(data_point, currObj1->vector_, dist_func_param_);

        if (top_candidates.size() < ef || lowerBound > dist) {
          candidate_set.emplace(-dist, candidate_id);

          if ((!has_deletions || !isMarkedDeleted(currObj1_data)) &&
              ((!snapshot) || getVersion(currObj1_data) >= *snapshot) &&
              (getTimestamp(currObj1_data) >= timestamp))
            top_candidates.emplace(dist, candidate_id);

          if (top_candidates.size() > ef) top_candidates.pop();

          if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
        }

        block_cache->Release(currObj1->cache_handle_);
      }
    }

    block_cache->Release(linklist0_ptr->cache_handle_);
  }

  visited_list_pool_->releaseVisitedList(vl);
  return top_candidates;
}

HnswReader::~HnswReader() { delete visited_list_pool_; }

KnnResult HnswReader::searchKnn(const void* query_data, size_t k, uint64_t ts,
                                const SequenceNumber* snapshot,
                                uint64_t file_number) const {
  KnnResult result;
  if (cur_element_count == 0) return result;

  tableint currObj = enterpoint_node_;
  dist_t curdist = calDist(query_data, enterpoint_node_, Cache::Priority::HIGH);

  for (unsigned int level = maxlevel_; level > 0; --level) {
    bool changed = true;
    while (changed) {
      changed = false;
      unsigned int* data;

      data = (unsigned int*)get_linklist(currObj, level);
      int size = getListCount(data);

      auto* datal = reinterpret_cast<tableint*>(data + 1);
      for (int i = 0; i < size; ++i) {
        tableint cand = datal[i];
        if (cand > cur_element_count) throw std::runtime_error("cand error");
        dist_t d = calDist(query_data, cand, Cache::Priority::LOW);

        if (d < curdist) {
          curdist = d;
          currObj = cand;
          changed = true;
        }
      }
    }
  }

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
      top_candidates;
  top_candidates =
      searchBaseLayerST(currObj, query_data, std::max(ef_, k), snapshot, ts);
  while (top_candidates.size() > k) {
    top_candidates.pop();
  }
  while (!top_candidates.empty()) {
    std::pair<dist_t, tableint> rez = top_candidates.top();
    result.push(getResultItem(rez, file_number));
    top_candidates.pop();
  }
  return result;
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
