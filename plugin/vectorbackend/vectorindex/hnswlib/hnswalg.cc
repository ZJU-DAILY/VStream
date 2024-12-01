//
// Created by shb on 23-10-29.
//

#include "hnswalg.h"

#include "db/dbformat.h"
#include "memory/allocator.h"
#include "space_ip.h"
#include "space_l2.h"

namespace hnswlib {
template <typename dist_t>
HierarchicalNSW<dist_t>::HierarchicalNSW(
    size_t dim, SpaceType s, ROCKSDB_NAMESPACE::Allocator *allocator,
    size_t max_elements, size_t M, size_t ef_construction, size_t random_seed,
    size_t visit_list_pool_size, bool allow_replace_deleted)
    : max_elements_(max_elements),
      cur_element_count(0),
      num_deleted_(0),
      M_(M),
      maxM_(M_),
      maxM0_(M_ * 2),
      ef_construction_(std::max(ef_construction, M_)),
      ef_(10),
      label_op_locks_(MAX_LABEL_OPERATION_LOCKS),
      link_list_locks_(max_elements),
      element_levels_(max_elements),
      allow_replace_deleted_(allow_replace_deleted),
      allocator_(allocator) {
  switch (s) {
    case L2: {
      L2Space space(dim);
      data_size_ = space.get_data_size();
      fstdistfunc_ = space.get_dist_func();
      dist_func_param_ = space.get_dist_func_param();
      break;
    }
    case IP: {
      InnerProductSpace space(dim);
      data_size_ = space.get_data_size();
      fstdistfunc_ = space.get_dist_func();
      dist_func_param_ = space.get_dist_func_param();
      break;
    }
  }
  level_generator_.seed(random_seed);
  update_probability_generator_.seed(random_seed + 1);

  size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
  version_offset_ = size_links_level0_ + sizeof(labeltype);
  value_type_offset_ = version_offset_ + 1;
  timestamp_offset_ = version_offset_ + sizeof(SequenceNumber);
  size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype) +
                           sizeof(SequenceNumber) + sizeof(uint64_t);
  offsetData_ = timestamp_offset_ + sizeof(uint64_t);
  label_offset_ = size_links_level0_;

  data_level0_memory_ =
      allocator_->AllocateAligned(max_elements_ * size_data_per_element_);
  if (data_level0_memory_ == nullptr)
    throw std::runtime_error("Not enough memory");

  visit_list_pool_size_ = visit_list_pool_size,

  visited_list_pool_ = std::make_unique<VisitedListPool>(
      visit_list_pool_size, max_elements, allocator_);

  // initializations for special treatment of the first node
  enterpoint_node_ = -1;
  maxlevel_ = -1;

  linkLists_ =
      (char **)allocator_->AllocateAligned(sizeof(void *) * max_elements_);
  if (linkLists_ == nullptr)
    throw std::runtime_error(
        "Not enough memory: HierarchicalNSW failed to allocate linklists");
  size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
  mult_ = 1 / log(1.0 * M_);
  revSize_ = 1.0 / mult_;

  approximate_init_memory_usage =
      8 + sizeof(cur_element_count) + 8 + 8 + sizeof(num_deleted_) + 7 * 8 + 4 +
      8 + sizeof(*visited_list_pool_) +
      visit_list_pool_size_ * (8 + sizeof(VisitedList)) +
      sizeof(label_op_locks_) + MAX_LABEL_OPERATION_LOCKS * sizeof(std::mutex) +
      sizeof(global) + sizeof(link_list_locks_) +
      max_elements_ * sizeof(std::mutex) + 4 + 6 * 8 + sizeof(element_levels_) +
      max_elements * 4 + 3 * 8 + sizeof(label_lookup_lock) +
      sizeof(label_lookup_) + sizeof(level_generator_) +
      sizeof(update_probability_generator_) +
      sizeof(metric_distance_computations) + sizeof(metric_hops) + 2 +
      sizeof(deleted_elements_lock) + sizeof(deleted_elements) + 8 + 8 +
      sizeof(*allocator) + 8;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::saveIndex(std::string *output) {
  output->reserve(120 + cur_element_count * size_links_per_element_ * mult_);

  ROCKSDB_NAMESPACE::PutFixed64(output, cur_element_count);  // size_t
  ROCKSDB_NAMESPACE::PutFixed32(output, maxlevel_);          // unsigned int
  ROCKSDB_NAMESPACE::PutFixed32(output,
                                enterpoint_node_);          // unsigned int
  ROCKSDB_NAMESPACE::PutFixed64(output, M_);                // size_t
  ROCKSDB_NAMESPACE::PutFixed64(output, ef_construction_);  // size_t
  ROCKSDB_NAMESPACE::PutFixed64(output,
                                visit_list_pool_size_);  // size_t
  bool has_deletions = num_deleted_ > 0;
  output->append(
      const_cast<const char *>(reinterpret_cast<char *>(&has_deletions)),
      sizeof(has_deletions));  // bool

  for (size_t i = 0; i < cur_element_count; i++) {
    unsigned int linkListSize =
        element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i]
                               : 0;
    ROCKSDB_NAMESPACE::PutFixed32(output,
                                  linkListSize);  // unsigned int
    if (linkListSize) {
      output->append(linkLists_[i],
                     linkListSize);  // linkListSize = size_links_per_element_ *
                                     // element_levels_[i]
    }
  }
}

template <typename dist_t>
char *HierarchicalNSW<dist_t>::getDataByLabel(labeltype label) const {
  // lock all operations with element by label
  std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label),
                                          std::defer_lock);
  if (!readonly_) {
    lock_label.lock();
  }
  tableint internalId;
  {
    std::shared_lock<std::shared_mutex> rw_lock(label_lookup_lock,
                                                std::defer_lock);
    if (!readonly_) {
      rw_lock.lock();
    }
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
      throw std::runtime_error("Label not found");
    }
    internalId = search->second;
  }
  return getDataByInternalId(internalId);
}

template <typename dist_t>
bool HierarchicalNSW<dist_t>::contains(labeltype label) const {
  std::shared_lock<std::shared_mutex> read_lock(label_lookup_lock,
                                                std::defer_lock);
  if (!readonly_) {
    read_lock.lock();
  }
  auto search = label_lookup_.find(label);
  if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
    return false;
  }
  return true;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::updatePoint(const void *dataPoint,
                                          tableint internalId,
                                          float updateNeighborProbability,
                                          uint64_t version, uint64_t timestamp,
                                          bool concurrent) {
  // update the feature vector associated with existing point with new
  // vector and update the version
  memcpy(getDataByInternalId(internalId), dataPoint, data_size_);
  setVersionWithType(internalId, version,
                     ROCKSDB_NAMESPACE::ValueType::kTypeValue);
  setTimestamp(internalId, timestamp);

  int maxLevelCopy = maxlevel_;
  tableint entryPointCopy = enterpoint_node_;
  // If point to be updated is entry point and graph just contains single
  // element then just return.
  if (entryPointCopy == internalId && cur_element_count == 1) return;

  int elemLevel = element_levels_[internalId];
  std::uniform_real_distribution<float> distribution(0.0, 1.0);
  for (int layer = 0; layer <= elemLevel; layer++) {
    std::unordered_set<tableint> sCand;
    std::unordered_set<tableint> sNeigh;
    std::vector<tableint> listOneHop =
        getConnections(internalId, layer, concurrent);
    if (listOneHop.empty()) continue;

    sCand.insert(internalId);

    for (auto &&elOneHop : listOneHop) {
      sCand.insert(elOneHop);

      if (distribution(update_probability_generator_) >
          updateNeighborProbability)
        continue;

      sNeigh.insert(elOneHop);

      std::vector<tableint> listTwoHop =
          getConnections(elOneHop, layer, concurrent);
      for (auto &&elTwoHop : listTwoHop) {
        sCand.insert(elTwoHop);
      }
    }

    for (auto &&neigh : sNeigh) {
      // if (neigh == internalId)
      //     continue;

      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst<dist_t>>
          candidates;
      size_t size =
          sCand.find(neigh) == sCand.end()
              ? sCand.size()
              : sCand.size() - 1;  // sCand guaranteed to have size >= 1
      size_t elementsToKeep = std::min(ef_construction_, size);
      for (auto &&cand : sCand) {
        if (cand == neigh) continue;

        dist_t distance =
            fstdistfunc_(getDataByInternalId(neigh), getDataByInternalId(cand),
                         dist_func_param_);
        if (candidates.size() < elementsToKeep) {
          candidates.emplace(distance, cand);
        } else {
          if (distance < candidates.top().first) {
            candidates.pop();
            candidates.emplace(distance, cand);
          }
        }
      }

      // Retrieve neighbours using heuristic and set connections.
      getNeighborsByHeuristic2(candidates, layer == 0 ? maxM0_ : maxM_);

      {
        std::unique_lock<std::mutex> lock(link_list_locks_[neigh],
                                          std::defer_lock);
        if (concurrent) {
          lock.lock();
        }
        linklistsizeint *ll_cur;
        ll_cur = get_linklist_at_level(neigh, layer);
        size_t candSize = candidates.size();
        setListCount(ll_cur, candSize);
        auto *data = (tableint *)(ll_cur + 1);
        for (size_t idx = 0; idx < candSize; idx++) {
          data[idx] = candidates.top().second;
          candidates.pop();
        }
      }
    }
  }

  repairConnectionsForUpdate(dataPoint, entryPointCopy, internalId, elemLevel,
                             maxLevelCopy, concurrent);
}

template <typename dist_t>
std::vector<tableint> HierarchicalNSW<dist_t>::getConnections(
    tableint internalId, int level, bool concurrent) {
  std::unique_lock<std::mutex> lock(link_list_locks_[internalId],
                                    std::defer_lock);
  if (concurrent) {
    lock.lock();
  }
  unsigned int *data = get_linklist_at_level(internalId, level);
  int size = getListCount(data);
  std::vector<tableint> result(size);
  auto *ll = (tableint *)(data + 1);
  memcpy(result.data(), ll, size * sizeof(tableint));
  return result;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::repairConnectionsForUpdate(
    const void *dataPoint, tableint entryPointInternalId,
    tableint dataPointInternalId, int dataPointLevel, int maxLevel,
    bool concurrent) {
  tableint currObj = entryPointInternalId;
  if (dataPointLevel < maxLevel) {
    dist_t curdist =
        fstdistfunc_(dataPoint, getDataByInternalId(currObj), dist_func_param_);
    for (int level = maxLevel; level > dataPointLevel; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        unsigned int *data;
        std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
        data = get_linklist_at_level(currObj, level);
        int size = getListCount(data);
        auto *datal = (tableint *)(data + 1);
#ifdef USE_SSE
        _mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
#endif
        for (int i = 0; i < size; i++) {
#ifdef USE_SSE
          _mm_prefetch(getDataByInternalId(*(datal + i + 1)), _MM_HINT_T0);
#endif
          tableint cand = datal[i];
          dist_t d = fstdistfunc_(dataPoint, getDataByInternalId(cand),
                                  dist_func_param_);
          if (d < curdist) {
            curdist = d;
            currObj = cand;
            changed = true;
          }
        }
      }
    }
  }

  if (dataPointLevel > maxLevel)
    throw std::runtime_error(
        "Level of item to be updated cannot be bigger than max level");

  for (int level = dataPointLevel; level >= 0; level--) {
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst<dist_t>>
        topCandidates = searchBaseLayer(currObj, dataPoint, level, concurrent);

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst<dist_t>>
        filteredTopCandidates;
    while (topCandidates.size() > 0) {
      if (topCandidates.top().second != dataPointInternalId)
        filteredTopCandidates.push(topCandidates.top());

      topCandidates.pop();
    }

    // Since element_levels_ is being used to get `dataPointLevel`, there
    // could be cases where `topCandidates` could just contains entry point
    // itself. To prevent self loops, the `topCandidates` is filtered and
    // thus can be empty.
    if (filteredTopCandidates.size() > 0) {
      bool epDeleted = isMarkedDeleted(entryPointInternalId);
      if (epDeleted) {
        filteredTopCandidates.emplace(
            fstdistfunc_(dataPoint, getDataByInternalId(entryPointInternalId),
                         dist_func_param_),
            entryPointInternalId);
        if (filteredTopCandidates.size() > ef_construction_)
          filteredTopCandidates.pop();
      }

      currObj = mutuallyConnectNewElement(dataPoint, dataPointInternalId,
                                          filteredTopCandidates, level, true,
                                          concurrent);
    }
  }
}

template <typename dist_t>
tableint HierarchicalNSW<dist_t>::addPointInternal(
    const void *data_point, labeltype label, tableint internalId,
    uint64_t version, uint64_t timestamp, bool concurrent) {
  std::unique_lock<std::mutex> lock_el(link_list_locks_[internalId]);
  int curlevel = getRandomLevel(mult_);

  element_levels_[internalId] = curlevel;

  std::unique_lock<std::mutex> templock(global, std::defer_lock);
  if (concurrent) {
    templock.lock();
  }

  int maxlevelcopy = maxlevel_;
  if (curlevel <= maxlevelcopy && concurrent) templock.unlock();
  tableint currObj = enterpoint_node_;
  tableint enterpoint_copy = enterpoint_node_;

  memset(data_level0_memory_ + internalId * size_data_per_element_, 0,
         size_data_per_element_);

  // Initialisation of the data and label and version
  memcpy(getDataByInternalId(internalId), data_point, data_size_);
  // memcpy(getLabelType(internalId), &label, sizeof(labeltype));
  setExternalLabel(internalId, label);
  setVersionWithType(internalId, version,
                     ROCKSDB_NAMESPACE::ValueType::kTypeValue);
  setTimestamp(internalId, timestamp);

  if (curlevel) {
    linkLists_[internalId] =
        allocator_->AllocateAligned(size_links_per_element_ * curlevel + 1);
    if (linkLists_[internalId] == nullptr)
      throw std::runtime_error(
          "Not enough memory: addPoint failed to allocate linklist");
    memset(linkLists_[internalId], 0, size_links_per_element_ * curlevel + 1);
  }

  if ((signed)currObj != -1) {
    if (curlevel < maxlevelcopy) {
      dist_t curdist = fstdistfunc_(data_point, getDataByInternalId(currObj),
                                    dist_func_param_);
      for (int _level = maxlevelcopy; _level > curlevel; _level--) {
        bool changed = true;
        while (changed) {
          changed = false;
          unsigned int *data;
          std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
          data = get_linklist(currObj, _level);
          int size = getListCount(data);

          auto *datal = (tableint *)(data + 1);
          for (int i = 0; i < size; i++) {
            tableint cand = datal[i];
            if (cand < 0 || cand > max_elements_)
              throw std::runtime_error("cand error");
            dist_t d = fstdistfunc_(data_point, getDataByInternalId(cand),
                                    dist_func_param_);
            if (d < curdist) {
              curdist = d;
              currObj = cand;
              changed = true;
            }
          }
        }
      }
    }

    bool epDeleted = isMarkedDeleted(enterpoint_copy);
    for (int _level = std::min(curlevel, maxlevelcopy); _level >= 0; _level--) {
      if (_level > maxlevelcopy || _level < 0)  // possible?
        throw std::runtime_error("Level error");

      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst<dist_t>>
          top_candidates =
              searchBaseLayer(currObj, data_point, _level, concurrent);
      if (epDeleted) {
        top_candidates.emplace(
            fstdistfunc_(data_point, getDataByInternalId(enterpoint_copy),
                         dist_func_param_),
            enterpoint_copy);
        if (top_candidates.size() > ef_construction_) top_candidates.pop();
      }
      currObj = mutuallyConnectNewElement(
          data_point, internalId, top_candidates, _level, false, concurrent);
    }
  } else {
    // Do nothing for the first element
    enterpoint_node_ = 0;
    maxlevel_ = curlevel;
  }

  // Releasing lock for the maximum level
  if (curlevel > maxlevelcopy) {
    enterpoint_node_ = internalId;
    maxlevel_ = curlevel;
  }
  return internalId;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::unmarkDelete(labeltype label, bool concurrent) {
  // lock all operations with element by label
  std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

  tableint internalId;
  {
    std::shared_lock<std::shared_mutex> table_read_lock(label_lookup_lock,
                                                        std::defer_lock);
    if (concurrent) {
      table_read_lock.lock();
    }
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      throw std::runtime_error("Label not found");
    }
    internalId = search->second;
  }

  unmarkDeletedInternal(internalId, false, concurrent);
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::unmarkDeletedInternal(tableint internalId,
                                                    bool force,
                                                    bool concurrent) {
  assert(internalId < cur_element_count);
  if (force || isMarkedDeleted(internalId)) {
    setValueType(internalId, ROCKSDB_NAMESPACE::ValueType::kTypeValue);
    num_deleted_ -= 1;
    if (allow_replace_deleted_) {
      std::unique_lock<std::mutex> lock_deleted_elements(deleted_elements_lock,
                                                         std::defer_lock);
      if (concurrent) {
        lock_deleted_elements.lock();
      }
      deleted_elements.erase(internalId);
    }
  } else {
    throw std::runtime_error(
        "The requested to undelete element is not deleted");
  }
}

template <typename dist_t>
inline bool HierarchicalNSW<dist_t>::isMarkedDeleted(
    tableint internalId) const {
  unsigned char *ll_cur =
      ((unsigned char *)get_linklist0(internalId)) + value_type_offset_;
  return *ll_cur == ROCKSDB_NAMESPACE::ValueType::kTypeDeletion;
}

template <typename dist_t>
inline void HierarchicalNSW<dist_t>::setValueType(
    tableint internalId, ROCKSDB_NAMESPACE::ValueType type) {
  unsigned char *ll_cur =
      ((unsigned char *)get_linklist0(internalId)) + value_type_offset_;
  memcpy(ll_cur, &type, sizeof(type));
}

template <typename dist_t>
inline uint64_t HierarchicalNSW<dist_t>::getVersion(tableint internalId) const {
  uint64_t version;
  memcpy(&version,
         ((unsigned char *)get_linklist0(internalId)) + version_offset_,
         sizeof(SequenceNumber) - sizeof(ROCKSDB_NAMESPACE::ValueType));
  return version >> 8;
}

template <typename dist_t>
inline uint64_t HierarchicalNSW<dist_t>::getVersionWithType(
    tableint internalId) const {
  uint64_t version;
  memcpy(&version,
         ((unsigned char *)get_linklist0(internalId)) + version_offset_,
         sizeof(SequenceNumber) - sizeof(ROCKSDB_NAMESPACE::ValueType));
  return version;
}

template <typename dist_t>
inline uint64_t HierarchicalNSW<dist_t>::getTimestamp(
    tableint internalId) const {
  uint64_t version;
  memcpy(&version,
         ((unsigned char *)get_linklist0(internalId)) + timestamp_offset_,
         sizeof(uint64_t));
  return version;
}

template <typename dist_t>
inline void HierarchicalNSW<dist_t>::setVersionWithType(
    tableint internalId, uint64_t version, ROCKSDB_NAMESPACE::ValueType type) {
  uint64_t version_with_type = (version << 8) + type;
  memcpy(((unsigned char *)get_linklist0(internalId)) + version_offset_,
         &version_with_type, sizeof(SequenceNumber));
}

template <typename dist_t>
inline void HierarchicalNSW<dist_t>::setTimestamp(tableint internalId,
                                                  uint64_t timestamp) {
  memcpy(((unsigned char *)get_linklist0(internalId)) + timestamp_offset_,
         &timestamp, sizeof(uint64_t));
}

template <typename dist_t>
unsigned short int HierarchicalNSW<dist_t>::getListCount(
    const linklistsizeint *ptr) const {
  return *((unsigned short int *)ptr);
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::setListCount(linklistsizeint *ptr,
                                           unsigned short int size) const {
  *((unsigned short int *)(ptr)) = *((unsigned short int *)&size);
}

template <typename dist_t>
inline tableint HierarchicalNSW<dist_t>::allocateInternalId(labeltype label,
                                                            bool concurrent) {
  std::unique_lock<std::shared_mutex> table_write_lock(label_lookup_lock,
                                                       std::defer_lock);
  if (concurrent) {
    table_write_lock.lock();
  }
  if (cur_element_count >= max_elements_) {
    throw std::runtime_error(
        "The number of elements exceeds the specified limit");
  }
  tableint internalId = cur_element_count;
  cur_element_count++;
  label_lookup_[label] = internalId;
  return internalId;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::markDelete(labeltype label, uint64_t version,
                                         bool concurrent) {
  // lock all operations with element by label
  std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

  tableint internalId;
  {
    std::unique_lock<std::shared_mutex> write_lock(label_lookup_lock,
                                                   std::defer_lock);
    if (concurrent) {
      write_lock.lock();
    }
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      return;
    }
    if (version < getVersion(search->second)) {
      return;
    }
    label_lookup_.erase(search);
    internalId = search->second;
  }

  markDeletedInternal(internalId, version);
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::markDeletedInternal(tableint internalId,
                                                  uint64_t version) {
  assert(internalId < cur_element_count);
  if (!isMarkedDeleted(internalId)) {
    setValueType(internalId, ROCKSDB_NAMESPACE::ValueType::kTypeDeletion);
    num_deleted_ += 1;
    if (allow_replace_deleted_) {
      std::unique_lock<std::mutex> lock_deleted_elements(deleted_elements_lock);
      deleted_elements.insert(internalId);
    }
  } else {
    //      throw std::runtime_error("The requested to delete element is
    //      already deleted");
  }
}

template <typename dist_t>
bool HierarchicalNSW<dist_t>::addPoint(const void *data_point, labeltype label,
                                       uint64_t version, uint64_t timestamp,
                                       bool replace_deleted,
                                       bool allow_duplicate, bool concurrent) {
  if (!allow_replace_deleted_ && replace_deleted) {
    throw std::runtime_error(
        "Replacement of deleted elements is disabled in constructor");
  }

  // lock all operations with element by label
  std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label),
                                          std::defer_lock);
  if (concurrent) {
    lock_label.lock();
  }

  tableint internalId;
  bool existing = false;
  {  // Checking if the element with the same label already exists
    // if so, updating it *instead* of creating a new element.
    std::shared_lock<std::shared_mutex> table_read_lock(label_lookup_lock,
                                                        std::defer_lock);
    if (concurrent) {
      table_read_lock.lock();
    }
    auto search = label_lookup_.find(label);
    if (search != label_lookup_.end()) {
      existing = true;
      internalId = search->second;
    }
  }

  if (existing) {
    if (version < getVersion(internalId)) {
      return true;
    }

    if (memcmp(data_point, getDataByInternalId(internalId), data_size_) == 0) {
      return allow_duplicate || version != getVersion(internalId);
    }

    if (!replace_deleted) {
      internalId = allocateInternalId(label, concurrent);
      addPointInternal(data_point, label, internalId, version, timestamp,
                       concurrent);
      return true;
    }

    if (isMarkedDeleted(internalId)) {
      unmarkDeletedInternal(internalId, true, concurrent);
    } else {
      setValueType(internalId, ROCKSDB_NAMESPACE::ValueType::kTypeValue);
    }
    updatePoint(data_point, internalId, 1.0, version, timestamp, concurrent);
    return true;
  }

  // check if there is vacant place
  bool is_vacant_place;
  {
    std::unique_lock<std::mutex> lock_deleted_elements(deleted_elements_lock,
                                                       std::defer_lock);
    if (concurrent) {
      lock_deleted_elements.lock();
    }
    is_vacant_place = !deleted_elements.empty();
    if (is_vacant_place) {
      internalId = *deleted_elements.begin();
      deleted_elements.erase(internalId);
    }
  }

  // if there is no vacant place then add or update point
  // else add point to vacant place
  if (!is_vacant_place) {
    internalId = allocateInternalId(label, concurrent);
    addPointInternal(data_point, label, internalId, version, timestamp,
                     concurrent);
  } else {
    // we assume that there are no concurrent operations on deleted element
    labeltype label_replaced = getExternalLabel(internalId);
    setExternalLabel(internalId, label);

    {
      std::unique_lock<std::shared_mutex> table_write_lock(label_lookup_lock,
                                                           std::defer_lock);
      if (concurrent) {
        table_write_lock.lock();
      }
      label_lookup_.erase(label_replaced);
      label_lookup_.insert(std::make_pair(label, internalId));
    }

    unmarkDeletedInternal(internalId, false, concurrent);
    updatePoint(data_point, internalId, 1.0, version, timestamp, concurrent);
  }
  return true;
}

VisitedList::VisitedList(size_t numelements1,
                         ROCKSDB_NAMESPACE::Allocator *allocator) {
  curV = -1;
  numelements = numelements1;
  mass = (vl_type *)allocator->AllocateAligned(numelements * sizeof(vl_type));
}

template <typename dist_t>
KnnResult<dist_t> HierarchicalNSW<dist_t>::searchKnn(
    const void *query_data, size_t k, BaseFilterFunctor *isIdAllowed) const {
  KnnResult<dist_t> result;
  if (cur_element_count == 0) return result;

  tableint currObj = enterpoint_node_;
  dist_t curdist = fstdistfunc_(
      query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);

  for (int level = maxlevel_; level > 0; level--) {
    bool changed = true;
    while (changed) {
      changed = false;
      unsigned int *data;

      data = (unsigned int *)get_linklist(currObj, level);
      int size = getListCount(data);
      metric_hops++;
      metric_distance_computations += size;

      auto *datal = (tableint *)(data + 1);
      for (int i = 0; i < size; i++) {
        tableint cand = datal[i];
        if (cand < 0 || cand > max_elements_)
          throw std::runtime_error("cand error");
        dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand),
                                dist_func_param_);

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
  if (num_deleted_) {
    top_candidates = searchBaseLayerST(currObj, query_data, std::max(ef_, k),
                                       true, true, isIdAllowed);
  } else {
    top_candidates = searchBaseLayerST(currObj, query_data, std::max(ef_, k),
                                       false, true, isIdAllowed);
  }

  while (top_candidates.size() > k) {
    top_candidates.pop();
  }
  while (top_candidates.size() > 0) {
    std::pair<dist_t, tableint> rez = top_candidates.top();
    result.emplace(rez.first, getExternalLabel(rez.second),
                   getVersion(rez.second));
    top_candidates.pop();
  }
  return result;
}

template <typename dist_t>
std::priority_queue<std::pair<dist_t, tableint>,
                    std::vector<std::pair<dist_t, tableint>>,
                    CompareByFirst<dist_t>>
HierarchicalNSW<dist_t>::searchBaseLayer(tableint ep_id, const void *data_point,
                                         int layer, bool concurrent) {
  VisitedList *vl = visited_list_pool_->getFreeVisitedList();
  vl_type *visited_array = vl->mass;
  vl_type visited_array_tag = vl->curV;

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
      top_candidates;
  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
      candidateSet;

  dist_t lowerBound;
  if (!isMarkedDeleted(ep_id)) {
    dist_t dist =
        fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
    top_candidates.emplace(dist, ep_id);
    lowerBound = dist;
    candidateSet.emplace(-dist, ep_id);
  } else {
    lowerBound = std::numeric_limits<dist_t>::max();
    candidateSet.emplace(-lowerBound, ep_id);
  }
  visited_array[ep_id] = visited_array_tag;

  while (!candidateSet.empty()) {
    std::pair<dist_t, tableint> curr_el_pair = candidateSet.top();
    if ((-curr_el_pair.first) > lowerBound &&
        top_candidates.size() == ef_construction_) {
      break;
    }
    candidateSet.pop();

    tableint curNodeNum = curr_el_pair.second;

    std::unique_lock<std::mutex> lock(link_list_locks_[curNodeNum],
                                      std::defer_lock);
    if (concurrent) {
      lock.lock();
    }

    int *data;  // = (int *)(linkList0_ + curNodeNum *
                // size_links_per_element0_);
    if (layer == 0) {
      data = (int *)get_linklist0(curNodeNum);
    } else {
      data = (int *)get_linklist(curNodeNum, layer);
      //                    data = (int *) (linkLists_[curNodeNum] + (layer -
      //                    1) * size_links_per_element_);
    }
    size_t size = getListCount((linklistsizeint *)data);
    auto *datal = (tableint *)(data + 1);
#ifdef USE_SSE
    _mm_prefetch((void *)((char *)visited_array + *(data + 1)), _MM_HINT_T0);
    _mm_prefetch((void *)((char *)visited_array + *(data + 1) + 64),
                 _MM_HINT_T0);
    _mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
    _mm_prefetch(getDataByInternalId(*(datal + 1)), _MM_HINT_T0);
#endif

    for (size_t j = 0; j < size; j++) {
      tableint candidate_id = *(datal + j);
//                    if (candidate_id == 0) continue;
#ifdef USE_SSE
      _mm_prefetch((void *)((char *)visited_array + *(datal + j + 1)),
                   _MM_HINT_T0);
      _mm_prefetch(getDataByInternalId(*(datal + j + 1)), _MM_HINT_T0);
#endif
      if (visited_array[candidate_id] == visited_array_tag) continue;
      visited_array[candidate_id] = visited_array_tag;
      char *currObj1 = (getDataByInternalId(candidate_id));

      dist_t dist1 = fstdistfunc_(data_point, currObj1, dist_func_param_);
      if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
        candidateSet.emplace(-dist1, candidate_id);
#ifdef USE_SSE
        _mm_prefetch(getDataByInternalId(candidateSet.top().second),
                     _MM_HINT_T0);
#endif

        if (!isMarkedDeleted(candidate_id))
          top_candidates.emplace(dist1, candidate_id);

        if (top_candidates.size() > ef_construction_) top_candidates.pop();

        if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
      }
    }
  }
  visited_list_pool_->releaseVisitedList(vl);

  return top_candidates;
}

template <typename dist_t>
std::priority_queue<std::pair<dist_t, tableint>,
                    std::vector<std::pair<dist_t, tableint>>,
                    CompareByFirst<dist_t>>
HierarchicalNSW<dist_t>::searchBaseLayerST(
    tableint ep_id, const void *data_point, size_t ef, bool has_deletions,
    bool collect_metrics, BaseFilterFunctor *isIdAllowed) const {
  VisitedList *vl = visited_list_pool_->getFreeVisitedList();
  vl_type *visited_array = vl->mass;
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
  if ((!has_deletions || !isMarkedDeleted(ep_id)) &&
      ((!isIdAllowed) ||
       (*isIdAllowed)(getExternalLabel(ep_id), getVersion(ep_id),
                      getTimestamp(ep_id)))) {
    dist_t dist =
        fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
    lowerBound = dist;
    top_candidates.emplace(dist, ep_id);
    candidate_set.emplace(-dist, ep_id);
  } else {
    lowerBound = std::numeric_limits<dist_t>::max();
    candidate_set.emplace(-lowerBound, ep_id);
  }

  visited_array[ep_id] = visited_array_tag;

  while (!candidate_set.empty()) {
    std::pair<dist_t, tableint> current_node_pair = candidate_set.top();

    if ((-current_node_pair.first) > lowerBound &&
        (top_candidates.size() == ef || (!isIdAllowed && !has_deletions))) {
      break;
    }
    candidate_set.pop();

    tableint current_node_id = current_node_pair.second;
    int *data = (int *)get_linklist0(current_node_id);
    size_t size = getListCount((linklistsizeint *)data);
    //                bool cur_node_deleted =
    //                isMarkedDeleted(current_node_id);
    if (collect_metrics) {
      metric_hops++;
      metric_distance_computations += size;
    }

#ifdef USE_SSE
    _mm_prefetch((void *)((char *)visited_array + *(data + 1)), _MM_HINT_T0);
    _mm_prefetch((void *)((char *)visited_array + *(data + 1) + 64),
                 _MM_HINT_T0);
    _mm_prefetch((void *)(data_level0_memory_ +
                          (*(data + 1)) * size_data_per_element_ + offsetData_),
                 _MM_HINT_T0);
    _mm_prefetch((void *)(data + 2), _MM_HINT_T0);
#endif

    for (size_t j = 1; j <= size; j++) {
      int candidate_id = *(data + j);
//                    if (candidate_id == 0) continue;
#ifdef USE_SSE
      _mm_prefetch((void *)((char *)visited_array + *(data + j + 1)),
                   _MM_HINT_T0);
      _mm_prefetch(
          (void *)(data_level0_memory_ +
                   (*(data + j + 1)) * size_data_per_element_ + offsetData_),
          _MM_HINT_T0);  ////////////
#endif
      if (visited_array[candidate_id] != visited_array_tag) {
        visited_array[candidate_id] = visited_array_tag;

        char *currObj1 = (getDataByInternalId(candidate_id));
        dist_t dist = fstdistfunc_(data_point, currObj1, dist_func_param_);

        if (top_candidates.size() < ef || lowerBound > dist) {
          candidate_set.emplace(-dist, candidate_id);
#ifdef USE_SSE
          _mm_prefetch(
              data_level0_memory_ + candidate_set.top().second *
                                        size_data_per_element_,  ///////////
              _MM_HINT_T0);  ////////////////////////
#endif

          if ((!has_deletions || !isMarkedDeleted(candidate_id)) &&
              ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(candidate_id),
                                                getVersion(candidate_id),
                                                getTimestamp(candidate_id))))
            top_candidates.emplace(dist, candidate_id);

          if (top_candidates.size() > ef) top_candidates.pop();

          if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
        }
      }
    }
  }

  visited_list_pool_->releaseVisitedList(vl);
  return top_candidates;
}

template <typename dist_t>
void HierarchicalNSW<dist_t>::getNeighborsByHeuristic2(
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst<dist_t>> &top_candidates,
    const size_t M) {
  if (top_candidates.size() < M) {
    return;
  }

  std::priority_queue<std::pair<dist_t, tableint>> queue_closest;
  std::vector<std::pair<dist_t, tableint>> return_list;
  while (top_candidates.size() > 0) {
    queue_closest.emplace(-top_candidates.top().first,
                          top_candidates.top().second);
    top_candidates.pop();
  }

  while (queue_closest.size()) {
    if (return_list.size() >= M) break;
    std::pair<dist_t, tableint> curent_pair = queue_closest.top();
    dist_t dist_to_query = -curent_pair.first;
    queue_closest.pop();
    bool good = true;

    for (std::pair<dist_t, tableint> second_pair : return_list) {
      dist_t curdist = fstdistfunc_(getDataByInternalId(second_pair.second),
                                    getDataByInternalId(curent_pair.second),
                                    dist_func_param_);
      if (curdist < dist_to_query) {
        good = false;
        break;
      }
    }
    if (good) {
      return_list.push_back(curent_pair);
    }
  }

  for (std::pair<dist_t, tableint> curent_pair : return_list) {
    top_candidates.emplace(-curent_pair.first, curent_pair.second);
  }
}

template <typename dist_t>
tableint HierarchicalNSW<dist_t>::mutuallyConnectNewElement(
    const void *data_point, tableint cur_c,
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst<dist_t>> &top_candidates,
    int level, bool isUpdate, bool concurrent) {
  size_t Mcurmax = level ? maxM_ : maxM0_;
  getNeighborsByHeuristic2(top_candidates, M_);
  if (top_candidates.size() > M_)
    throw std::runtime_error(
        "Should be not be more than M_ candidates returned by the heuristic");

  std::vector<tableint> selectedNeighbors;
  selectedNeighbors.reserve(M_);
  while (top_candidates.size() > 0) {
    selectedNeighbors.push_back(top_candidates.top().second);
    top_candidates.pop();
  }

  tableint next_closest_entry_point = selectedNeighbors.back();

  {
    // lock only during the update
    // because during the addition the lock for cur_c is already acquired
    std::unique_lock<std::mutex> lock(link_list_locks_[cur_c], std::defer_lock);
    if (concurrent && isUpdate) {
      lock.lock();
    }
    linklistsizeint *ll_cur;
    if (level == 0)
      ll_cur = get_linklist0(cur_c);
    else
      ll_cur = get_linklist(cur_c, level);

    if (*ll_cur && !isUpdate) {
      throw std::runtime_error(
          "The newly inserted element should have blank link list");
    }
    setListCount(ll_cur, selectedNeighbors.size());
    auto *data = (tableint *)(ll_cur + 1);
    for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
      if (data[idx] && !isUpdate)
        throw std::runtime_error("Possible memory corruption");
      if (level > element_levels_[selectedNeighbors[idx]])
        throw std::runtime_error(
            "Trying to make a link on a non-existent level");

      data[idx] = selectedNeighbors[idx];
    }
  }

  for (unsigned int &selectedNeighbor : selectedNeighbors) {
    std::unique_lock<std::mutex> lock(link_list_locks_[selectedNeighbor]);

    linklistsizeint *ll_other;
    if (level == 0)
      ll_other = get_linklist0(selectedNeighbor);
    else
      ll_other = get_linklist(selectedNeighbor, level);

    size_t sz_link_list_other = getListCount(ll_other);

    if (sz_link_list_other > Mcurmax)
      throw std::runtime_error("Bad value of sz_link_list_other");
    if (selectedNeighbor == cur_c)
      throw std::runtime_error("Trying to connect an element to itself");
    if (level > element_levels_[selectedNeighbor])
      throw std::runtime_error("Trying to make a link on a non-existent level");

    auto *data = (tableint *)(ll_other + 1);

    bool is_cur_c_present = false;
    if (isUpdate) {
      for (size_t j = 0; j < sz_link_list_other; j++) {
        if (data[j] == cur_c) {
          is_cur_c_present = true;
          break;
        }
      }
    }

    // If cur_c is already present in the neighboring connections of
    // `selectedNeighbors[idx]` then no need to modify any connections or run
    // the heuristics.
    if (!is_cur_c_present) {
      if (sz_link_list_other < Mcurmax) {
        data[sz_link_list_other] = cur_c;
        setListCount(ll_other, sz_link_list_other + 1);
      } else {
        // finding the "weakest" element to replace it with the new one
        dist_t d_max = fstdistfunc_(getDataByInternalId(cur_c),
                                    getDataByInternalId(selectedNeighbor),
                                    dist_func_param_);
        // Heuristic:
        std::priority_queue<std::pair<dist_t, tableint>,
                            std::vector<std::pair<dist_t, tableint>>,
                            CompareByFirst<dist_t>>
            candidates;
        candidates.emplace(d_max, cur_c);

        for (size_t j = 0; j < sz_link_list_other; j++) {
          candidates.emplace(fstdistfunc_(getDataByInternalId(data[j]),
                                          getDataByInternalId(selectedNeighbor),
                                          dist_func_param_),
                             data[j]);
        }

        getNeighborsByHeuristic2(candidates, Mcurmax);

        int indx = 0;
        while (candidates.size() > 0) {
          data[indx] = candidates.top().second;
          candidates.pop();
          indx++;
        }

        setListCount(ll_other, indx);
        // Nearest K:
        /*int indx = -1;
        for (int j = 0; j < sz_link_list_other; j++) {
            dist_t d = fstdistfunc_(getDataByInternalId(data[j]),
        getDataByInternalId(rez[idx]), dist_func_param_); if (d > d_max) {
                indx = j;
                d_max = d;
            }
        }
        if (indx >= 0) {
            data[indx] = cur_c;
        } */
      }
    }
  }

  return next_closest_entry_point;
}

template class HierarchicalNSW<float>;
}  // namespace hnswlib