#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <list>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "hnswlib.h"
#include "visited_list_pool.h"

namespace ROCKSDB_NAMESPACE {
class Allocator;
class Slice;
enum ValueType : unsigned char;
}  // namespace ROCKSDB_NAMESPACE

namespace hnswlib {
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;

template <typename dist_t>
struct CompareByFirst {
  using value_type = std::pair<dist_t, tableint> const &;
  constexpr bool operator()(
      std::pair<dist_t, tableint> const &a,
      std::pair<dist_t, tableint> const &b) const noexcept {
    return a.first < b.first;
  }
};

template <typename dist_t>
class HierarchicalNSW : public AlgorithmInterface<dist_t> {
 public:

  static const tableint MAX_LABEL_OPERATION_LOCKS = 65536;

  size_t max_elements_{0};
  mutable std::atomic<size_t> cur_element_count{
      0};  // current number of elements
  size_t size_data_per_element_{0};
  size_t size_links_per_element_{0};
  mutable std::atomic<size_t> num_deleted_{0};  // number of deleted elements
  size_t M_{0};
  size_t maxM_{0};
  size_t maxM0_{0};
  size_t ef_construction_{0};
  size_t ef_{0};

  double mult_{0.0}, revSize_{0.0};
  int maxlevel_{0};

  size_t visit_list_pool_size_{1};
  std::unique_ptr<VisitedListPool> visited_list_pool_;

  // Locks operations with element by label value
  mutable std::vector<std::mutex> label_op_locks_;

  std::mutex global;
  std::vector<std::mutex> link_list_locks_;

  tableint enterpoint_node_{0};

  size_t size_links_level0_{0};
  size_t offsetData_{0}, label_offset_{0}, version_offset_{0},
      value_type_offset_{0}, timestamp_offset_{0};

  char *data_level0_memory_{nullptr};
  char **linkLists_{nullptr};
  std::vector<int> element_levels_;  // keeps level of each element

  size_t data_size_{0};

  DISTFUNC<dist_t> fstdistfunc_;
  size_t dist_func_param_;

  mutable std::shared_mutex label_lookup_lock;  // lock for label_lookup_
  std::unordered_map<labeltype, tableint> label_lookup_;

  std::default_random_engine level_generator_;
  std::default_random_engine update_probability_generator_;

  mutable std::atomic<size_t> metric_distance_computations{0};
  mutable std::atomic<long> metric_hops{0};
  size_t approximate_init_memory_usage;

  bool allow_replace_deleted_ = false;  // flag to replace deleted elements
                                        // (marked as deleted) during insertions
  bool readonly_{false};

  std::mutex deleted_elements_lock;  // lock for deleted_elements
  std::unordered_set<tableint>
      deleted_elements;  // contains internal ids of deleted elements

  ROCKSDB_NAMESPACE::Allocator *const allocator_{nullptr};

  explicit HierarchicalNSW(SpaceInterface<dist_t> *s) {}

  HierarchicalNSW(size_t dim, SpaceType s,
                  ROCKSDB_NAMESPACE::Allocator *allocator, size_t max_elements,
                  size_t M = 16, size_t ef_construction = 200,
                  size_t random_seed = 100, size_t visit_list_pool_size = 1,
                  bool allow_replace_deleted = false);

  /*  ~HierarchicalNSW() {
      // memories are managed by rocksdb, so we don't need to free them
      //    free(data_level0_memory_);
      //    for (tableint i = 0; i < cur_element_count; i++) {
      //      if (element_levels_[i] > 0) free(linkLists_[i]);
      //    }
      //    free(linkLists_);
      delete visited_list_pool_;
    }*/

  void setEf(size_t ef) { ef_ = ef; }

  inline std::mutex &getLabelOpMutex(labeltype label) const {
    // calculate hash
    size_t lock_id = label & (MAX_LABEL_OPERATION_LOCKS - 1);
    return label_op_locks_[lock_id];
  }

  inline labeltype getExternalLabel(tableint internal_id) const {
    labeltype return_label;
    memcpy(&return_label,
           (data_level0_memory_ + internal_id * size_data_per_element_ +
            label_offset_),
           sizeof(labeltype));
    return return_label;
  }

  inline void setExternalLabel(tableint internal_id, labeltype label) const {
    memcpy((data_level0_memory_ + internal_id * size_data_per_element_ +
            label_offset_),
           &label, sizeof(labeltype));
  }

  inline labeltype *getLabelType(tableint internal_id) const {
    return (labeltype *)(data_level0_memory_ +
                         internal_id * size_data_per_element_ + label_offset_);
  }

  inline char *getDataByInternalId(tableint internal_id) const {
    return (data_level0_memory_ + internal_id * size_data_per_element_ +
            offsetData_);
  }

  int getRandomLevel(double reverse_size) {
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    double r = -log(distribution(level_generator_)) * reverse_size;
    return (int)r;
  }

  size_t getMaxElements() { return max_elements_; }

  size_t getCurrentElementCount() { return cur_element_count; }

  size_t getDeletedCount() { return num_deleted_; }

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
  searchBaseLayer(tableint ep_id, const void *data_point, int layer,
                  bool concurrent);

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>,
                      CompareByFirst<dist_t>>
  searchBaseLayerST(tableint ep_id, const void *data_point, size_t ef,
                    bool has_deletions, bool collect_metrics = false,
                    BaseFilterFunctor *isIdAllowed = nullptr) const;

  void getNeighborsByHeuristic2(
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst<dist_t>> &top_candidates,
      const size_t M);

  linklistsizeint *get_linklist0(tableint internal_id) const {
    return (linklistsizeint *)(data_level0_memory_ +
                               internal_id * size_data_per_element_);
  }

  linklistsizeint *get_linklist0(tableint internal_id,
                                 char *dataLevel0Memory) const {
    return (linklistsizeint *)(dataLevel0Memory +
                               internal_id * size_data_per_element_);
  }

  linklistsizeint *get_linklist(tableint internal_id, int level) const {
    return (linklistsizeint *)(linkLists_[internal_id] +
                               (level - 1) * size_links_per_element_);
  }

  linklistsizeint *get_linklist_at_level(tableint internal_id,
                                         int level) const {
    return level == 0 ? get_linklist0(internal_id)
                      : get_linklist(internal_id, level);
  }

  tableint mutuallyConnectNewElement(
      const void *data_point, tableint cur_c,
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst<dist_t>> &top_candidates,
      int level, bool isUpdate, bool concurrent);

  void saveIndex(std::string *output);

  char *getDataByLabel(labeltype label) const;

  bool contains(labeltype label) const;

  /*
   * Marks an element with the given label deleted when the given version is
   * larger than the current version. It is thread-safe. Note that this method
   * does NOT really change the current graph.
   */
  void markDelete(labeltype label, uint64_t version, bool concurrent);

  void markDeletedInternal(tableint internalId, uint64_t version);

  /*
   * Removes the deleted mark of the node, does NOT really change the current
   * graph.
   *
   * Note: the method is not safe to use when replacement of deleted elements is
   * enabled, because elements marked as deleted can be completely removed by
   * addPoint
   */
  void unmarkDelete(labeltype label, bool concurrent);

  /*
   * Remove the deleted mark of the node.
   */
  void unmarkDeletedInternal(tableint internalId, bool force, bool concurrent);

  inline bool isMarkedDeleted(tableint internalId) const;

  inline void setValueType(tableint internalId,
                           ROCKSDB_NAMESPACE::ValueType type);

  inline uint64_t getVersion(tableint internalId) const;

  inline uint64_t getVersionWithType(tableint internalId) const;

  inline uint64_t getTimestamp(tableint internalId) const;

  inline void setVersionWithType(tableint internalId, uint64_t version, ROCKSDB_NAMESPACE::ValueType type);

  inline void setTimestamp(tableint internalId, uint64_t timestamp);

  unsigned short int getListCount(const linklistsizeint *ptr) const;

  void setListCount(linklistsizeint *ptr, unsigned short int size) const;

  /*
   * Allocate internal id for the new element
   * Note: this method is thread-safe
   */
  inline tableint allocateInternalId(labeltype label, bool concurrent);

  /*
   * Adds point. Updates the point if it is already in the index.
   * If replacement of deleted elements is enabled: replaces previously deleted
   * point if any, updating it with new point
   */
  bool addPoint(const void *data_point, labeltype label, uint64_t version,
                uint64_t timestamp, bool replace_deleted, bool allow_duplicate,
                bool concurrent) override;

  void updatePoint(const void *dataPoint, tableint internalId,
                   float updateNeighborProbability, uint64_t version,
                   uint64_t timestamp, bool concurrent);

  void repairConnectionsForUpdate(const void *dataPoint,
                                  tableint entryPointInternalId,
                                  tableint dataPointInternalId,
                                  int dataPointLevel, int maxLevel,
                                  bool concurrent);

  std::vector<tableint> getConnections(tableint internalId, int level,
                                       bool concurrent);

  tableint addPointInternal(const void *data_point, labeltype label,
                            tableint internalId, uint64_t version,
                            uint64_t timestamp, bool concurrent);

  KnnResult<dist_t> searchKnn(const void *query_data, size_t k,
                              BaseFilterFunctor *isIdAllowed) const override;

  size_t ApproximateMemoryUsage() {
    return approximate_init_memory_usage + cur_element_count * 12 +
           num_deleted_ * 4;
  }
};
}  // namespace hnswlib