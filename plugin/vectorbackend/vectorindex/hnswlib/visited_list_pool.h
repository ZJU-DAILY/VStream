#pragma once

#include <cstring>
#include <deque>
#include <mutex>

namespace ROCKSDB_NAMESPACE {
class Allocator;
}

namespace hnswlib {
typedef unsigned short int vl_type;

class VisitedList {
 public:
  vl_type curV;
  unsigned int numelements;
  vl_type *mass;

  explicit VisitedList(size_t numelements1,
                       ROCKSDB_NAMESPACE::Allocator *allocator);

  void reset() {
    curV++;
    if (curV == 0) {
      memset(mass, 0, sizeof(vl_type) * numelements);
      curV++;
    }
  }

  // Memories are managed by rocksdb::Allocator, so no need to free them here.
  // delete[] mass;
  ~VisitedList() = default;
};
///////////////////////////////////////////////////////////
//
// Class for multithreading pool-management of VisitedLists
//
/////////////////////////////////////////////////////////

class VisitedListPool {
  std::deque<VisitedList *> pool;
  std::mutex poolguard;
  size_t numelements;
  rocksdb::Allocator *allocator_{nullptr};

 public:
  VisitedListPool(size_t initmaxpools, size_t numelements1,
                  rocksdb::Allocator *allocator)
      : allocator_(allocator) {
    numelements = numelements1;
    for (size_t i = 0; i < initmaxpools; i++)
      pool.push_front(new VisitedList(numelements, allocator_));
  }

  VisitedList *getFreeVisitedList() {
    VisitedList *rez;
    {
      std::unique_lock<std::mutex> lock(poolguard);
      if (!pool.empty()) {
        rez = pool.front();
        pool.pop_front();
      } else {
        rez = new VisitedList(numelements, allocator_);
      }
    }
    rez->reset();
    return rez;
  }

  void releaseVisitedList(VisitedList *vl) {
    std::unique_lock<std::mutex> lock(poolguard);
    pool.push_front(vl);
  }

  ~VisitedListPool() {
    while (!pool.empty()) {
      VisitedList *rez = pool.front();
      pool.pop_front();
      delete rez;
    }
  }
};
}  // namespace hnswlib
