#pragma once

#ifndef NO_MANUAL_VECTORIZATION
#if (defined(__SSE__) || _M_IX86_FP > 0 || defined(_M_AMD64) || defined(_M_X64))
#define USE_SSE
#ifdef __AVX__
#define USE_AVX
#ifdef __AVX512F__
#define USE_AVX512
#endif
#endif
#endif
#endif

#if defined(USE_AVX) || defined(USE_SSE)
#ifdef _MSC_VER
#include <intrin.h>

#include <stdexcept>
void cpuid(int32_t out[4], int32_t eax, int32_t ecx) {
  __cpuidex(out, eax, ecx);
}
static __int64 xgetbv(unsigned int x) { return _xgetbv(x); }
#else
#include <cpuid.h>
#include <x86intrin.h>

#include <cstdint>
static void cpuid(int32_t cpuInfo[4], int32_t eax, int32_t ecx) {
  __cpuid_count(eax, ecx, cpuInfo[0], cpuInfo[1], cpuInfo[2], cpuInfo[3]);
}
static uint64_t xgetbv(unsigned int index) {
  uint32_t eax, edx;
  __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
  return ((uint64_t)edx << 32) | eax;
}
#endif

#if defined(USE_AVX512)
#include <immintrin.h>
#endif

#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#define PORTABLE_ALIGN64 __attribute__((aligned(64)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#define PORTABLE_ALIGN64 __declspec(align(64))
#endif

// Adapted from https://github.com/Mysticial/FeatureDetector
#define _XCR_XFEATURE_ENABLED_MASK 0

static bool AVXCapable() {
  int cpuInfo[4];

  // CPU support
  cpuid(cpuInfo, 0, 0);
  int nIds = cpuInfo[0];

  bool HW_AVX = false;
  if (nIds >= 0x00000001) {
    cpuid(cpuInfo, 0x00000001, 0);
    HW_AVX = (cpuInfo[2] & ((int)1 << 28)) != 0;
  }

  // OS support
  cpuid(cpuInfo, 1, 0);

  bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
  bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

  bool avxSupported = false;
  if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
    uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
    avxSupported = (xcrFeatureMask & 0x6) == 0x6;
  }
  return HW_AVX && avxSupported;
}

static bool AVX512Capable() {
  if (!AVXCapable()) return false;

  int cpuInfo[4];

  // CPU support
  cpuid(cpuInfo, 0, 0);
  int nIds = cpuInfo[0];

  bool HW_AVX512F = false;
  if (nIds >= 0x00000007) {  //  AVX512 Foundation
    cpuid(cpuInfo, 0x00000007, 0);
    HW_AVX512F = (cpuInfo[1] & ((int)1 << 16)) != 0;
  }

  // OS support
  cpuid(cpuInfo, 1, 0);

  bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
  bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

  bool avx512Supported = false;
  if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
    uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
    avx512Supported = (xcrFeatureMask & 0xe6) == 0xe6;
  }
  return HW_AVX512F && avx512Supported;
}
#endif

#include <cstring>
#include <iostream>
#include <limits>
#include <queue>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/types.h"

using SequenceNumber = ROCKSDB_NAMESPACE::SequenceNumber;

namespace hnswlib {
typedef size_t labeltype;

// This can be extended to store state for filtering (e.g. from a std::set)
class BaseFilterFunctor {
 public:
  virtual bool operator()(labeltype id) { return true; }
  virtual bool operator()(labeltype id, SequenceNumber seq, uint64_t ts) {
    return true;
  }
  virtual ~BaseFilterFunctor() = default;
};

class FilterSeqTs : public BaseFilterFunctor {
 public:
  FilterSeqTs(SequenceNumber seqnum, uint64_t ts) : seqnum_(seqnum), ts_(ts) {}
  bool operator()(labeltype id) override { return true; }
  bool operator()(labeltype id, SequenceNumber seqnum, uint64_t ts) override {
    return seqnum < seqnum_ && ts >= ts_;
  }

 private:
  rocksdb::SequenceNumber seqnum_;
  uint64_t ts_;
};

template <typename T>
class pairGreater {
 public:
  bool operator()(const T &p1, const T &p2) { return p1.first > p2.first; }
};

template <typename T>
static void writeBinaryPOD(std::ostream &out, const T &podRef) {
  out.write((char *)&podRef, sizeof(T));
}

template <typename T>
static void readBinaryPOD(std::istream &in, T &podRef) {
  in.read((char *)&podRef, sizeof(T));
}

template <typename MTYPE>
using DISTFUNC = MTYPE (*)(const void *, const void *, const size_t);

template <typename MTYPE>
class SpaceInterface {
 public:
  // virtual void search(void *);
  virtual size_t get_data_size() = 0;

  virtual DISTFUNC<MTYPE> get_dist_func() = 0;

  virtual size_t get_dist_func_param() = 0;

  virtual ~SpaceInterface() = default;
};

template <typename dist_t>
struct ResultItem {
  dist_t dist{std::numeric_limits<dist_t>::max()};
  labeltype label{0};
  SequenceNumber version{0};
  uint64_t file_number{std::numeric_limits<uint64_t>::max()};
  ResultItem() = default;
  ResultItem(dist_t d, labeltype l, SequenceNumber v)
      : dist(d), label(l), version(v) {}
  ResultItem(dist_t d, labeltype l, SequenceNumber v, uint64_t f)
      : dist(d), label(l), version(v), file_number(f) {}
};

template <typename dist_t>
struct CompareWithVersion {
  constexpr bool operator()(const ResultItem<dist_t> &a,
                            const ResultItem<dist_t> &b) const noexcept {
    if (a.dist != b.dist) {
      return a.dist < b.dist;
    } else if (a.label != b.label) {
      return a.label < b.label;
    } else {
      return a.version > b.version;
    }
  }
};

template <typename dist_t>
using KnnResult =
    std::priority_queue<ResultItem<dist_t>, std::vector<ResultItem<dist_t>>,
                        CompareWithVersion<dist_t>>;

template <typename dist_t>
class AlgorithmInterface {
 public:
  virtual bool addPoint(const void *datapoint, labeltype label,
                        uint64_t version = 0, uint64_t timestamp = 0,
                        bool replace_deleted = false,
                        bool allow_duplicate = true,
                        bool concurrent = true) = 0;

  virtual KnnResult<dist_t> searchKnn(
      const void *, size_t, BaseFilterFunctor *isIdAllowed = nullptr) const = 0;

  // Return k nearest neighbor in the order of closer fist
  virtual std::vector<ResultItem<dist_t>> searchKnnCloserFirst(
      const void *query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr) const;

  virtual ~AlgorithmInterface() = default;
};

template <typename dist_t>
std::vector<ResultItem<dist_t>>
AlgorithmInterface<dist_t>::searchKnnCloserFirst(
    const void *query_data, size_t k, BaseFilterFunctor *isIdAllowed) const {
  std::vector<ResultItem<dist_t>> result;

  // here searchKnn returns the result in the order of further first
  auto ret = searchKnn(query_data, k, isIdAllowed);
  {
    size_t sz = ret.size();
    result.resize(sz);
    while (!ret.empty()) {
      result[--sz] = ret.top();
      ret.pop();
    }
  }

  return result;
}

enum SpaceType : uint8_t { L2, IP };
}  // namespace hnswlib

// #include "bruteforce.h"
// #include "hnswalg.h"
// #include "space_ip.h"
// #include "space_l2.h"
// #include "space_cos.h"
