//
// Created by shb on 23-9-11.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "db/version_edit.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
struct FileMetaData;

struct VectorSSTFileMetrics {
  std::atomic<uint64_t> last_timestamp{0};

  std::atomic<uint32_t> access_count{0};
  std::atomic<float> access_score{0.0f};
  static constexpr float access_score_decay_factor =
      0.9f;  // XXX: configurable?
  static constexpr float access_score_weight = 100.0f;

  std::atomic<uint32_t> hit_count{0};
  std::atomic<float> hit_score{0.0f};
  static constexpr float hit_score_decay_factor = 0.9f;
  static constexpr float hit_score_weight = 100.0f;

  std::atomic<float> sim_count{0.0f};
  std::atomic<float> sim_score{0.0f};
  static constexpr float sim_score_decay_factor = 0.9f;
  static constexpr float sim_score_weight = 0.1f;

  static constexpr float age_score_decay_factor = 0.9f;
  static constexpr float age_weight = 0.1f;

  std::atomic<float> temperature;

  std::string Encode() {
    std::string result;
    PutVarint64(&result, last_timestamp.load());
    PutVarint32(&result, access_count.load());
    PutVarint32(&result, std::bit_cast<uint32_t>(access_score.load()));
    PutVarint32(&result, hit_count.load());
    PutVarint32(&result, std::bit_cast<uint32_t>(hit_score.load()));
    PutVarint32(&result, std::bit_cast<uint32_t>(sim_count.load()));
    PutVarint32(&result, std::bit_cast<uint32_t>(sim_score.load()));
    return result;
  }

  bool Decode(const Slice& src) {
    const char* data = src.data();
    const char* limit = src.data() + src.size();
    uint64_t _last_timestamp;
    uint32_t _access_count;
    uint32_t _access_score;
    uint32_t _hit_count;
    uint32_t _hit_score;
    uint32_t _sim_count;
    uint32_t _sim_score;
    data = GetVarint64Ptr(data, limit, &_last_timestamp);
    last_timestamp.store(_last_timestamp);
    data = GetVarint32Ptr(data, limit, &_access_count);
    access_count.store(_access_count);
    data = GetVarint32Ptr(data, limit, &_access_score);
    access_score.store(std::bit_cast<float>(_access_score));
    data = GetVarint32Ptr(data, limit, &_hit_count);
    hit_count.store(_hit_count);
    data = GetVarint32Ptr(data, limit, &_hit_score);
    hit_score.store(std::bit_cast<float>(_hit_score));
    data = GetVarint32Ptr(data, limit, &_sim_count);
    sim_count.store(_sim_count);
    data = GetVarint32Ptr(data, limit, &_sim_score);
    sim_score.store(std::bit_cast<float>(_sim_score));
    return data == limit;
  }

  void UpdateTemperature(uint64_t file_creation_time, uint64_t now) {
    float temperature_score;
    uint32_t _access_count = access_count.load();
    uint32_t _hit_count = hit_count.load();
    float _access_score = access_score.load();
    float _hit_score = hit_score.load();
    float _sim_count = _access_count ? sim_count.load() / _access_count : 0;
    float _sim_score = sim_score.load();
    uint32_t _last_timestamp = last_timestamp.load();
    uint32_t time_diff = now - _last_timestamp;
    float access_score_decay = expf(-(time_diff * access_score_decay_factor));
    float hit_score_decay = expf(-(time_diff * hit_score_decay_factor));
    float sim_score_decay = expf(-(time_diff * sim_score_decay_factor));
    _access_score = _access_score * access_score_decay +
                    _access_count * (1 - access_score_decay);
    _hit_score =
        _hit_score * hit_score_decay + _hit_count * (1 - hit_score_decay);
    _sim_score =
        _sim_score * sim_score_decay + _sim_count * (1 - sim_score_decay);
    float _age_score =
        1 - expf(-((now - file_creation_time) * age_score_decay_factor));
    temperature_score = _access_score * access_score_weight +
                        _hit_score * hit_score_weight +
                        _sim_score * sim_score_weight + _age_score * age_weight;
    last_timestamp.store(now);
    access_count.store(0);
    access_score.store(_access_score);
    hit_count.store(0);
    hit_score.store(_hit_score);
    sim_count.store(0.0f);
    sim_score.store(_sim_score);
    temperature.store(temperature_score);
  }
#ifdef TRACER
  float get_age_score() {
    return (temperature.load() - access_score.load() * access_score_weight -
            hit_score.load() * hit_score_weight -
            sim_score.load() * sim_score_weight) /
           age_weight;
  }
#endif
};
}  //  namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE