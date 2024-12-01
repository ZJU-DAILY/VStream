#pragma once

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
static int64_t getCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration)
      .count();
}

static std::string floatArrayToByteArray(const std::vector<float>& src) {
  std::string byteArray;
  for (auto value : src) {
    PutFixed32(&byteArray, *reinterpret_cast<const uint32_t*>(&value));
  }
  return byteArray;
}

static std::string floatArrayToByteArrayWithTimestamp(
    const std::vector<float>& src, int64_t timestamp) {
  std::string byteArray;
  PutFixed64(&byteArray, timestamp);
  for (auto value : src) {
    PutFixed32(&byteArray, *reinterpret_cast<const uint32_t*>(&value));
  }
  return byteArray;
}

static float bytesToFloat(const std::string& bytes, size_t offset) {
  uint32_t value = DecodeFixed32(bytes.data() + offset);
  return *reinterpret_cast<float*>(&value);
}

static std::string longToBytes(uint64_t l) {
  std::string byteArray;
  PutFixed64(&byteArray, l);
  return byteArray;
}

static uint64_t bytesToLong(const std::string& bytes, size_t offset) {
  return DecodeFixed64(bytes.data() + offset);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE