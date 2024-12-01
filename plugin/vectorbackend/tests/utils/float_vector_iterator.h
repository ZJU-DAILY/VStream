#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <vector>

#include "float_vector.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class FvecIterator {
 public:
  enum class InputType { F_VEC, B_VEC };

 private:
  std::ifstream file_;
  int numLoops_;
  int loop_ = 0;
  std::streampos length_;
  InputType inputType_;
  std::streampos startPosition_;
  int limit_;
  int count_ = 0;
  std::vector<char> buffer_;

  int readIntLittleEndian() {
    int value;
    file_.read(reinterpret_cast<char *>(&value), sizeof(value));
    return value;
  }

  void ensureBuffer(size_t length) {
    if (buffer_.size() < length) buffer_.resize(length);
  }

  bool isEOF() { return (count_ != 0 && count_ % limit_ == 0) || file_.eof(); }

 public:
  static FvecIterator fromFile(const std::string &filename, int numLoops = 1) {
    if (filename.ends_with(".fvecs")) {
      return FvecIterator(filename, numLoops, 0,
                          std::numeric_limits<int>::max(), InputType::F_VEC);
    } else if (filename.ends_with(".bvecs")) {
      return FvecIterator(filename, numLoops, 0,
                          std::numeric_limits<int>::max(), InputType::B_VEC);
    } else {
      throw std::runtime_error("Unknown file type.");
    }
  }

  FvecIterator(const std::string &filename, int numLoops, std::streampos skip,
               std::streampos limit, InputType inputType)
      : numLoops_(numLoops), limit_(limit), inputType_(inputType) {
    file_.open(filename, std::ios::binary);
    if (!file_) throw std::runtime_error("Failed to open file.");
    file_.seekg(0, std::ios::end);
    length_ = file_.tellg();
    file_.seekg(0, std::ios::beg);

    int dimension = readIntLittleEndian();
    int vectorWidth = 4 + dimension * (inputType == InputType::F_VEC ? 4 : 1);
    startPosition_ = vectorWidth * skip;
    file_.seekg(startPosition_);
  }

  bool hasNext() { return !(isEOF() && loop_ == numLoops_ - 1); }

  std::vector<float> next() {
    if (isEOF()) {
      if (loop_ < numLoops_) {
        file_.seekg(startPosition_);
        loop_++;
      } else {
        throw std::runtime_error("No more vector.");
      }
    }

    count_++;
    int dimension = readIntLittleEndian();

    if (inputType_ == InputType::F_VEC) {
      ensureBuffer(dimension * 4);
      file_.read(buffer_.data(), dimension * 4);
      std::vector<float> vector(dimension);
      std::memcpy(vector.data(), buffer_.data(), dimension * 4);
      return vector;
    } else if (inputType_ == InputType::B_VEC) {
      ensureBuffer(dimension);
      file_.read(buffer_.data(), dimension);
      std::vector<float> floatBuffer(dimension);
      for (int i = 0; i < dimension; ++i) {
        floatBuffer[i] = static_cast<unsigned char>(buffer_[i]);
      }
      return floatBuffer;
    } else {
      throw std::runtime_error("Impossible branch.");
    }
  }
};

class FloatVectorIterator {
 private:
  int count_ = 0;

  FvecIterator it_;

 public:
  FloatVectorIterator(FvecIterator &&it) : it_(std::move(it)) {}

  bool hasNext() { return it_.hasNext(); }

  FloatVector next() { return {count_++, it_.next()}; }

  static FloatVectorIterator fromFile(std::string filename) {
    return FloatVectorIterator(FvecIterator::fromFile(filename));
  }

  static FloatVectorIterator fromFile(std::string filename, int numLoops) {
    return FloatVectorIterator(FvecIterator::fromFile(filename, numLoops));
  }
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE