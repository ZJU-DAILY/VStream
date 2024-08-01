#include "compression.h"

#include <immintrin.h>
#include <xmmintrin.h>

#include <cstring>
#include <limits>

namespace compression {
__m256i GorillaVectorDecompressor::and_f = _mm256_set1_epi8(0xf);

__m256i GorillaVectorDecompressor::zeros_operand[2][2 << 16];

GorillaVectorDecompressor::GorillaVectorDecompressor(size_t dim)
    : dim_(dim), bit_left_(32), iW(0) {
  unpacked_leading_zeros_ = static_cast<uint8_t*>(
      std::aligned_alloc(32, (dim + 31) * sizeof(uint8_t)));
  unpacked_trailing_zeros_ = static_cast<uint8_t*>(
      std::aligned_alloc(32, (dim + 31) * sizeof(uint8_t)));
  stored_leading_zeros_ =
      static_cast<uint8_t*>(std::aligned_alloc(32, dim * sizeof(uint8_t)));
  stored_trailing_zeros_ =
      static_cast<uint8_t*>(std::aligned_alloc(32, dim * sizeof(uint8_t)));
  stored_val_ = new unsigned int[dim];
  int_offset_ = 0;
  static bool zeros_operand_need_initialize = true;
  if (zeros_operand_need_initialize) {
    for (size_t i = 0; i < (2 << 16); ++i) {
      _mm256_store_si256(
          &zeros_operand[0][i],
          _mm256_set_epi8(
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              (((i >> 15) & 1) ? 0x10 : 0), (((i >> 14) & 1) ? 0x10 : 0),
              (((i >> 13) & 1) ? 0x10 : 0), (((i >> 12) & 1) ? 0x10 : 0),
              (((i >> 11) & 1) ? 0x10 : 0), (((i >> 10) & 1) ? 0x10 : 0),
              (((i >> 9) & 1) ? 0x10 : 0), (((i >> 8) & 1) ? 0x10 : 0),
              (((i >> 7) & 1) ? 0x10 : 0), (((i >> 6) & 1) ? 0x10 : 0),
              (((i >> 5) & 1) ? 0x10 : 0), (((i >> 4) & 1) ? 0x10 : 0),
              (((i >> 3) & 1) ? 0x10 : 0), (((i >> 2) & 1) ? 0x10 : 0),
              (((i >> 1) & 1) ? 0x10 : 0), ((i & 1) ? 0x10 : 0)));
      _mm256_store_si256(
          &zeros_operand[1][i],
          _mm256_set_epi8(
              (((i >> 15) & 1) ? 0x10 : 0), (((i >> 14) & 1) ? 0x10 : 0),
              (((i >> 13) & 1) ? 0x10 : 0), (((i >> 12) & 1) ? 0x10 : 0),
              (((i >> 11) & 1) ? 0x10 : 0), (((i >> 10) & 1) ? 0x10 : 0),
              (((i >> 9) & 1) ? 0x10 : 0), (((i >> 8) & 1) ? 0x10 : 0),
              (((i >> 7) & 1) ? 0x10 : 0), (((i >> 6) & 1) ? 0x10 : 0),
              (((i >> 5) & 1) ? 0x10 : 0), (((i >> 4) & 1) ? 0x10 : 0),
              (((i >> 3) & 1) ? 0x10 : 0), (((i >> 2) & 1) ? 0x10 : 0),
              (((i >> 1) & 1) ? 0x10 : 0), ((i & 1) ? 0x10 : 0), 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }
    zeros_operand_need_initialize = false;
  }
}

void GorillaVectorDecompressor::UnpackMetadata(unsigned int packed_zeros_size) {
  for (unsigned int i = 0; i < packed_zeros_size; i += 32) {
    //    _mm_prefetch(&(stored_leading_zeros_[i]), _MM_HINT_T0);
    //    _mm_prefetch(&(stored_trailing_zeros_[i]), _MM_HINT_T0);
    __m256i_u packed_zeros_batch = _mm256_loadu_si256(
        reinterpret_cast<const __m256i_u*>(&(packed_zeros_buffer_[i])));
    __m256i leading_zeros_batch =
        _mm256_and_si256(_mm256_srli_epi16(packed_zeros_batch, 4), and_f);
    __m256i trailing_zeros_batch = _mm256_and_si256(packed_zeros_batch, and_f);
    for (unsigned int j = 0; j < 2; ++j) {
      uint16_t first_leading_zeros_batch = first_leading_zeros_buffer_[(i >> 4) + j];
      uint16_t first_trailing_zeros_batch = first_trailing_zeros_buffer_[(i >> 4) + j];
      leading_zeros_batch = _mm256_or_si256(
          leading_zeros_batch, zeros_operand[j][first_leading_zeros_batch]);
      trailing_zeros_batch = _mm256_or_si256(
          trailing_zeros_batch, zeros_operand[j][first_trailing_zeros_batch]);
    }
    _mm256_store_si256(
        reinterpret_cast<__m256i*>(&(unpacked_leading_zeros_[i])),
        leading_zeros_batch);
    _mm256_store_si256(
        reinterpret_cast<__m256i*>(&(unpacked_trailing_zeros_[i])),
        trailing_zeros_batch);
  }
}

const void* GorillaVectorDecompressor::ReadFirstVector(const void* in,
                                                       float*& vector) const {
  memset(stored_leading_zeros_, std::numeric_limits<int>::max(),
         dim_ * sizeof(uint8_t));
  memset(stored_trailing_zeros_, 0, dim_ * sizeof(uint8_t));
  const void* input = in;
  vector = new float[dim_];
  memcpy(vector, input, dim_ * sizeof(float));
  memcpy(stored_val_, vector, dim_ * sizeof(unsigned int));
  input = reinterpret_cast<const float*>(input) + dim_;
  return input;
}

const void* GorillaVectorDecompressor::ReadSecondVector(const void* in,
                                                        float*& _vector) {
  /* Format
   * packed_zero_size
   * first_control_bit_buffer_
   * first_leading_zeros_buffer_
   * first_trailing_zeros_buffer_
   * packed_zeros_buffer_
   * xor_value_
   */
  const void* input = in;
  const unsigned int packed_zeros_size =
      *reinterpret_cast<const unsigned int*>(input);
  first_control_bit_buffer_ = reinterpret_cast<const uint32_t*>(input) + 1;
  first_leading_zeros_buffer_ = reinterpret_cast<const uint16_t*>(
      reinterpret_cast<const uint8_t*>(first_control_bit_buffer_) +
      ((dim_ + 7) >> 3));
  first_trailing_zeros_buffer_ = reinterpret_cast<const uint16_t*>(
      reinterpret_cast<const uint8_t*>(first_leading_zeros_buffer_) +
      ((packed_zeros_size + 7) >> 3));
  packed_zeros_buffer_ =
      reinterpret_cast<const uint8_t*>(first_trailing_zeros_buffer_) +
      ((packed_zeros_size + 7) >> 3);
  UnpackMetadata(packed_zeros_size);
  input = reinterpret_cast<const uint8_t*>(packed_zeros_buffer_) +
          packed_zeros_size;
  int_offset_ = 0;
  bit_left_ = 32;
  iW = *(reinterpret_cast<const unsigned int*>(input) + int_offset_);
  for (size_t i = 0, j = 0; i < dim_; ++i) {
    // process in batch of 32
    uint32_t control_word;
    if ((i & 31) == 0) {
      control_word = first_control_bit_buffer_[i >> 5];
    }
    // Note CPU is little elden, the last bit is actually the first element
    if ((control_word & 1) == 0) {
      stored_leading_zeros_[i] = unpacked_leading_zeros_[j];
      stored_trailing_zeros_[i] = unpacked_trailing_zeros_[j];
      unsigned int value = GetInt(
          input, 32 - stored_leading_zeros_[i] - stored_trailing_zeros_[i]);
      value <<= stored_trailing_zeros_[i];

      stored_val_[i] ^= value;
      ++j;
    }
    control_word >>= 1;
  }
  _vector = new float[dim_];
  memcpy(_vector, stored_val_, dim_ * sizeof(unsigned int));

  if (bit_left_ < 32) {
    int_offset_++;
  }

  return reinterpret_cast<const unsigned int*>(input) + int_offset_;
}

const void* GorillaVectorDecompressor::ReadNextVector(const void* in,
                                                      float*& _vector) {
  /* Format
   * second_control_bits_size packed_zeros_size
   * first_control_bit_buffer_
   * second_control_bit_buffer_
   * first_leading_zeros_buffer_
   * first_trailing_zeros_buffer_
   * packed_zeros_buffer_
   * xor_value_
   */
  const void* input = in;
  const unsigned int second_control_bits_size =
      *reinterpret_cast<const unsigned int*>(input);
  const unsigned int packed_zeros_size =
      *(reinterpret_cast<const unsigned int*>(input) + 1);
  first_control_bit_buffer_ = reinterpret_cast<const uint32_t*>(input) + 2;
  second_control_bit_buffer_ =
      reinterpret_cast<const uint8_t*>(first_control_bit_buffer_) +
      ((dim_ + 7) >> 3);
  first_leading_zeros_buffer_ = reinterpret_cast<const uint16_t*>(
      reinterpret_cast<const uint8_t*>(second_control_bit_buffer_) +
      ((second_control_bits_size + 7) >> 3));
  first_trailing_zeros_buffer_ = reinterpret_cast<const uint16_t*>(
      reinterpret_cast<const uint8_t*>(first_leading_zeros_buffer_) +
      ((packed_zeros_size + 7) >> 3));
  packed_zeros_buffer_ =
      reinterpret_cast<const uint8_t*>(first_trailing_zeros_buffer_) +
      ((packed_zeros_size + 7) >> 3);
  UnpackMetadata(packed_zeros_size);
  input = reinterpret_cast<const uint8_t*>(packed_zeros_buffer_) +
          packed_zeros_size;
  int_offset_ = 0;
  bit_left_ = 32;
  iW = *(reinterpret_cast<const unsigned int*>(input) + int_offset_);
  for (uint32_t i = 0, j = 0, k = 0; i < dim_; ++i) {
    // process in batch of 32
    uint32_t control_word;
    // process in batch of 8
    uint8_t second_control_byte;
    if ((i & 31) == 0) {
      control_word = first_control_bit_buffer_[i >> 5];
    }
    // Note CPU is little elden, the last bit is actually the first element
    if (control_word & 1) {
      if ((j & 7) == 0) {
        second_control_byte = second_control_bit_buffer_[j >> 3];
      } else {
        second_control_byte >>= 1;
      }
      if (second_control_byte & 1) {
        stored_leading_zeros_[i] = unpacked_leading_zeros_[k];
        stored_trailing_zeros_[i] = unpacked_trailing_zeros_[k];
        ++k;
        unsigned int value = GetInt(
            input, 32 - stored_leading_zeros_[i] - stored_trailing_zeros_[i]);
        value <<= stored_trailing_zeros_[i];

        stored_val_[i] ^= value;
      }
      ++j;
    } else {
      unsigned int value = GetInt(
          input, 32 - stored_leading_zeros_[i] - stored_trailing_zeros_[i]);
      value <<= stored_trailing_zeros_[i];
      stored_val_[i] ^= value;
    }
    control_word >>= 1;
  }
  _vector = new float[dim_];
  memcpy(_vector, stored_val_, dim_ * sizeof(float));

  if (bit_left_ < 32) {
    int_offset_++;
  }

  return reinterpret_cast<const unsigned int*>(input) + int_offset_;
}

GorillaVectorDecompressor::~GorillaVectorDecompressor() {
  std::free(unpacked_leading_zeros_);
  std::free(unpacked_trailing_zeros_);
  std::free(stored_leading_zeros_);
  std::free(stored_trailing_zeros_);
  delete[] stored_val_;
}

GorillaVectorCompressor::GorillaVectorCompressor(size_t dim)
    : dim_(dim), int_offset_(0), bit_left_(32), iW(0) {
  buffer_ = new float[dim * 2];
  stored_leading_zeros_ = new uint8_t[dim_];
  stored_trailing_zeros_ = new uint8_t[dim_];
  stored_val_ = static_cast<unsigned int*>(
      std::aligned_alloc(32, dim_ * sizeof(unsigned int)));
  xor_buffer_ = static_cast<unsigned int*>(
      std::aligned_alloc(32, dim_ * sizeof(unsigned int)));
  size_buffer_ = new unsigned int[2];
  buffer_size_ = (dim_ + 7) >> 3;
  first_control_bit_buffer_ = new uint8_t[buffer_size_];
  second_control_bit_buffer_ = new uint8_t[buffer_size_];
  first_leading_zeros_buffer_ = new uint8_t[buffer_size_];
  first_trailing_zeros_buffer_ = new uint8_t[buffer_size_];
  packed_zeros_buffer_ = new uint8_t[dim_];
}

void GorillaVectorCompressor::AddFirstVector(const float* vector,
                                             std::string& out) const {
  memset(stored_leading_zeros_, std::numeric_limits<int>::max(),
         dim_ * sizeof(uint8_t));
  memset(stored_trailing_zeros_, 0, dim_ * sizeof(uint8_t));
  memcpy(stored_val_, vector, dim_ * sizeof(float));
  out.append((char*)stored_val_, dim_ * sizeof(int));
}

void GorillaVectorCompressor::PackMetadata(const float* vector,
                                           bool need_save) {
  auto* buffer = reinterpret_cast<uint8_t*>(first_control_bit_buffer_);
  //  memset(first_control_bit_buffer_, 0, buffer_size_);
  for (unsigned int i = 0; i < dim_; i += 8) {
    _mm_prefetch(&(stored_val_[i]), _MM_HINT_T0);
    __m256i_u vector_batch =
        _mm256_loadu_si256(reinterpret_cast<const __m256i_u*>(&(vector[i])));
    __m256i stored_batch =
        _mm256_load_si256(reinterpret_cast<const __m256i*>(&(stored_val_[i])));
    __m256i xor_word = _mm256_xor_si256(vector_batch, stored_batch);
    _mm256_store_si256(reinterpret_cast<__m256i*>(&(xor_buffer_[i])), xor_word);
    if (need_save) {
      // Compare the vector with zero
      __m256i cmp_result = _mm256_cmpeq_epi32(xor_word, _mm256_setzero_si256());
      // Extract the comparison result as a bitmap
      int bitmap = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result));
      *buffer++ = bitmap;
    }
  }
}

void GorillaVectorCompressor::AddSecondVector(const float* vector,
                                              std::string& out) {
  int_offset_ = 0;
  bit_left_ = 32;
  iW = 0;
  size_buffer_[0] = 0;
  memset(first_leading_zeros_buffer_, 0, buffer_size_);
  memset(first_trailing_zeros_buffer_, 0, buffer_size_);
  PackMetadata(vector, true);
  for (size_t i = 0; i < dim_; i++) {
    if (unsigned int xor_val = xor_buffer_[i]; xor_val != 0) {
      const int leading_zeros = __builtin_clz(xor_val);
      const int trailing_zeros = __builtin_ctz(xor_val);
      const int meaningful_bits = 32 - leading_zeros - trailing_zeros;

      if ((leading_zeros >> 4) == 0x1) {
        SetBit(first_leading_zeros_buffer_, size_buffer_[0]);
      }
      if ((trailing_zeros >> 4) == 0x1) {
        SetBit(first_trailing_zeros_buffer_, size_buffer_[0]);
      }
      packed_zeros_buffer_[size_buffer_[0]] =
          (leading_zeros << 4) | (trailing_zeros & 0xf);
      WriteBits(xor_val >> trailing_zeros, meaningful_bits);

      stored_leading_zeros_[i] = leading_zeros;
      stored_trailing_zeros_[i] = trailing_zeros;
      ++size_buffer_[0];
    }
  }

  memcpy(stored_val_, vector, dim_ * sizeof(int));
  if (bit_left_ < 32) {
    *(reinterpret_cast<unsigned int*>(buffer_) + int_offset_++) = iW;
  }
  out.append((char*)size_buffer_, sizeof(unsigned int));
  out.append((char*)first_control_bit_buffer_, buffer_size_);
  out.append((char*)first_leading_zeros_buffer_,
             ((size_buffer_[0] + 7) >> 3) * sizeof(uint8_t));
  out.append((char*)first_trailing_zeros_buffer_,
             ((size_buffer_[0] + 7) >> 3) * sizeof(uint8_t));
  out.append((char*)packed_zeros_buffer_, size_buffer_[0] * sizeof(uint8_t));
  out.append((char*)buffer_, int_offset_ * sizeof(unsigned int));
}

void GorillaVectorCompressor::AddNextVector(const float* vector,
                                            std::string& out) {
  int_offset_ = 0;
  bit_left_ = 32;
  iW = 0;
  memset(size_buffer_, 0, sizeof(unsigned int) * 2);
  memset(first_control_bit_buffer_, 0, buffer_size_);
  memset(first_leading_zeros_buffer_, 0, buffer_size_);
  memset(first_trailing_zeros_buffer_, 0, buffer_size_);
  memset(second_control_bit_buffer_, 0, buffer_size_);
  PackMetadata(vector, false);
  for (size_t i = 0; i < dim_; i++) {
    if (unsigned int xor_val = xor_buffer_[i]; xor_val != 0) {
      const int leading_zeros = __builtin_clz(xor_val);
      const int trailing_zeros = __builtin_ctz(xor_val);

      if (leading_zeros >= stored_leading_zeros_[i] &&
          trailing_zeros >= stored_trailing_zeros_[i]) {
        const uint8_t meaningful_bits =
            32 - stored_leading_zeros_[i] - stored_trailing_zeros_[i];

        xor_val >>= stored_trailing_zeros_[i];
        WriteBits(xor_val, meaningful_bits);
      } else {
        const int meaningful_bits = 32 - leading_zeros - trailing_zeros;

        if ((leading_zeros >> 4) == 0x1) {
          SetBit(first_leading_zeros_buffer_, size_buffer_[1]);
        }
        if ((trailing_zeros >> 4) == 0x1) {
          SetBit(first_trailing_zeros_buffer_, size_buffer_[1]);
        }
        packed_zeros_buffer_[size_buffer_[1]] =
            (leading_zeros << 4) | (trailing_zeros & 0xf);
        WriteBits(xor_val >> trailing_zeros, meaningful_bits);

        stored_leading_zeros_[i] = leading_zeros;
        stored_trailing_zeros_[i] = trailing_zeros;
        SetBit(first_control_bit_buffer_, i);
        SetBit(second_control_bit_buffer_, size_buffer_[0]);
        ++size_buffer_[0];
        ++size_buffer_[1];
      }
    } else {
      SetBit(first_control_bit_buffer_, i);
      ++size_buffer_[0];
    }
  }

  memcpy(stored_val_, vector, dim_ * sizeof(int));
  if (bit_left_ < 32) {
    *(reinterpret_cast<unsigned int*>(buffer_) + int_offset_++) = iW;
  }
  out.append((char*)size_buffer_, sizeof(unsigned int) * 2);
  out.append((char*)first_control_bit_buffer_, buffer_size_);
  out.append((char*)second_control_bit_buffer_,
             ((size_buffer_[0] + 7) >> 3) * sizeof(uint8_t));
  out.append((char*)first_leading_zeros_buffer_,
             ((size_buffer_[1] + 7) >> 3) * sizeof(uint8_t));
  out.append((char*)first_trailing_zeros_buffer_,
             ((size_buffer_[1] + 7) >> 3) * sizeof(uint8_t));
  out.append((char*)packed_zeros_buffer_, size_buffer_[1] * sizeof(uint8_t));
  out.append((char*)buffer_, int_offset_ * sizeof(unsigned int));
}

GorillaVectorCompressor::~GorillaVectorCompressor() {
  delete[] buffer_;
  delete[] stored_leading_zeros_;
  delete[] stored_trailing_zeros_;
  std::free(stored_val_);
  std::free(xor_buffer_);
  delete[] size_buffer_;
  delete[] first_control_bit_buffer_;
  delete[] second_control_bit_buffer_;
  delete[] first_leading_zeros_buffer_;
  delete[] first_trailing_zeros_buffer_;
  delete[] packed_zeros_buffer_;
}

}  // namespace compression
