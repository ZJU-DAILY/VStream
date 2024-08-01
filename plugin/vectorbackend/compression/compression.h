#pragma once

#include <immintrin.h>
#include <xmmintrin.h>

#include <memory>
#include <string>

namespace compression {

static constexpr unsigned int MASK[]{
    1,          3,         7,         15,        31,        63,
    127,        255,       511,       1023,      2047,      4095,
    8191,       16383,     32767,     65535,     131071,    262143,
    524287,     1048575,   2097151,   4194303,   8388607,   16777215,
    33554431,   67108863,  134217727, 268435455, 536870911, 1073741823,
    2147483647, 4294967295};
static constexpr unsigned int BIT_SET_MASK[]{
    0x00000001, 0x00000002, 0x00000004, 0x00000008, 0x00000010, 0x00000020,
    0x00000040, 0x00000080, 0x00000100, 0x00000200, 0x00000400, 0x00000800,
    0x00001000, 0x00002000, 0x00004000, 0x00008000, 0x00010000, 0x00020000,
    0x00040000, 0x00080000, 0x00100000, 0x00200000, 0x00400000, 0x00800000,
    0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000,
    0x40000000, 0x80000000};

class GorillaVectorDecompressor {
 public:
  explicit GorillaVectorDecompressor(size_t dim);
  void UnpackMetadata(unsigned int packed_zeros_size);
  const void* ReadFirstVector(const void* in, float*& vector) const;
  const void* ReadSecondVector(const void* in, float*& vector);
  const void* ReadNextVector(const void* in, float*& vector);
  ~GorillaVectorDecompressor();

 private:
  uint32_t dim_;
  const uint32_t* first_control_bit_buffer_ = nullptr;
  const uint8_t* second_control_bit_buffer_ = nullptr;
  const uint16_t* first_leading_zeros_buffer_ = nullptr;
  const uint16_t* first_trailing_zeros_buffer_ = nullptr;
  const uint8_t* packed_zeros_buffer_{};
  uint8_t __attribute__((aligned(32))) * unpacked_leading_zeros_;
  uint8_t __attribute__((aligned(32))) * unpacked_trailing_zeros_;
  uint8_t __attribute__((aligned(32))) * stored_leading_zeros_;
  uint8_t __attribute__((aligned(32))) * stored_trailing_zeros_;
  static __m256i and_f;
  static __m256i zeros_operand[2][2 << 16];
  unsigned int* stored_val_;
  unsigned int int_offset_;
  unsigned int bit_left_;
  unsigned int iW;

  inline bool ReadBit(const void* input) {
    bool bit = (iW & BIT_SET_MASK[--bit_left_]) != 0;
    CheckAndFlipWord(input);
    return bit;
  }

  inline void FlipWord(const void* input) {
    iW = *(reinterpret_cast<const unsigned int*>(input) + ++int_offset_);
    bit_left_ = 32;
  }

  inline void CheckAndFlipWord(const void* input) {
    if (bit_left_ == 0) {
      FlipWord(input);
    }
  }

  inline unsigned int GetInt(const void* input, unsigned int bits) {
    unsigned int value;
    if (bits <= bit_left_) {
      value = (iW >> (bit_left_ - bits)) & MASK[bits - 1];
      bit_left_ -= bits;
      CheckAndFlipWord(input);
    } else {
      value = iW & MASK[bit_left_ - 1];
      bits -= bit_left_;
      FlipWord(input);
      value <<= bits;
      value |= (iW >> (bit_left_ - bits));
      bit_left_ -= bits;
    }
    return value;
  }

  inline unsigned int NextClearBit(const void* input, size_t maxBits) {
    unsigned int val = 0x00;

    for (size_t i = 0; i < maxBits; ++i) {
      val <<= 1;
      bool bit = ReadBit(input);

      if (bit) {
        val |= 0x01;
      } else {
        break;
      }
    }
    return val;
  }
};

class GorillaVectorCompressor {
 public:
  explicit GorillaVectorCompressor(size_t dim);
  void PackMetadata(const float* vector, bool need_save);
  void AddFirstVector(const float* vector, std::string& out) const;
  void AddSecondVector(const float* vector, std::string& out);
  void AddNextVector(const float* vector, std::string& out);
  ~GorillaVectorCompressor();

 private:
  unsigned int dim_;
  uint8_t __attribute__((aligned(32))) * stored_leading_zeros_;
  uint8_t __attribute__((aligned(32))) * stored_trailing_zeros_;
  unsigned int __attribute__((aligned(32))) * stored_val_;
  unsigned int __attribute__((aligned(32))) * xor_buffer_;
  float* buffer_;
  unsigned int* size_buffer_;
  size_t buffer_size_;
  uint8_t __attribute__((aligned(32))) * first_control_bit_buffer_;
  uint8_t __attribute__((aligned(32))) * second_control_bit_buffer_;
  uint8_t __attribute__((aligned(32))) * first_leading_zeros_buffer_;
  uint8_t __attribute__((aligned(32))) * first_trailing_zeros_buffer_;
  uint8_t __attribute__((aligned(32))) * packed_zeros_buffer_;
  unsigned int int_offset_;
  unsigned int bit_left_;
  unsigned int iW;

  inline void SetBit() {
    iW |= BIT_SET_MASK[--bit_left_];
    CheckAndFlipWord();
  }

  static inline void SetBit(uint8_t* buffer, uint32_t offset) {
    buffer[offset >> 3] |= BIT_SET_MASK[offset & 0x7];
  }

  inline void ResetBit() {
    --bit_left_;
    CheckAndFlipWord();
  }

  inline void FlipWord() {
    *(reinterpret_cast<unsigned int*>(buffer_) + int_offset_++) = iW;
    iW = 0;
    bit_left_ = 32;
  }

  inline void CheckAndFlipWord() {
    if (bit_left_ == 0) {
      FlipWord();
    }
  }

  inline void WriteBits(unsigned int value, unsigned int bits) {
    if (bits <= bit_left_) {
      unsigned int bit_offset = bit_left_ - bits;
      iW |= (value << bit_offset) & MASK[bit_left_ - 1];
      bit_left_ -= bits;
      CheckAndFlipWord();
    } else {
      value &= MASK[bits - 1];
      unsigned int bit_offset = bits - bit_left_;
      iW |= value >> bit_offset;
      bits -= bit_left_;
      FlipWord();
      iW |= value << (32 - bits);
      bit_left_ -= bits;
    }
  };
};
}  // namespace compression