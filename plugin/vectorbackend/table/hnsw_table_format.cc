//
// Created by shb on 23-8-26.
//

#include "hnsw_table_format.h"

#include <string>

#include "file/random_access_file_reader.h"
#include "hnsw_table_builder.h"
#include "memory/memory_allocator_impl.h"
#include "options/options_helper.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

namespace {
inline uint8_t BlockTrailerSizeForMagicNumber(uint64_t magic_number) {
  if (magic_number == kHnswTableMagicNumber) {
    return static_cast<uint8_t>(BlockBasedTable::kBlockTrailerSize);
  } else {
    return 0;
  }
}

// Footer format, in three parts:
// * Part1
//   -> format_version == 0 (inferred from legacy magic number)
//      <empty> (0 bytes)
//   -> format_version >= 1
//      checksum type (char, 1 byte)
// * Part2
//      hnsw handle (varint64 offset, varint64 size)
//      metaindex handle (varint64 offset, varint64 size)
//      index handle     (varint64 offset, varint64 size)
//      <zero padding> for part2 size = 2 * BlockHandle::kMaxEncodedLength = 40
// * Part3
//   -> format_version == 0 (inferred from legacy magic number)
//      legacy magic number (8 bytes)
//   -> format_version >= 1 (inferred from NOT legacy magic number)
//      format_version (uint32LE, 4 bytes), also called "footer version"
//      newer magic number (8 bytes)

constexpr size_t kFooterPart2Size = 3 * BlockHandle::kMaxEncodedLength;
}  // namespace

void FooterBuilder::Build(uint64_t magic_number, uint32_t format_version,
                          uint64_t footer_offset, ChecksumType checksum_type,
                          const BlockHandle& hnsw_handle,
                          const BlockHandle& metaindex_handle,
                          const BlockHandle& index_handle) {
  (void)footer_offset;  // Future use

  assert(magic_number != Footer::kNullTableMagicNumber);
  assert(IsSupportedFormatVersion(format_version));

  char* part2;
  char* part3;
  if (format_version > 0) {
    slice_ = Slice(data_.data(), Footer::kNewVersionsEncodedLength);
    // Generate parts 1 and 3
    char* cur = data_.data();
    // Part 1
    *(cur++) = checksum_type;
    // Part 2
    part2 = cur;
    // Skip over part 2 for now
    cur += kFooterPart2Size;
    // Part 3
    part3 = cur;
    EncodeFixed32(cur, format_version);
    cur += 4;
    EncodeFixed64(cur, magic_number);
    assert(cur + 8 == slice_.data() + slice_.size());
  } else {
    throw std::runtime_error("Unsupported format version");
  }

  {
    char* cur = part2;
    cur = hnsw_handle.EncodeTo(cur);
    cur = metaindex_handle.EncodeTo(cur);
    cur = index_handle.EncodeTo(cur);
    // Zero pad remainder
    std::fill(cur, part3, char{0});
  }
}

Status Footer::DecodeFrom(Slice input, uint64_t input_offset,
                          uint64_t enforce_table_magic_number) {
  (void)input_offset;  // Future use

  // Only decode to unused Footer
  assert(table_magic_number_ == kNullTableMagicNumber);
  assert(input != nullptr);
  assert(input.size() >= kMinEncodedLength);

  const char* magic_ptr = input.data() + input.size() - kMagicNumberLengthByte;
  uint64_t magic = DecodeFixed64(magic_ptr);

  // We check for legacy formats here and silently upconvert them
  if (enforce_table_magic_number != 0 && enforce_table_magic_number != magic) {
    return Status::Corruption("Bad table magic number: expected " +
                              std::to_string(enforce_table_magic_number) +
                              ", found " + std::to_string(magic));
  }
  table_magic_number_ = magic;
  block_trailer_size_ = BlockTrailerSizeForMagicNumber(magic);

  // Parse Part3
  const char* part3_ptr = magic_ptr - 4;
  format_version_ = DecodeFixed32(part3_ptr);
  if (!IsSupportedFormatVersion(format_version_)) {
    return Status::Corruption("Corrupt or unsupported format_version: " +
                              std::to_string(format_version_));
  }
  // All known format versions >= 1 occupy exactly this many bytes.
  if (input.size() < kNewVersionsEncodedLength) {
    return Status::Corruption("Input is too short to be an SST file");
  }
  uint64_t adjustment = input.size() - kNewVersionsEncodedLength;
  input.remove_prefix(adjustment);

  // Parse Part1
  char chksum = input.data()[0];
  checksum_type_ = lossless_cast<ChecksumType>(chksum);
  if (!IsSupportedChecksumType(checksum_type())) {
    return Status::Corruption("Corrupt or unsupported checksum type: " +
                              std::to_string(lossless_cast<uint8_t>(chksum)));
  }
  // Consume checksum type field
  input.remove_prefix(1);

  // Parse Part2
  Status result = hnsw_handle_.DecodeFrom(&input);
  if (result.ok()) {
    result = metaindex_handle_.DecodeFrom(&input);
    if (result.ok()) {
      result = index_handle_.DecodeFrom(&input);
    }
  }
  return result;
  // Padding in part2 is ignored
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  result.append("hnsw handle: " + hnsw_handle_.ToString() + "\n  ");
  result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
  result.append("index handle: " + index_handle_.ToString() + "\n  ");
  result.append("table_magic_number: " + std::to_string(table_magic_number_) +
                "\n  ");
  return result;
}

Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short (" +
                              std::to_string(file_size) +
                              " bytes) to be an "
                              "sstable: " +
                              file->file_name());
  }

  std::string footer_buf;
  AlignedBuf internal_buf;
  Slice footer_input;
  uint64_t read_offset = (file_size > Footer::kMaxEncodedLength)
                             ? file_size - Footer::kMaxEncodedLength
                             : 0;
  Status s;
  // TODO: Need to pass appropriate deadline to TryReadFromCache(). Right now,
  // there is no readahead for point lookups, so TryReadFromCache will fail if
  // the required data is not in the prefetch buffer. Once deadline is enabled
  // for iterator, TryReadFromCache might do a readahead. Revisit to see if we
  // need to pass a timeout at that point
  // TODO: rate limit footer reads.
  if (prefetch_buffer == nullptr ||
      !prefetch_buffer->TryReadFromCache(
          opts, file, read_offset, Footer::kMaxEncodedLength, &footer_input,
          nullptr, opts.rate_limiter_priority)) {
    if (file->use_direct_io()) {
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, nullptr, &internal_buf,
                     opts.rate_limiter_priority);
    } else {
      footer_buf.reserve(Footer::kMaxEncodedLength);
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, &footer_buf[0], nullptr,
                     opts.rate_limiter_priority);
    }
    if (!s.ok()) return s;
  }

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    uint64_t size_on_disk = 0;
    if (fs.GetFileSize(file->file_name(), IOOptions(), &size_on_disk, nullptr)
            .ok()) {
      // Similar to CheckConsistency message, but not completely sure the
      // expected size always came from manifest.
      return Status::Corruption("Sst file size mismatch: " + file->file_name() +
                                ". Expected " + std::to_string(file_size) +
                                ", actual size " +
                                std::to_string(size_on_disk) + "\n");
    } else {
      return Status::Corruption(
          "Missing SST footer data in file " + file->file_name() +
          " File too short? Expected size: " + std::to_string(file_size));
    }
  }

  s = footer->DecodeFrom(footer_input, read_offset, enforce_table_magic_number);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", file->file_name());
    return s;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE