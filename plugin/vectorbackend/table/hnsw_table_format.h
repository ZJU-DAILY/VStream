//
// Created by shb on 23-8-26.
//

#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "../vectorbackend/vectorbackend_namespace.h"
#include "file/file_prefetch_buffer.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "options/cf_options.h"
#include "port/malloc.h"
#include "port/port.h"  // noexcept
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/format.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

// Footer encapsulates the fixed information stored at the tail end of every
// SST file. In general, it should only include things that cannot go
// elsewhere under the metaindex block. For example, checksum_type is
// required for verifying metaindex block checksum (when applicable), but
// index block handle can easily go in metaindex block (possible future).
// See also FooterBuilder below.
class Footer : public ROCKSDB_NAMESPACE::Footer {
 public:
  // Create empty. Populate using Decode.
  Footer() = default;

  // Deserialize a footer (populate fields) from `input` and check for various
  // corruptions. `input_offset` is the offset within the target file of
  // `input` buffer (future use).
  // If enforce_table_magic_number != 0, will return corruption if table magic
  // number is not equal to enforce_table_magic_number.
  Status DecodeFrom(Slice input, uint64_t input_offset,
                    uint64_t enforce_table_magic_number = 0) override;

  // Block handle for hnsw block.
  const BlockHandle& hnsw_handle() const { return hnsw_handle_; }

  // Convert this object to a human-readable form
  std::string ToString() const override;

  // Encoded lengths of Footers. Bytes for serialized Footer will always be
  // >= kMinEncodedLength and <= kMaxEncodedLength.
  //
  // Footer version 0 (legacy) will always occupy exactly this many bytes.
  // It consists of three block handles, padding, and a magic number.
  static constexpr uint32_t kVersion0EncodedLength =
      3 * BlockHandle::kMaxEncodedLength + kMagicNumberLengthByte;
  static constexpr uint32_t kMinEncodedLength = kVersion0EncodedLength;

  // Footer of versions 1 and higher will always occupy exactly this many
  // bytes. It originally consisted of the checksum type, three block handles,
  // padding (to maximum handle encoding size), a format version number, and a
  // magic number.
  static constexpr uint32_t kNewVersionsEncodedLength =
      1 + 3 * BlockHandle::kMaxEncodedLength + 4 + kMagicNumberLengthByte;
  static constexpr uint32_t kMaxEncodedLength = kNewVersionsEncodedLength;

 private:
  BlockHandle hnsw_handle_;
};

// Builder for Footer
class FooterBuilder {
 public:
  // Run builder in inputs. This is a single step with lots of parameters for
  // efficiency (based on perf testing).
  // * table_magic_number identifies file as RocksDB SST file and which kind of
  // SST format is use.
  // * format_version is a version for the footer and can also apply to other
  // aspects of the SST file (see BlockBasedTableOptions::format_version).
  // NOTE: To save complexity in the caller, when format_version == 0 and
  // there is a corresponding legacy magic number to the one specified, the
  // legacy magic number will be written for forward compatibility.
  // * footer_offset is the file offset where the footer will be written
  // (for future use).
  // * checksum_type is for formats using block checksums.
  // * index_handle is optional for some kinds of SST files.
  void Build(uint64_t table_magic_number, uint32_t format_version,
             uint64_t footer_offset, ChecksumType checksum_type,
             const BlockHandle& hnsw_handle,
             const BlockHandle& metaindex_handle,
             const BlockHandle& index_handle = BlockHandle::NullBlockHandle());

  // After Builder, get a Slice for the serialized Footer, backed by this
  // FooterBuilder.
  const Slice& GetSlice() const {
    assert(slice_.size());
    return slice_;
  }

 private:
  Slice slice_;
  std::array<char, Footer::kMaxEncodedLength> data_;
};

// Read the footer from file
// If enforce_table_magic_number != 0, ReadFooterFromFile() will return
// corruption if table_magic number is not equal to enforce_table_magic_number
Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number = 0);

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
