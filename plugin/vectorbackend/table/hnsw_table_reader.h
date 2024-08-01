//
// Created by shb on 23-8-29.
//

#pragma once

#include <memory>

#include "file/filename.h"
#include "hnsw_block.h"
#include "hnsw_reader.h"
#include "hnsw_table_factory.h"
#include "hnsw_table_format.h"
#include "hnsw_table_uncompression_dict_reader.h"
#include "table/block_based/block.h"
#include "table/block_based/block_cache.h"
#include "table/block_based/cachable_entry.h"
#include "table/persistent_cache_options.h"
#include "table/table_reader.h"
#include "trace_replay/block_cache_tracer.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
// Reader class for HnswTable format.
class HnswTable : public TableReader {
 public:
  // 1-byte compression type + 32-bit checksum
  static constexpr size_t kBlockTrailerSize = 5;

  // Attempt to open the table that is stored in bytes [0...file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table_reader" to the newly opened
  // table.  The client should delete "*table_reader" when no longer needed.
  // If there was an error while initializing the table, sets "*table_reader"
  // to nullptr and returns a non-ok status.
  //
  // @param file must remain live while this Table is in use.
  // @param prefetch_index_in_cache can be used to disable
  // prefetching of
  //    index and filter blocks into block cache at startup
  // @param skip_filters Disables loading/accessing the filter block. Overrides
  //    prefetch_index_in_cache, so filter will be skipped if both
  //    are set.
  // @param force_direct_prefetch if true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  static Status Open(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      const EnvOptions& env_options, const HnswTableOptions& table_options,
      const InternalKeyComparator& internal_key_comparator,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      uint8_t block_protection_bytes_per_key,
      std::unique_ptr<TableReader>* table_reader, uint64_t tail_size,
      const std::shared_ptr<CacheReservationManager>&
          table_reader_cache_res_mgr = nullptr,
      bool prefetch_index_in_cache = true, int level = -1,
      bool immortal_table = false, SequenceNumber largest_seqno = 0,
      bool force_direct_prefetch = false,
      TailPrefetchStats* tail_prefetch_stats = nullptr,
      BlockCacheTracer* block_cache_tracer = nullptr,
      size_t max_file_size_for_l0_meta_pin = 0,
      const std::string& cur_db_session_id = "", uint64_t cur_file_num = 0,
      UniqueId64x2 expected_unique_id = {});

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // @param read_options Must outlive the returned iterator.
  // @param skip_filters Disabled but keep for inheirtance.
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction.
  InternalIterator* NewIterator(const ReadOptions& read_options,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;

  Status GetData(const ReadOptions& read_options, uint32_t target_internal_id,
                 Entry* entry);

  // @param skip_filters Disables loading/accessing the filter block
  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  // Pre-fetch the disk blocks that correspond to the key range specified by
  // (kbegin, kend). The call will return error status in the event of
  // IO or iteration error.
  Status Prefetch(const ReadOptions& read_options, const Slice* begin,
                  const Slice* end) override;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file). The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                               const Slice& key,
                               TableReaderCaller caller) override;

  // Given start and end keys, return the approximate data size in the file
  // between the keys. The returned value is in terms of file bytes, and so
  // includes effects like compression of the underlying data.
  // The start key must not be greater than the end key.
  uint64_t ApproximateSize(const ReadOptions& read_options, const Slice& start,
                           const Slice& end, TableReaderCaller caller) override;

  Status ApproximateKeyAnchors(const ReadOptions& read_options,
                               std::vector<Anchor>& anchors) override;

  bool TEST_BlockInCache(const BlockHandle& handle) const;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table && block cache enabled
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  size_t ApproximateMemoryUsage() const override;

  // convert SST file to a human-readable form
  Status DumpTable(WritableFile* out_file) override;

  Status VerifyChecksum(const ReadOptions& readOptions,
                        TableReaderCaller caller) override;

  ~HnswTable();

  bool TEST_IndexBlockInCache() const;
  bool TEST_HnswBlockInCache() const;

  // IndexReader is the interface that provides the functionality for index
  // access.
  class IndexReader {
   public:
    virtual ~IndexReader() = default;

    // Create an iterator for index access. If iter is null, then a new object
    // is created on the heap, and the callee will have the ownership.
    // If a non-null iter is passed in, it will be used, and the returned value
    // is either the same as iter or a new on-heap object that
    // wraps the passed iter. In the latter case the return value points
    // to a different object then iter, and the callee has the ownership of the
    // returned object.
    virtual HnswIndexBlockIter* NewIterator(
        const ReadOptions& read_options, HnswIndexBlockIter* iter,
        GetContext* get_context, BlockCacheLookupContext* lookup_context) = 0;

    // Report an approximation of how much memory has been used other than
    // memory that was allocated in block cache.
    virtual size_t ApproximateMemoryUsage() const = 0;
    // Cache the dependencies of the index reader (e.g. the partitions
    // of a partitioned index).
    virtual Status CacheDependencies(
        const ReadOptions& /*ro*/, bool /* pin */,
        FilePrefetchBuffer* /* tail_prefetch_buffer */) {
      return Status::OK();
    }
  };

  class IndexReaderCommon;

  static void SetupBaseCacheKey(const TableProperties* properties,
                                const std::string& cur_db_session_id,
                                uint64_t cur_file_number,
                                OffsetableCacheKey* out_base_cache_key,
                                bool* out_is_stable = nullptr);

  static CacheKey GetCacheKey(const OffsetableCacheKey& base_cache_key,
                              const BlockHandle& handle);

  static void UpdateCacheInsertionMetrics(BlockType block_type,
                                          GetContext* get_context, size_t usage,
                                          bool redundant,
                                          Statistics* const statistics);

  Statistics* GetStatistics() const;
  bool IsLastLevel() const;

  // Get the size to read from storage for a BlockHandle. size_t because we
  // are about to load into memory.
  static inline size_t BlockSizeWithTrailer(const BlockHandle& handle) {
    return static_cast<size_t>(handle.size() + kBlockTrailerSize);
  }

  // It is the caller's responsibility to make sure that this is called with
  // block-based table serialized block contents, which contains the compression
  // byte in the trailer after `block_size`.
  static inline CompressionType GetBlockCompressionType(const char* block_data,
                                                        size_t block_size) {
    return static_cast<CompressionType>(block_data[block_size]);
  }
  static inline CompressionType GetBlockCompressionType(
      const BlockContents& contents) {
    assert(contents.has_trailer);
    return GetBlockCompressionType(contents.data.data(), contents.data.size());
  }

  struct Rep;

  Rep* get_rep() { return rep_; }
  const Rep* get_rep() const { return rep_; }

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(const ReadOptions& ro,
                                   const BlockHandle& block_handle,
                                   TBlockIter* input_iter, BlockType block_type,
                                   FilePrefetchBuffer* prefetch_buffer,
                                   bool for_compaction, bool async_read,
                                   Status& s) const;

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(const ReadOptions& ro,
                                   CachableEntry<Block>& block,
                                   TBlockIter* input_iter, Status s) const;

  friend class UncompressionDictReader;

 protected:
  Rep* rep_;
  explicit HnswTable(Rep* rep, BlockCacheTracer* const block_cache_tracer)
      : rep_(rep), block_cache_tracer_(block_cache_tracer) {}
  // No copying allowed
  explicit HnswTable(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;

 private:
  friend class MockedHnswTable;
  friend class HnswTableReaderTestVerifyChecksum_ChecksumMismatch_Test;
  BlockCacheTracer* const block_cache_tracer_;

  void UpdateCacheHitMetrics(BlockType block_type, GetContext* get_context,
                             size_t usage) const;
  void UpdateCacheMissMetrics(BlockType block_type,
                              GetContext* get_context) const;

  // Either Block::NewDataIterator() or Block::NewIndexIterator().
  template <typename TBlockIter>
  static TBlockIter* InitBlockIterator(const Rep* rep, Block* block,
                                       BlockType block_type,
                                       TBlockIter* input_iter,
                                       bool block_contents_pinned);

  // If block cache enabled (compressed or uncompressed), looks for the block
  // identified by handle in (1) uncompressed cache, (2) compressed cache, and
  // then (3) file. If found, inserts into the cache(s) that were searched
  // unsuccessfully (e.g., if found in file, will add to both uncompressed and
  // compressed caches if they're enabled).
  //
  // @param block_entry value is set to the uncompressed block if found. If
  //    in uncompressed block cache, also sets cache_handle to reference that
  //    block.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> MaybeReadBlockAndLoadToCache(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, const UncompressionDict& uncompression_dict,
      bool for_compaction, CachableEntry<TBlocklike>* block_entry,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      BlockContents* contents, bool async_read) const;

  // Similar to the above, with one crucial difference: it will retrieve the
  // block from the file even if there are no caches configured (assuming the
  // read options allow I/O).
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> RetrieveBlock(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, const UncompressionDict& uncompression_dict,
      CachableEntry<TBlocklike>* block_entry, GetContext* get_context,
      BlockCacheLookupContext* lookup_context, bool for_compaction,
      bool use_cache, bool async_read) const;

  template <typename TBlocklike>
  WithBlocklikeCheck<void, TBlocklike> SaveLookupContextOrTraceRecord(
      const Slice& block_key, bool is_cache_hit, const ReadOptions& ro,
      const TBlocklike* parsed_block_value,
      BlockCacheLookupContext* lookup_context) const;

  void FinishTraceRecord(const BlockCacheLookupContext& lookup_context,
                         const Slice& block_key, const Slice& referenced_key,
                         bool does_referenced_key_exist,
                         uint64_t referenced_data_size) const;

  // Get the iterator from the index reader.
  //
  // If input_iter is not set, return a new Iterator.
  // If input_iter is set, try to update it and return it as Iterator.
  // However note that in some cases the returned iterator may be different
  // from input_iter. In such case the returned iterator should be freed.
  //
  // Note: ErrorIterator with Status::Incomplete shall be returned if all the
  // following conditions are met:
  //  1. We enabled table_options.cache_index_and_filter_blocks.
  //  2. index is not present in block cache.
  //  3. We disallowed any io to be performed, that is, read_options ==
  //     kBlockCacheTier
  HnswIndexBlockIter* NewIndexIterator(
      const ReadOptions& read_options, HnswIndexBlockIter* input_iter,
      GetContext* get_context, BlockCacheLookupContext* lookup_context) const;

  template <typename TBlocklike>
  Cache::Priority GetCachePriority() const;

  // Read block cache from block caches (if set): block_cache.
  // On success, Status::OK with be returned and @block will be populated with
  // pointer to the block as well as its block handle.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> GetDataBlockFromCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* block, GetContext* get_context) const;

  // Put a maybe compressed block to the corresponding block caches.
  // This method will perform decompression against block_contents if needed
  // and then populate the block caches.
  // On success, Status::OK will be returned; also @block will be populated with
  // uncompressed block and its cache handle.
  //
  // Allocated memory managed by block_contents will be transferred to
  // PutDataBlockToCache(). After the call, the object will be invalid.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> PutDataBlockToCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* cached_block, BlockContents&& block_contents,
      CompressionType block_comp_type,
      const UncompressionDict& uncompression_dict,
      MemoryAllocator* memory_allocator, GetContext* get_context) const;

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  friend class HnswTableBuilder;

  // Create a index reader based on the index type stored in the table.
  // Optionally, user can pass a preloaded meta_index_iter for the index that
  // need to access extra meta blocks for index construction. This parameter
  // helps avoid re-reading meta index block if caller already created one.
  Status CreateIndexReader(const ReadOptions& ro,
                           FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* preloaded_meta_index_iter,
                           bool use_cache, bool prefetch, bool pin,
                           BlockCacheLookupContext* lookup_context,
                           std::unique_ptr<IndexReader>* index_reader);

  // If force_direct_prefetch is true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  static Status PrefetchTail(
      const ReadOptions& ro, RandomAccessFileReader* file, uint64_t file_size,
      bool force_direct_prefetch, TailPrefetchStats* tail_prefetch_stats,
      bool prefetch_all, bool preload_all,
      std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer, Statistics* stats,
      uint64_t tail_size, Logger* logger);
  Status ReadMetaIndexBlock(const ReadOptions& ro,
                            FilePrefetchBuffer* prefetch_buffer,
                            std::unique_ptr<Block>* metaindex_block,
                            std::unique_ptr<InternalIterator>* iter);
  Status ReadHnswBlock(const ReadOptions& ro,
                       FilePrefetchBuffer* prefetch_buffer, bool use_cache,
                       bool prefetch, bool pin,
                       BlockCacheLookupContext* lookup_context,
                       std::unique_ptr<HnswReader>* hnsw_reader);
  Status ReadPropertiesBlock(const ReadOptions& ro,
                             FilePrefetchBuffer* prefetch_buffer,
                             InternalIterator* meta_iter,
                             SequenceNumber largest_seqno);
  Status PrefetchIndexAndFilterBlocks(const ReadOptions& ro,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      InternalIterator* meta_iter,
                                      HnswTable* new_table, bool prefetch_all,
                                      const HnswTableOptions& table_options,
                                      int level, size_t file_size,
                                      size_t max_file_size_for_l0_meta_pin,
                                      BlockCacheLookupContext* lookup_context);

  static BlockType GetBlockTypeForMetaBlockByName(const Slice& meta_block_name);

  Status VerifyChecksumInMetaBlocks(const ReadOptions& read_options,
                                    InternalIteratorBase<Slice>* index_iter);
  Status VerifyChecksumInBlocks(const ReadOptions& read_options,
                                InternalIteratorBase<BlockHandle>* index_iter);

  // Size of all data blocks, maybe approximate
  uint64_t GetApproximateDataSize();

  // Given an iterator return its offset in data block section of file.
  uint64_t ApproximateDataOffsetOf(
      const InternalIteratorBase<BlockHandle>& index_iter,
      uint64_t data_size) const;

  // Helper functions for DumpTable()
  Status DumpIndexBlock(std::ostream& out_stream);
  Status DumpHnswBlock(std::ostream& out_stream);
  Status DumpDataBlocks(std::ostream& out_stream);
  static void DumpKeyValue(const Slice& key, const Slice& value,
                           std::ostream& out_stream);
};

// Stores all the properties associated with a HnswTable.
// These are immutable.
struct HnswTable::Rep {
  Rep(const ImmutableOptions& _ioptions, const EnvOptions& _env_options,
      const HnswTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator, uint64_t _file_size,
      int _level, const bool _immortal_table)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        internal_comparator(_internal_comparator),
        index_type(HnswTableOptions::IndexType::kBinarySearch),
        global_seqno(kDisableGlobalSequenceNumber),
        file_size(_file_size),
        level(_level),
        immortal_table(_immortal_table) {}
  ~Rep() { status.PermitUncheckedError(); }
  const ImmutableOptions& ioptions;
  const EnvOptions& env_options;
  const HnswTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  Status status;
  std::unique_ptr<RandomAccessFileReader> file;
  OffsetableCacheKey base_cache_key;
  PersistentCacheOptions persistent_cache_options;

  // Footer contains the fixed table information
  Footer footer;

  std::unique_ptr<IndexReader> index_reader;
  std::unique_ptr<HnswReader> hnsw_reader;
  std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;

  BlockHandle hnsw_handle;
  BlockHandle compression_dict_handle;

  std::shared_ptr<const TableProperties> table_properties;
  HnswTableOptions::IndexType index_type;

  // FIXME
  // If true, data blocks in this file are definitely ZSTD compressed. If false
  // they might not be. When false we skip creating a ZSTD digested
  // uncompression dictionary. Even if we get a false negative, things should
  // still work, just not as quickly.
  BlockCreateContext create_context;

  // If global_seqno is used, all Keys in this file will have the same
  // seqno with value `global_seqno`.
  //
  // A value of kDisableGlobalSequenceNumber means that this feature is disabled
  // and every key have it's own seqno.
  SequenceNumber global_seqno;

  // Size of the table file on disk
  uint64_t file_size;

  // the level when the table is opened, could potentially change when trivial
  // move is involved
  int level;

  // the timestamp range of table
  // Points into memory owned by TableProperties. This would need to change if
  // TableProperties become subject to cache eviction.
  Slice min_timestamp;
  Slice max_timestamp;

  // If false, blocks in this file are definitely all uncompressed. Knowing this
  // before reading individual blocks enables certain optimizations.
  bool blocks_maybe_compressed = true;

  const bool immortal_table;

  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      table_reader_cache_res_handle = nullptr;

  SequenceNumber get_global_seqno(BlockType block_type) const {
    return (block_type == BlockType::kFilterPartitionIndex ||
            block_type == BlockType::kCompressionDictionary)
               ? kDisableGlobalSequenceNumber
               : global_seqno;
  }

  uint64_t cf_id_for_tracing() const {
    return table_properties
               ? table_properties->column_family_id
               : TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  }

  Slice cf_name_for_tracing() const {
    return table_properties ? table_properties->column_family_name
                            : BlockCacheTraceHelper::kUnknownColumnFamilyName;
  }

  uint32_t level_for_tracing() const { return level >= 0 ? level : UINT32_MAX; }

  uint64_t sst_number_for_tracing() const {
    return file ? TableFileNameToNumber(file->file_name()) : UINT64_MAX;
  }
  void CreateFilePrefetchBuffer(
      size_t readahead_size, size_t max_readahead_size,
      std::unique_ptr<FilePrefetchBuffer>* fpb, bool implicit_auto_readahead,
      uint64_t num_file_reads,
      uint64_t num_file_reads_for_auto_readahead) const {
    *fpb = std::make_unique<FilePrefetchBuffer>(
        readahead_size, max_readahead_size,
        !ioptions.allow_mmap_reads /* enable */, false /* track_min_offset */,
        implicit_auto_readahead, num_file_reads,
        num_file_reads_for_auto_readahead, ioptions.fs.get(), ioptions.clock,
        ioptions.stats);
  }

  void CreateFilePrefetchBufferIfNotExists(
      size_t readahead_size, size_t max_readahead_size,
      std::unique_ptr<FilePrefetchBuffer>* fpb, bool implicit_auto_readahead,
      uint64_t num_file_reads,
      uint64_t num_file_reads_for_auto_readahead) const {
    if (!(*fpb)) {
      CreateFilePrefetchBuffer(readahead_size, max_readahead_size, fpb,
                               implicit_auto_readahead, num_file_reads,
                               num_file_reads_for_auto_readahead);
    }
  }

  std::size_t ApproximateMemoryUsage() const {
    std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<HnswTable::Rep*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }
};

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
