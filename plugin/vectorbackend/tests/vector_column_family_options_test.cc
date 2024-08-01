//
// Created by shb on 23-11-20.
//

#include "plugin/vectorbackend/memtable/hnsw_memtable_factory.h"
#include "plugin/vectorbackend/options/vector_options.h"
#include "plugin/vectorbackend/options/vector_options_helper.h"
#include "plugin/vectorbackend/table/hnsw_table_factory.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswalg.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

bool putVector_helper(DB *db, const WriteOptions &write_options,
                      ColumnFamilyHandle *vcf_handle,
                      ColumnFamilyHandle *cf_handle, Slice &key, Slice &value) {
  Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    s = db->Put(write_options, vcf_handle, key, value);
    if (s.ok()) {
      auto sequence_number =
          *reinterpret_cast<const SequenceNumber *>(s.getState());
      sequence_number =
          PackSequenceAndType(sequence_number, ValueType::kTypeValue);
      Slice seqno_slice(reinterpret_cast<const char *>(&sequence_number),
                        sizeof(SequenceNumber));
      s = db->Put(write_options, cf_handle, key, seqno_slice);
    }
  }

  if (s.ok()) {
    return true;
  } else {
    return false;
  }
}

Status vectorSearch_helper(DB *db,
                           const VectorSearchOptions &vectorSearchOptions,
                           ColumnFamilyHandle *vcf_handle,
                           ColumnFamilyHandle *cf_handle, Slice &key,
                           std::string *result, size_t *result_len) {
  uint16_t real_result_size = 0;
  Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    std::string value_str;
    s = db->Get(vectorSearchOptions, vcf_handle, key, &value_str);
    if (s.ok()) {
      const char *data = value_str.data();
      uint16_t result_size;
      memcpy(&result_size, data, sizeof(uint16_t));
      data += sizeof(uint16_t);
      result->reserve(result_size * (sizeof(uint64_t) + sizeof(float)));
      float dist;
      uint64_t label;
      SequenceNumber seqno;
      std::string stored_version_str;
      SequenceNumber stored_version;
      uint64_t stored_seq;
      ValueType type;
      for (uint16_t i = 0; i < result_size; i++) {
        memcpy(&dist, data, sizeof(float));
        data += sizeof(float);
        memcpy(&label, data, sizeof(uint64_t));
        data += sizeof(uint64_t);
        memcpy(&seqno, data, sizeof(SequenceNumber));
        data += sizeof(SequenceNumber);
        data += sizeof(uint64_t);
        s = db->Get(vectorSearchOptions, cf_handle,
                    Slice(reinterpret_cast<char *>(&label), sizeof(uint64_t)),
                    &stored_version_str);
        if (s.ok()) {
          memcpy(&stored_version, stored_version_str.data(), sizeof(uint64_t));
          UnPackSequenceAndType(stored_version, &stored_seq, &type);
          if (type == ValueType::kTypeValue && stored_seq == seqno) {
            PutFixed64(result, label);
            PutFixed32(result, *reinterpret_cast<uint32_t *>(&dist));
            ++real_result_size;
          }
        }
      }
    }
  }

  *result_len = real_result_size * (sizeof(uint64_t) + sizeof(float));

  return s;
}

class VectorColumnFamilyOptionsTest : public testing::Test {
 private:
  VectorColumnFamilyOptions options;

 public:
  VectorColumnFamilyOptions InitializeOptions() {
    HnswOptions hnsw_opts;
    hnsw_opts.dim = 2;
    hnsw_opts.space = hnswlib::SpaceType::L2;
    hnsw_opts.M = 10;
    HnswTableOptions hnsw_tbl_opts(hnsw_opts);
    options.space = hnswlib::SpaceType::L2;
    options.M = 10;
    options.dim = 2;
    options.memtable_factory.reset(NewHnswMemTableFactory(hnsw_opts));
    options.table_factory.reset(NewHnswTableFactory(hnsw_tbl_opts));
    return options;
  }
};

TEST_F(VectorColumnFamilyOptionsTest, copyConstructor) {
  VectorColumnFamilyOptions options;
  options.num_levels = 1;
  options.max_elements = 100, 000;
  options.M = 20;
  options.ef_construction = 20;
  options.random_seed = 114514;
  options.visit_list_pool_size = 2;
  options.termination_threshold = 100.0;
  options.termination_weight = 0.2;
  options.dim = 256;
  options.space = hnswlib::SpaceType::L2;
  options.allow_replace_deleted = true;
  VectorColumnFamilyOptions copyOpts = VectorColumnFamilyOptions(options);
  ASSERT_EQ(options.num_levels, copyOpts.num_levels);
  ASSERT_EQ(options.max_elements, copyOpts.max_elements);
  ASSERT_EQ(options.M, copyOpts.M);
  ASSERT_EQ(options.ef_construction, copyOpts.ef_construction);
  ASSERT_EQ(options.random_seed, copyOpts.random_seed);
  ASSERT_EQ(options.visit_list_pool_size, copyOpts.visit_list_pool_size);
  ASSERT_EQ(options.termination_threshold, copyOpts.termination_threshold);
  ASSERT_EQ(options.termination_weight, copyOpts.termination_weight);
  ASSERT_EQ(options.dim, copyOpts.dim);
  ASSERT_EQ(options.space, copyOpts.space);
  ASSERT_EQ(options.allow_replace_deleted, copyOpts.allow_replace_deleted);
}

TEST_F(VectorColumnFamilyOptionsTest, getVectorColumnFamilyOptionsFromProps) {
  std::string opt_string = "M=13;random_seed=112;";
  auto *vcf_options = new VectorColumnFamilyOptions();
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  Status status = GetVectorColumnFamilyOptionsFromString(
      config_options,
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions(),
      opt_string, vcf_options);
  ASSERT_OK(status);
  assert(vcf_options != nullptr);
  ASSERT_EQ(13, vcf_options->M);
  ASSERT_EQ(112, vcf_options->random_seed);
}

TEST_F(VectorColumnFamilyOptionsTest, failGetDisposedCF) {
  auto cfDescriptors = std::vector<ColumnFamilyDescriptor>{
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()),
      ColumnFamilyDescriptor("new_cf", ColumnFamilyOptions())};
  auto columnFaimlyHnaldeList = std::vector<ColumnFamilyHandle *>();
  DBOptions options = DBOptions();
  options.create_if_missing = true;
  DB *db;
  DB::Open(options, "test_db", cfDescriptors, &columnFaimlyHnaldeList, &db);
  db->DropColumnFamily(columnFaimlyHnaldeList[1]);
  std::string value;
  ASSERT_NOK(db->Get(ReadOptions(), columnFaimlyHnaldeList[1], "key", &value));
}

TEST_F(VectorColumnFamilyOptionsTest, createWriteDropVectorColumnFamily) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  DB *db;
  DB::Open(options, "test_db", &db);
  VectorColumnFamilyOptions vcf_opts = InitializeOptions();
  ColumnFamilyHandle *columnFamilyHandle;
  db->CreateColumnFamily(vcf_opts, "tmpVCF", &columnFamilyHandle);
  std::string vec_str;
  float vec[]{0.0f, 1.0f};
  for (size_t offset = 0; offset < 2; offset++) {
    PutFixed32(&vec_str, *reinterpret_cast<uint32_t *>(vec + offset));
  }
  std::string key_str;
  PutFixed64(&key_str, 1);
  Slice key(key_str.c_str(), sizeof(size_t));
  Slice value(vec_str.c_str(), 2 * sizeof(float));
  Status s = db->Put(WriteOptions(), columnFamilyHandle, key, value);
  std::cout << "seq: " << DecodeFixed64(s.getState()) << std::endl;

  std::string vec_str2;
  float vec2[]{1.0f, 2.0f};
  for (size_t offset = 0; offset < 2; offset++) {
    PutFixed32(&vec_str, *reinterpret_cast<uint32_t *>(vec2 + offset));
  }
  std::string key_str2;
  PutFixed64(&key_str, 1);
  Slice key2(key_str2.c_str(), sizeof(size_t));
  Slice value2(vec_str2.c_str(), 2 * sizeof(float));
  Status s2 = db->Put(WriteOptions(), columnFamilyHandle, key2, value2);
  std::cout << "seq2: " << DecodeFixed64(s2.getState()) << std::endl;
  db->DropColumnFamily(columnFamilyHandle);
  db->Close();
}

TEST_F(VectorColumnFamilyOptionsTest, simpleTest) {
  char buf[100]{0};
  std::string key_str;
  float value = -1.0f;
  memcpy(buf, &value, sizeof(value));
  Slice key(buf, sizeof(value));
  key_str.assign(buf, sizeof(value));
  uint32_t value_int = *reinterpret_cast<uint32_t *>(&value);
  PutFixed32(&key_str, value_int);
  PutFixed32(&key_str, 1);
  std::string vec_str;
  float vec[]{0.0f, 1.0f};
  for (size_t offset = 0; offset < 2; offset++) {
    PutFixed32(&vec_str, *reinterpret_cast<uint32_t *>(vec + offset));
  }
}

TEST_F(VectorColumnFamilyOptionsTest, vectorSearchTest) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  DB *db;
  DB::Open(options, "test_db", &db);
  VectorColumnFamilyOptions vcf_opts = InitializeOptions();
  ColumnFamilyHandle *vectorColumnFamilyHandle;
  ColumnFamilyHandle *columnFamilyHandle;
  db->CreateColumnFamily(vcf_opts, "tmpVCF", &vectorColumnFamilyHandle);
  db->CreateColumnFamily(ColumnFamilyOptions(), "tmpVCF-ext",
                         &columnFamilyHandle);
  std::string vec_str;
  float vec[]{0.0f, 1.0f};
  for (size_t offset = 0; offset < 2; offset++) {
    PutFixed32(&vec_str, *reinterpret_cast<uint32_t *>(vec + offset));
  }
  std::string key_str;
  PutFixed64(&key_str, 1);
  Slice key(key_str.c_str(), sizeof(size_t));
  Slice value(vec_str.c_str(), 2 * sizeof(float));
  putVector_helper(db, WriteOptions(), vectorColumnFamilyHandle,
                   columnFamilyHandle, key, value);

  std::string query_str;
  float query_vec[]{0.0f, 1.0f};
  for (size_t offset = 0; offset < 2; offset++) {
    PutFixed32(&query_str, *reinterpret_cast<uint32_t *>(query_vec + offset));
  }
  Slice query(query_str.c_str(), 2 * sizeof(float));
  std::string search_value;
  size_t search_value_len;
  vectorSearch_helper(db, VectorSearchOptions(), vectorColumnFamilyHandle,
                      columnFamilyHandle, query, &search_value, &search_value_len);
  uint16_t result_size =
      search_value_len/ (sizeof(float) + sizeof(uint64_t));
  const char *data = search_value.data();
  std::cout << "result size: " << result_size << std::endl;
  for (uint16_t i = 0; i < result_size; i++) {
    float dist;
    uint64_t label;
    memcpy(&label, data, sizeof(label));
    data += sizeof(label);
    memcpy(&dist, data, sizeof(dist));
    data += sizeof(dist);
    std::cout << "label: " << label << std::endl
              << "dist: " << dist << std::endl;
  }
  db->DropColumnFamily(columnFamilyHandle);
  db->DropColumnFamily(vectorColumnFamilyHandle);
  db->Close();
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE

int main(int argc, char **argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}