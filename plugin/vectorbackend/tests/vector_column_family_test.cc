//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "options/options_parser.h"
#include "plugin/vectorbackend/db/vector_columnfamily.h"
#include "plugin/vectorbackend/memtable/hnsw_memtablerep.h"
#include "plugin/vectorbackend/options/vector_options.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "utilities/fault_injection_env.h"
#include "utils/config.h"
#include "utils/serialize.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {

static const int kValueSize = 1000;
class EnvCounter : public SpecialEnv {
 public:
  // counts how many operations were performed
  explicit EnvCounter(Env* base)
      : SpecialEnv(base), num_new_writable_file_(0) {}
  int GetNumberOfNewWritableFileCalls() { return num_new_writable_file_; }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    ++num_new_writable_file_;
    return EnvWrapper::NewWritableFile(f, r, soptions);
  }

 private:
  std::atomic<int> num_new_writable_file_;
};

class VectorColumnFamilyTestBase : public testing::Test {
 public:
  explicit VectorColumnFamilyTestBase()
      : rnd_(139),
        config_(Config::parse("/home/shb/Projects/VStream/plugin/vectorbackend/"
                              "tests/config.toml")) {
    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(nullptr, base_env);
    env_ = new EnvCounter(base_env);
    env_->skip_fsync_ = true;
    dbname_ = test::PerThreadDBPath("vector_column_family_test");
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
//    db_options_.avoid_flush_during_recovery = true;
//    db_options_.avoid_flush_during_shutdown = true;
    db_options_.env = env_;
    column_family_options_ = config_->vcf_opts();
    EXPECT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_)));
  }

  ~VectorColumnFamilyTestBase() override {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (auto h : cf_handles_) {
      ColumnFamilyDescriptor cfdescriptor;
      Status s = h->GetDescriptor(&cfdescriptor);
      EXPECT_OK(s);
      column_families.push_back(cfdescriptor);
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    Destroy(column_families);
    delete env_;
  }

  void Close() {
    for (auto h : vcf_handles_) {
      if (h) {
        ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
      }
    }
    for (auto h : cf_handles_) {
      if (h) {
        ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
      }
    }
    cf_handles_.clear();
    cf_names_.clear();
    vcf_handles_.clear();
    vcf_names_.clear();
    ex_cf_handle_map_.clear();
    delete db_;
    db_ = nullptr;
  }

  Status TryOpen(const std::vector<std::string>& cf_names,
                 const std::vector<ColumnFamilyOptions>& cf_opts = {},
                 const std::vector<std::string>& vcf_names = {},
                 const std::vector<VectorColumnFamilyOptions>& vcf_opts = {},
                 const std::vector<ColumnFamilyOptions>& ex_cf_opts = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<VectorCFDescriptor> vector_column_families;
    cf_names_.clear();
    cf_names_.reserve(cf_names.size() + vcf_names.size());
    vcf_names_.clear();
    vcf_names_.reserve(vcf_names.size());
    for (size_t i = 0; i < cf_names.size(); ++i) {
      column_families.emplace_back(
          cf_names[i], cf_opts.empty() ? ColumnFamilyOptions() : cf_opts[i]);
      cf_names_.emplace_back(cf_names[i]);
    }
    for (size_t i = 0; i < vcf_names.size(); ++i) {
      vector_column_families.emplace_back(
          vcf_names[i],
          vcf_opts.empty() ? VectorColumnFamilyOptions() : vcf_opts[i]);
      vcf_names_.emplace_back(vcf_names[i]);
      column_families.emplace_back(
          vcf_names[i] + vcf_extension_,
          ex_cf_opts.empty() ? column_family_options_ : ex_cf_opts[i]);
    }
    Status s = DB::Open(db_options_, dbname_, column_families, &cf_handles_, &db_,
                    vector_column_families, &vcf_handles_);
    if (s == Status::OK()) {
      for (size_t i = 0; i < vcf_names.size(); ++i) {
        ex_cf_handle_map_[vcf_handles_[i]] = cf_handles_[cf_names_.size() + i];
      }
    }
    return s;
  }

  void Open(const std::vector<std::string>& cf_names,
            const std::vector<ColumnFamilyOptions>& cf_opts = {},
            const std::vector<std::string>& vcf_names = {},
            const std::vector<VectorColumnFamilyOptions>& vcf_opts = {},
            const std::vector<ColumnFamilyOptions>& ex_cf_opts = {}) {
    ASSERT_OK(TryOpen(cf_names, cf_opts, vcf_names, vcf_opts, ex_cf_opts));
  }

  void Open() { Open({"default"}); }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_); }

  void Destroy(const std::vector<ColumnFamilyDescriptor>& column_families =
                   std::vector<ColumnFamilyDescriptor>()) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_),
                        column_families));
  }

  void CreateVectorColumnFamily(
      const std::string& vcf_name, ColumnFamilyHandle** vcf_handle,
      const VectorColumnFamilyOptions* vcf_opts = nullptr,
      const ColumnFamilyOptions* ex_cf_opts = nullptr) {
    ASSERT_OK(db_->CreateColumnFamily(
        vcf_opts ? *vcf_opts : column_family_options_, vcf_name, vcf_handle));
    vcf_handles_.emplace_back(*vcf_handle);

    std::string ex_cf_name = vcf_name + vcf_extension_;
    ColumnFamilyHandle* ex_cf_handle;
    ASSERT_OK(db_->CreateColumnFamily(
        ex_cf_opts ? *ex_cf_opts : ColumnFamilyOptions(), ex_cf_name,
        &ex_cf_handle));
    cf_handles_.emplace_back(ex_cf_handle);
    ex_cf_handle_map_[*vcf_handle] = ex_cf_handle;
  }

  void Reopen(const std::vector<ColumnFamilyOptions>& options = {},
              const std::vector<VectorColumnFamilyOptions>& vcf_options = {},
              const std::vector<ColumnFamilyOptions>& ex_cf_options = {}) {
    std::vector<std::string> cf_names;
    std::vector<std::string> vcf_names;
    for (const auto& name : cf_names_) {
      if (!name.empty()) {
        cf_names.push_back(name);
        //        if (options.empty()) {
        //          options.emplace_back({});
        //        }
      }
    }
    for (const auto& name : vcf_names_) {
      if (!name.empty()) {
        vcf_names.push_back(name);
        cf_names.push_back(name + vcf_extension_);
      }
    }
    Close();
    Open(cf_names, options, vcf_names, vcf_options, ex_cf_options);
  }

  Status Flush(ColumnFamilyHandle* cf_handle) {
    return db_->Flush(FlushOptions(), cf_handle);
  }

  std::string Get(int cf, const std::string& key) {
    ReadOptions options;
    options.verify_checksums = true;
    std::string result;
    Status s = db_->Get(options, cf_handles_[cf], Slice(key), &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  const std::string vcf_extension_ = "_ex";
  std::vector<ColumnFamilyHandle*> cf_handles_;
  std::vector<std::string> cf_names_;
  std::vector<ColumnFamilyHandle*> vcf_handles_;
  std::vector<std::string> vcf_names_;
  std::unordered_map<ColumnFamilyHandle*, ColumnFamilyHandle*>
      ex_cf_handle_map_;
  VectorColumnFamilyOptions column_family_options_;
  std::unique_ptr<Config> config_;
  DBOptions db_options_;
  std::string dbname_;
  DB* db_ = nullptr;
  EnvCounter* env_;
  std::shared_ptr<Env> env_guard_;
  Random rnd_;
};

class VectorColumnFamilyTest
    : public VectorColumnFamilyTestBase,
      virtual public ::testing::WithParamInterface<uint32_t> {
 public:
  VectorColumnFamilyTest() : VectorColumnFamilyTestBase() {}
};

INSTANTIATE_TEST_CASE_P(FormatDef, VectorColumnFamilyTest,
                        testing::Values(test::kDefaultFormatVersion));
INSTANTIATE_TEST_CASE_P(FormatLatest, VectorColumnFamilyTest,
                        testing::Values(kLatestFormatVersion));

TEST_P(VectorColumnFamilyTest, TestVectorSearch) {
  Open();
  ColumnFamilyHandle* vcf_handle;
  CreateVectorColumnFamily("vcf", &vcf_handle, &column_family_options_);
  FlushOptions flushOptions;
  WriteOptions writeOptions;
  writeOptions.disableWAL = true;
  auto base_iterator = config_->GetIterator(config_->base_file());
  auto query_iteator = config_->GetIterator(config_->query_file());
  for (int i = 0; base_iterator.hasNext() && i < config_->vcf_opts().max_elements; i++) {
    auto base_vector = base_iterator.next();
    Status s =
        db_->Put(writeOptions, vcf_handle, longToBytes(base_vector.id()),
                 floatArrayToByteArrayWithTimestamp(base_vector.value(), 1));
    //    ASSERT_EQ(s, Status::OK());
    if ((i + 1) % 1000 == 0) {
      break;
      db_->Flush(flushOptions, vcf_handle);
      std::cout << "flush block" << std::endl;
    }
  }
  std::cout << "insert finish" << std::endl;
  for (int i = 0; query_iteator.hasNext(); i++) {
    //    uint64_t ts = rnd_.Next64() % 10000'000;
    uint64_t ts = 0;
    auto query_vector = query_iteator.next();
    std::string value;
    auto vso = config_->vs_opts();
    vso.ts = ts;
    Status s = db_->Get(vso, vcf_handle,
                        floatArrayToByteArray(query_vector.value()), &value);
    int result_size =
        value.size() / (sizeof(dist_t) + sizeof(labeltype) +
                        sizeof(SequenceNumber) + sizeof(uint64_t));
    std::cout << "query " << i << ", find " << result_size << std::endl;
  }
  Close();
  std::cout << "close" << std::endl;
  Open({"default"}, {ColumnFamilyOptions()}, {"vcf"}, {column_family_options_});
  std::cout << "reopen" << std::endl;
  query_iteator = config_->GetIterator(config_->query_file());
  for (int i = 0; query_iteator.hasNext(); i++) {
    //    uint64_t ts = rnd_.Next64() % 10000'000;
    uint64_t ts = 0;
    auto query_vector = query_iteator.next();
    std::string value;
    auto vso = config_->vs_opts();
    vso.ts = ts;
    Status s = db_->Get(vso, vcf_handles_[0],
                        floatArrayToByteArray(query_vector.value()), &value);
    int result_size =
        value.size() / (sizeof(dist_t) + sizeof(labeltype) +
                        sizeof(SequenceNumber) + sizeof(uint64_t));
    std::cout << "query " << i << ", find " << result_size << std::endl;
  }
  Destroy();
}

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
