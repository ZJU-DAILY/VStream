// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::DB methods from Java side.

#include <string>

#include "include/org_rocksdb_RocksDB.h"
#include "plugin/vectorbackend/db/vector_columnfamily.h"
#include "plugin/vectorbackend/memtable/hnsw_memtablerep.h"
#include "plugin/vectorbackend/options/vector_options.h"
#include "rocksdb/db.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createVectorColumnFamily
 * Signature: (J[BIJ)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_RocksDB_createVectorColumnFamily(
    JNIEnv *env, jobject, jlong jhandle, jbyteArray jvcf_name,
    jint jvcf_name_len, jlong jvcf_options_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jhandle);
  jboolean has_exception = JNI_FALSE;
  const std::string vcf_name =
      ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jvcf_name, jvcf_name_len,
          [](const char *str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }
  auto *vcf_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jvcf_options_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle *cf_handle;
  ROCKSDB_NAMESPACE::Status s =
      db->CreateColumnFamily(*vcf_options, vcf_name, &cf_handle);
  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
  return GET_CPLUSPLUS_POINTER(cf_handle);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createVectorColumnFamilies
 * Signature: (JJ[[B)[J
 */
JNIEXPORT jlongArray JNICALL
Java_org_rocksdb_RocksDB_createVectorColumnFamilies__JJ_3_3B(
    JNIEnv *env, jobject, jlong jhandle, jlong jvcf_options_handle,
    jobjectArray jvcf_names) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jhandle);
  auto *vcf_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jvcf_options_handle);
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> vcf_names;
  ROCKSDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jvcf_names,
      [](const char *str, const size_t len) { return std::string(str, len); },
      [&vcf_names](const size_t, std::string str) { vcf_names.push_back(str); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle *> cf_handles;
  ROCKSDB_NAMESPACE::Status s =
      db->CreateColumnFamilies(*vcf_options, vcf_names, &cf_handles);
  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jcf_handles = ROCKSDB_NAMESPACE::JniUtil::toJPointers<
      ROCKSDB_NAMESPACE::ColumnFamilyHandle>(env, cf_handles, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createVectorColumnFamilies
 * Signature: (J[J[[B)[J
 */
JNIEXPORT jlongArray JNICALL
Java_org_rocksdb_RocksDB_createVectorColumnFamilies__J_3J_3_3B(
    JNIEnv *env, jobject, jlong jhandle, jlongArray jvcf_options_handles,
    jobjectArray jvcf_names) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jhandle);
  const jsize jlen = env->GetArrayLength(jvcf_options_handles);
  std::vector<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorCFDescriptor>
      vcf_descriptors;
  vcf_descriptors.reserve(jlen);

  jlong *jvcf_options_handles_elems =
      env->GetLongArrayElements(jvcf_options_handles, nullptr);
  if (jvcf_options_handles_elems == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  // extract the column family descriptors
  jboolean has_exception = JNI_FALSE;
  for (jsize i = 0; i < jlen; i++) {
    auto *vcf_options = reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions
            *>(jvcf_options_handles_elems[i]);
    auto jcf_name =
        static_cast<jbyteArray>(env->GetObjectArrayElement(jvcf_names, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jvcf_options_handles,
                                    jvcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }
    const auto vcf_name = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jcf_name,
        [](const char *str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jcf_name);
      env->ReleaseLongArrayElements(jvcf_options_handles,
                                    jvcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }

    vcf_descriptors.emplace_back(vcf_name, *vcf_options);

    env->DeleteLocalRef(jcf_name);
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle *> cf_handles;
  ROCKSDB_NAMESPACE::Status s =
      db->CreateColumnFamilies(vcf_descriptors, &cf_handles);

  env->ReleaseLongArrayElements(jvcf_options_handles,
                                jvcf_options_handles_elems, JNI_ABORT);

  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jcf_handles = ROCKSDB_NAMESPACE::JniUtil::toJPointers<
      ROCKSDB_NAMESPACE::ColumnFamilyHandle>(env, cf_handles, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/**
 * @return true if the put succeeded, false if a Java Exception was thrown
 */
bool rocksdb_putVector_helper(
    JNIEnv *env, ROCKSDB_NAMESPACE::DB *db,
    const ROCKSDB_NAMESPACE::WriteOptions &write_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *vcf_handle,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *cf_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len, jbyteArray jval, jint jval_off,
    jint jval_len) {
  jbyte *key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }

  jbyte *value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key), jkey_len);
  ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char *>(value),
                                       jval_len);

  ROCKSDB_NAMESPACE::Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    s = db->Put(write_options, vcf_handle, key_slice, value_slice);
    if (s.ok()) {
      auto sequence_number =
          *reinterpret_cast<const ROCKSDB_NAMESPACE::SequenceNumber *>(
              s.getState());
      sequence_number = ROCKSDB_NAMESPACE::PackSequenceAndType(
          sequence_number, ROCKSDB_NAMESPACE::ValueType::kTypeValue);
      ROCKSDB_NAMESPACE::Slice seqno_slice(
          reinterpret_cast<const char *>(&sequence_number),
          sizeof(ROCKSDB_NAMESPACE::SequenceNumber));
      ROCKSDB_NAMESPACE::WriteOptions version_write_options;
      version_write_options.disableWAL = true;
      s = db->Put(version_write_options, cf_handle, key_slice, seqno_slice);
    }
  } else {
    s = ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "Invalid ColumnFamilyHandle.");
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return false;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    putVector
 * Signature: (J[BII[BIIJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksDB_putVector__J_3BII_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jbyteArray jkey, jint jkey_off,
    jint jkey_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jvcf_handle, jlong jcf_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    rocksdb_putVector_helper(env, db, default_write_options, vcf_handle,
                             cf_handle, jkey, jkey_off, jkey_len, jval,
                             jval_off, jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    putVector
 * Signature: (JJ[BII[BIIJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksDB_putVector__JJ_3BII_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jvcf_handle, jlong jcf_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto *write_options = reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions *>(
      jwrite_options_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    rocksdb_putVector_helper(env, db, *write_options, vcf_handle, cf_handle,
                             jkey, jkey_off, jkey_len, jval, jval_off,
                             jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/**
 * @return true if the delete succeeded, false if a Java Exception was thrown
 */
bool rocksdb_deleteVector_helper(
    JNIEnv *env, ROCKSDB_NAMESPACE::DB *db,
    const ROCKSDB_NAMESPACE::WriteOptions &write_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *vcf_handle,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *cf_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len) {
  jbyte *key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key), jkey_len);

  ROCKSDB_NAMESPACE::Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    s = db->Delete(write_options, vcf_handle, key_slice);
    if (s.ok()) {
      auto sequence_number =
          *reinterpret_cast<const ROCKSDB_NAMESPACE::SequenceNumber *>(
              s.getState());
      sequence_number = ROCKSDB_NAMESPACE::PackSequenceAndType(
          sequence_number, ROCKSDB_NAMESPACE::ValueType::kTypeDeletion);
      ROCKSDB_NAMESPACE::Slice seqno_slice(
          reinterpret_cast<const char *>(&sequence_number),
          sizeof(ROCKSDB_NAMESPACE::SequenceNumber));
      s = db->Put(write_options, cf_handle, key_slice, seqno_slice);
    }
  } else {
    s = ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "Invalid ColumnFamilyHandle.");
  }

  // cleanup
  delete[] key;

  if (s.ok()) {
    return true;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteVector
 * Signature: (J[BIIJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksDB_deleteVector__J_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jbyteArray jkey, jint jkey_off,
    jint jkey_len, jlong jvcf_handle, jlong jcf_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    rocksdb_deleteVector_helper(env, db, default_write_options, vcf_handle,
                                cf_handle, jkey, jkey_off, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}
/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteVector
 * Signature: (JJ[BIIJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksDB_deleteVector__JJ_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jvcf_handle,
    jlong jcf_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto *write_options = reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions *>(
      jwrite_options_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    rocksdb_deleteVector_helper(env, db, *write_options, vcf_handle, cf_handle,
                                jkey, jkey_off, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

using dist_t = float;

jbyteArray rocksdb_vectorSearch_helper(
    JNIEnv *env, ROCKSDB_NAMESPACE::DB *db,
    const ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions
        &vectorSearchOptions,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *vcf_handle,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *cf_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len) {
  auto *key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return nullptr;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key), jkey_len);

  std::string result;
  uint16_t real_result_size = 0;
  ROCKSDB_NAMESPACE::Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    std::string value_str;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    std::chrono::time_point start = std::chrono::high_resolution_clock::now();
    s = db->Get(vectorSearchOptions, vcf_handle, key_slice, &value_str);
    std::chrono::time_point end = std::chrono::high_resolution_clock::now();
    long elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       end - start).count();
    ROCKS_LOG_DEBUG(db->GetDBOptions().info_log,
                    "vectorSearchHelper: Get vector CF took %ld ms", elapsed);
    if (s.ok()) {
      const char *data = value_str.data();
      uint16_t result_size;
      if (value_str.size() == 0) {
        // empty result set
        result_size = 0;
      } else {
        memcpy(&result_size, data, sizeof(uint16_t));
      }
      data += sizeof(uint16_t);
      result.reserve(
          result_size *
          (sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype) +
           sizeof(float)));
      float dist;
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype label;
      ROCKSDB_NAMESPACE::SequenceNumber seqno;
      std::string stored_version_str;
      ROCKSDB_NAMESPACE::SequenceNumber stored_version;
      uint64_t stored_seq;
      ROCKSDB_NAMESPACE::ValueType type;
      for (uint16_t i = 0; i < result_size; i++) {
        memcpy(&dist, data, sizeof(float));
        data += sizeof(float);
        memcpy(&label, data,
               sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype));
        data += sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype);
        memcpy(&seqno, data, sizeof(ROCKSDB_NAMESPACE::SequenceNumber));
        data += sizeof(ROCKSDB_NAMESPACE::SequenceNumber);
        data += sizeof(uint64_t);
        start = std::chrono::high_resolution_clock::now();
        s = db->Get(
            read_options, cf_handle,
            ROCKSDB_NAMESPACE::Slice(
                reinterpret_cast<char *>(&label),
                sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype)),
            &stored_version_str);
        end = std::chrono::high_resolution_clock::now();
        elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                      end - start).count();
        ROCKS_LOG_DEBUG(db->GetDBOptions().info_log,
                        "vectorSearchHelper: Get version for result %d took %ld "
                        "ms", i, elapsed);
        if (s.ok()) {
          memcpy(&stored_version, stored_version_str.data(), sizeof(uint64_t));
          ROCKSDB_NAMESPACE::UnPackSequenceAndType(stored_version, &stored_seq,
                                                   &type);
          if (type == ROCKSDB_NAMESPACE::ValueType::kTypeValue &&
              stored_seq == seqno) {
            ROCKSDB_NAMESPACE::PutFixed64(&result, label);
            ROCKSDB_NAMESPACE::PutFixed32(&result,
                                          *reinterpret_cast<uint32_t *>(&dist));
            ++real_result_size;
          }
        }
      }
    }
  } else {
    s = ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "Invalid ColumnFamilyHandle.");
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env,
        ROCKSDB_NAMESPACE::Slice(
            result.data(),
            real_result_size *
                (sizeof(float) +
                 sizeof(
                     ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype))));
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

jint rocksdb_vectorSearch_helper(
    JNIEnv *env, ROCKSDB_NAMESPACE::DB *db,
    const ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions
        &vectorSearchOptions,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *vcf_handle,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *cf_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len, jbyteArray jval, jint jval_off, jint jval_len,
    bool *has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  auto *key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    delete[] key;
    *has_exception = true;
    return kStatusError;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key), jkey_len);

  std::string result;
  uint16_t real_result_size = 0;
  ROCKSDB_NAMESPACE::Status s;
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    std::string value_str;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    s = db->Get(vectorSearchOptions, vcf_handle, key_slice, &value_str);
    if (s.ok()) {
      const char *data = value_str.data();
      uint16_t result_size;
      memcpy(&result_size, data, sizeof(uint16_t));
      data += sizeof(uint16_t);
      result.reserve(
          result_size *
          (sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype) +
           sizeof(float)));
      float dist;
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype label;
      ROCKSDB_NAMESPACE::SequenceNumber seqno;
      std::string stored_version_str;
      ROCKSDB_NAMESPACE::SequenceNumber stored_version;
      uint64_t stored_seq;
      ROCKSDB_NAMESPACE::ValueType type;
      for (uint16_t i = 0; i < result_size; i++) {
        memcpy(&dist, data, sizeof(float));
        data += sizeof(float);
        memcpy(&label, data,
               sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype));
        data += sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype);
        memcpy(&seqno, data, sizeof(ROCKSDB_NAMESPACE::SequenceNumber));
        data += sizeof(ROCKSDB_NAMESPACE::SequenceNumber);
        data += sizeof(uint64_t);
        s = db->Get(
            read_options, cf_handle,
            ROCKSDB_NAMESPACE::Slice(
                reinterpret_cast<char *>(&label),
                sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype)),
            &stored_version_str);
        if (s.ok()) {
          memcpy(&stored_version, stored_version_str.data(), sizeof(uint64_t));
          ROCKSDB_NAMESPACE::UnPackSequenceAndType(stored_version, &stored_seq,
                                                   &type);
          if (type == ROCKSDB_NAMESPACE::ValueType::kTypeValue &&
              stored_seq == seqno) {
            ROCKSDB_NAMESPACE::PutFixed64(&result, label);
            ROCKSDB_NAMESPACE::PutFixed32(&result,
                                          *reinterpret_cast<uint32_t *>(&dist));
            ++real_result_size;
          }
        }
      }
    }
  } else {
    s = ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "Invalid ColumnFamilyHandle.");
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint pinnable_value_len = static_cast<jint>(
      real_result_size *
      (sizeof(float) +
       sizeof(ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::labeltype)));
  const jint length = std::min(jval_len, pinnable_value_len);

  env->SetByteArrayRegion(
      jval, jval_off, length,
      const_cast<jbyte *>(reinterpret_cast<const jbyte *>(result.data())));
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    *has_exception = true;
    return kStatusError;
  }

  *has_exception = false;
  return pinnable_value_len;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    vectorSearch
 * Signature: (J[BII[BIIJJ)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_RocksDB_vectorSearch__J_3BII_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jbyteArray jkey, jint jkey_off,
    jint jkey_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jvcf_handle, jlong jcf_handle) {
  auto *db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    bool has_exception = false;
    return rocksdb_vectorSearch_helper(
        env, db_handle,
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions(),
        vcf_handle, cf_handle, jkey, jkey_off, jkey_len, jval, jval_off,
        jval_len, &has_exception);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    vectorSearch
 * Signature: (JJ[BII[BIIJJ)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_RocksDB_vectorSearch__JJ_3BII_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jvec_search_opt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jvcf_handle, jlong jcf_handle) {
  auto *db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto &vec_search_opt = *reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jvec_search_opt_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    bool has_exception = false;
    return rocksdb_vectorSearch_helper(
        env, db_handle, vec_search_opt, vcf_handle, cf_handle, jkey, jkey_off,
        jkey_len, jval, jval_off, jval_len, &has_exception);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    vectorSearch
 * Signature: (J[BIIJJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_RocksDB_vectorSearch__J_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jbyteArray jkey, jint jkey_off,
    jint jkey_len, jlong jvcf_handle, jlong jcf_handle) {
  auto *db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    return rocksdb_vectorSearch_helper(
        env, db_handle,
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions(),
        vcf_handle, cf_handle, jkey, jkey_off, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    vectorSearch
 * Signature: (JJ[BIIJJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_RocksDB_vectorSearch__JJ_3BIIJJ(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jvec_search_opt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jvcf_handle,
    jlong jcf_handle) {
  auto *db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto &vec_search_opt = *reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jvec_search_opt_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  auto *cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jcf_handle);
  if (vcf_handle != nullptr && cf_handle != nullptr) {
    return rocksdb_vectorSearch_helper(env, db_handle, vec_search_opt,
                                       vcf_handle, cf_handle, jkey, jkey_off,
                                       jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    setVectorOptions
 * Signature: (JJ[Ljava/lang/String;[Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksDB_setVectorOptions(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jvcf_handle,
    jobjectArray jkeys, jobjectArray jvalues) {
  const jsize len = env->GetArrayLength(jkeys);
  assert(len == env->GetArrayLength(jvalues));

  std::unordered_map<std::string, std::string> options_map;
  for (jsize i = 0; i < len; i++) {
    jobject jobj_key = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return;
    }

    jobject jobj_value = env->GetObjectArrayElement(jvalues, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key);
      return;
    }

    jboolean has_exception = JNI_FALSE;
    std::string s_key = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_key), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_value = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_value), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    options_map[s_key] = s_value;

    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);
  assert(vcf_handle != nullptr);
  auto s = db->SetVectorOptions(vcf_handle, options_map);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getVectorOptions
 * Signature: (JJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_rocksdb_RocksDB_getVectorOptions(
    JNIEnv *env, jobject, jlong jdb_handle, jlong jvcf_handle) {
  auto *db = reinterpret_cast<ROCKSDB_NAMESPACE::DB *>(jdb_handle);

  assert(jvcf_handle != 0);
  auto *vcf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle *>(jvcf_handle);

  ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions vector_options;
  db->GetVectorOptions(vcf_handle, &vector_options);
  std::string options_as_string;
  ROCKSDB_NAMESPACE::ConfigOptions config_options;
  ROCKSDB_NAMESPACE::Status s = GetStringFromVectorColumnFamilyOptions(
      &options_as_string, vector_options);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
  return env->NewStringUTF(options_as_string.c_str());
}
