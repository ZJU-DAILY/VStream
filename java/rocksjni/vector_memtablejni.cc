// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for MemTables.

//
// Created by shb on 23-11-25.
//

#include "include/org_rocksdb_HnswMemTableConfig.h"
#include "plugin/vectorbackend/memtable/hnsw_memtable_factory.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswlib.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_HnswMemTableConfig
 * Method:    newMemTableFactoryHandle0
 * Signature: (JBJJJJJZ)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_HnswMemTableConfig_newMemTableFactoryHandle0(
    JNIEnv *env, jobject /*jobj*/, jlong jdim, jbyte jspace_type,
    jlong jmax_elements, jlong jm, jlong jef_construction, jlong jrandom_seed,
    jlong jvisit_list_pool_size, jboolean jallow_replace_deleted) {
  ROCKSDB_NAMESPACE::Status statusDim =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jdim);
  ROCKSDB_NAMESPACE::Status statusMaxElements =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jmax_elements);
  ROCKSDB_NAMESPACE::Status statusM =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jm);
  ROCKSDB_NAMESPACE::Status statusEfConstruction =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jef_construction);
  ROCKSDB_NAMESPACE::Status statusRandomSeed =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jrandom_seed);
  ROCKSDB_NAMESPACE::Status statusVisitListPoolSize =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
          jvisit_list_pool_size);
  if (statusDim.ok() && statusMaxElements.ok() && statusM.ok() &&
      statusEfConstruction.ok() && statusRandomSeed.ok() &&
      statusVisitListPoolSize.ok()) {
    ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::HnswOptions hnsw_opts;
    hnsw_opts.dim = static_cast<size_t>(jdim);
    switch (jspace_type) {
      case 0x0:
        hnsw_opts.space = hnswlib::SpaceType::L2;
        break;
      case 0x1:
        hnsw_opts.space = hnswlib::SpaceType::IP;
        break;
      default:
        return 0;
    }
    hnsw_opts.max_elements = static_cast<size_t>(jmax_elements);
    hnsw_opts.M = static_cast<size_t>(jm);
    hnsw_opts.ef_construction = static_cast<size_t>(jef_construction);
    hnsw_opts.random_seed = static_cast<size_t>(jrandom_seed);
    hnsw_opts.visit_list_pool_size = static_cast<size_t>(jvisit_list_pool_size);
    hnsw_opts.allow_replace_deleted = jallow_replace_deleted;
    return GET_CPLUSPLUS_POINTER(
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::NewHnswMemTableFactory(
            hnsw_opts));
  }
  if (!statusDim.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, statusDim);
  }
  if (!statusMaxElements.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env,
                                                             statusMaxElements);
  }
  if (!statusM.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, statusM);
  }
  if (!statusEfConstruction.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env, statusEfConstruction);
  }
  if (!statusRandomSeed.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env,
                                                             statusRandomSeed);
  }
  if (!statusVisitListPoolSize.ok()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env, statusVisitListPoolSize);
  }
  return 0;
}