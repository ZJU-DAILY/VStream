//
// Created by shb on 23-11-8.
//

#include <jni.h>

#include "include/org_rocksdb_VectorColumnFamilyHandle.h"
#include "rocksjni/vector_portal.h"

/*
 * Class:     org_rocksdb_VectorColumnFamilyHandle
 * Method:    getName
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_VectorColumnFamilyHandle_getName(
    JNIEnv* env, jobject /*jobj*/, jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  std::string cf_name = cfh->GetName();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, cf_name);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyHandle
 * Method:    getID
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_VectorColumnFamilyHandle_getID(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyHandle
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/VectorColumnFamilyDescriptor;
 */
JNIEXPORT jobject JNICALL
Java_org_rocksdb_VectorColumnFamilyHandle_getDescriptor(JNIEnv* env,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorCFDescriptor desc;
  ROCKSDB_NAMESPACE::Status s = cfh->GetDescriptor(&desc);
  if (s.ok()) {
    return ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorCFDescriptorJni::
        construct(env, &desc);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyHandle
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_rocksdb_VectorColumnFamilyHandle_disposeInternal(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  assert(cfh != nullptr);
  delete cfh;
}