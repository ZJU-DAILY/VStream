//
// Created by shb on 23-11-13.
//

#include "vector_portal.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
jobject VectorCFDescriptorJni::construct(JNIEnv* env, VectorCFDescriptor* cfd) {
  jbyteArray jcf_name = JniUtil::copyBytes(env, cfd->name);
  jobject cfopts =
      VectorColumnFamilyOptionsJni::construct(env, &(cfd->options));

  jclass jclazz = getJClass(env);
  if (jclazz == nullptr) {
    // exception occurred accessing class
    return nullptr;
  }

  jmethodID mid = env->GetMethodID(
      jclazz, "<init>", "([BLorg/rocksdb/VectorColumnFamilyOptions;)V");
  if (mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    env->DeleteLocalRef(jcf_name);
    return nullptr;
  }

  jobject jcfd = env->NewObject(jclazz, mid, jcf_name, cfopts);
  if (env->ExceptionCheck()) {
    env->DeleteLocalRef(jcf_name);
    return nullptr;
  }

  return jcfd;
}

jobject VectorColumnFamilyOptionsJni::construct(
    JNIEnv* env, const VectorColumnFamilyOptions* cfoptions) {
  auto* cfo = new VectorColumnFamilyOptions(*cfoptions);
  jclass jclazz = getJClass(env);
  if (jclazz == nullptr) {
    // exception occurred accessing class
    return nullptr;
  }

  jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
  if (mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return nullptr;
  }

  jobject jcfd = env->NewObject(jclazz, mid, GET_CPLUSPLUS_POINTER(cfo));
  if (env->ExceptionCheck()) {
    return nullptr;
  }

  return jcfd;
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE