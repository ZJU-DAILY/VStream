//
// Created by shb on 23-11-13.
//

#ifndef JAVA_ROCKSJNI_VECTOR_PORTAL_H_
#define JAVA_ROCKSJNI_VECTOR_PORTAL_H_

#include "plugin/vectorbackend/db/vector_columnfamily.h"
#include "portal.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class VectorCFDescriptorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.VectorCFDescriptor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/VectorCFDescriptor");
  }

  /**
   * Create a new Java org.rocksdb.VectorCFDescriptor object with the same
   * properties as the provided C++
   * ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorCFDescriptor object
   *
   * @param env A pointer to the Java environment
   * @param cfd A pointer to
   * ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorCFDescriptor object
   *
   * @return A reference to a Java org.rocksdb.VectorCFDescriptor object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, VectorCFDescriptor* cfd);

  /**
   * Get the Java Method: VectorCFDescriptor#getName
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: VectorCFDescriptor#getOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "getOptions", "()Lorg/rocksdb/VectorColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.VectorColumnFamilyOptions
class VectorColumnFamilyOptionsJni
    : public RocksDBNativeClass<VectorColumnFamilyOptions*,
                                VectorColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.VectorColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(
        env, "org/rocksdb/VectorColumnFamilyOptions");
  }

  /**
   * Create a new Java org.rocksdb.VectorColumnFamilyOptions object with the
   * same properties as the provided C++ VectorColumnFamilyOptions object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to VectorColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.VectorColumnFamilyOptions object,
   * or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
                           const VectorColumnFamilyOptions* cfoptions);
};

}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE
#endif  // JAVA_ROCKSJNI_VECTOR_PORTAL_H_