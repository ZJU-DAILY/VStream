//
// Created by shb on 23-10-30.
//

#include "plugin/vectorbackend/options/vector_options.h"

#include <jni.h>

#include "include/org_rocksdb_VectorColumnFamilyOptions.h"
#include "include/org_rocksdb_VectorOptions.h"
#include "include/org_rocksdb_VectorSearchOptions.h"
#include "plugin/vectorbackend/options/vector_options_helper.h"
#include "plugin/vectorbackend/vectorindex/hnswlib/hnswlib.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    newVectorOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_VectorOptions_newVectorOptions__(JNIEnv *, jclass) {
  auto *vop = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions();
  return GET_CPLUSPLUS_POINTER(vop);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    newVectorOptions
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_VectorOptions_newVectorOptions__JJ(JNIEnv *, jclass,
                                                          jlong jdboptions,
                                                          jlong jvcfoptions) {
  auto *dbOpt =
      reinterpret_cast<const ROCKSDB_NAMESPACE::DBOptions *>(jdboptions);
  auto *vcfOpt =
      reinterpret_cast<const ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                           VectorColumnFamilyOptions *>(jvcfoptions);
  auto *vop = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions(
      *dbOpt, *vcfOpt);
  return GET_CPLUSPLUS_POINTER(vop);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    copyVectorOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_copyVectorOptions(JNIEnv *, jclass,
                                                       jlong jhandle) {
  auto new_vopt = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions(
      *(reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
          jhandle)));
  return GET_CPLUSPLUS_POINTER(new_vopt);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxWriteBatchGroupSizeBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxWriteBatchGroupSizeBytes(
    JNIEnv *, jclass, jlong jhandle, jlong jmax_write_batch_group_size_bytes) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->max_write_batch_group_size_bytes =
      static_cast<uint64_t>(jmax_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxWriteBatchGroupSizeBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxWriteBatchGroupSizeBytes(
    JNIEnv *, jclass, jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->max_write_batch_group_size_bytes);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static void rocksdb_set_event_listeners_helper(
    JNIEnv *env, jlongArray jlistener_array,
    std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>
        &listener_sptr_vec) {
  jlong *ptr_jlistener_array =
      env->GetLongArrayElements(jlistener_array, nullptr);
  if (ptr_jlistener_array == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  const jsize array_size = env->GetArrayLength(jlistener_array);
  listener_sptr_vec.clear();
  for (jsize i = 0; i < array_size; ++i) {
    const auto &listener_sptr =
        *reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener> *>(
            ptr_jlistener_array[i]);
    listener_sptr_vec.push_back(listener_sptr);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEventListeners
 * Signature: (J[J)V
 */
void Java_org_rocksdb_VectorOptions_setEventListeners(
    JNIEnv *env, jclass, jlong jhandle, jlongArray jlistener_array) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  rocksdb_set_event_listeners_helper(env, jlistener_array, vopt->listeners);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static jobjectArray rocksdb_get_event_listeners_helper(
    JNIEnv *env,
    const std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>
        &listener_sptr_vec) {
  jsize sz = static_cast<jsize>(listener_sptr_vec.size());
  jclass jlistener_clazz =
      ROCKSDB_NAMESPACE::AbstractEventListenerJni::getJClass(env);
  jobjectArray jlisteners = env->NewObjectArray(sz, jlistener_clazz, nullptr);
  if (jlisteners == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  for (jsize i = 0; i < sz; ++i) {
    const auto *jni_cb =
        static_cast<ROCKSDB_NAMESPACE::EventListenerJniCallback *>(
            listener_sptr_vec[i].get());
    env->SetObjectArrayElement(jlisteners, i, jni_cb->GetJavaObject());
  }
  return jlisteners;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    eventListeners
 * Signature: (J)[Lorg/rocksdb/AbstractEventListener;
 */
jobjectArray Java_org_rocksdb_VectorOptions_eventListeners(JNIEnv *env, jclass,
                                                           jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return rocksdb_get_event_listeners_helper(env, vopt->listeners);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSkipCheckingSstFileSizesOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setSkipCheckingSstFileSizesOnDbOpen(
    JNIEnv *, jclass, jlong jhandle,
    jboolean jskip_checking_sst_file_sizes_on_db_open) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->skip_checking_sst_file_sizes_on_db_open =
      static_cast<bool>(jskip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    skipCheckingSstFileSizesOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_skipCheckingSstFileSizesOnDbOpen(
    JNIEnv *, jclass, jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->skip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_optimizeForSmallDb__JJ(JNIEnv *, jclass,
                                                           jlong jhandle,
                                                           jlong cache_handle) {
  auto *cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *>(
          cache_handle);
  auto *vector_options_ptr = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *vcf_options_ptr = static_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      vector_options_ptr);
  vcf_options_ptr->OptimizeForSmallDb(cache_sptr_ptr);
}

static std::vector<ROCKSDB_NAMESPACE::DbPath>
rocksdb_convert_cf_paths_from_java_helper(JNIEnv *env, jobjectArray path_array,
                                          jlongArray size_array,
                                          jboolean *has_exception) {
  jboolean copy_str_has_exception;
  std::vector<std::string> paths = ROCKSDB_NAMESPACE::JniUtil::copyStrings(
      env, path_array, &copy_str_has_exception);
  if (JNI_TRUE == copy_str_has_exception) {
    // Exception thrown
    *has_exception = JNI_TRUE;
    return {};
  }

  if (static_cast<size_t>(env->GetArrayLength(size_array)) != paths.size()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env,
        ROCKSDB_NAMESPACE::Status::InvalidArgument(
            ROCKSDB_NAMESPACE::Slice("There should be a corresponding target "
                                     "size for every path and vice versa.")));
    *has_exception = JNI_TRUE;
    return {};
  }

  jlong *size_array_ptr = env->GetLongArrayElements(size_array, nullptr);
  if (nullptr == size_array_ptr) {
    // exception thrown: OutOfMemoryError
    *has_exception = JNI_TRUE;
    return {};
  }
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths;
  for (size_t i = 0; i < paths.size(); ++i) {
    jlong target_size = size_array_ptr[i];
    if (target_size < 0) {
      ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
          env,
          ROCKSDB_NAMESPACE::Status::InvalidArgument(ROCKSDB_NAMESPACE::Slice(
              "Path target size has to be positive.")));
      *has_exception = JNI_TRUE;
      env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);
      return {};
    }
    cf_paths.push_back(ROCKSDB_NAMESPACE::DbPath(
        paths[i], static_cast<uint64_t>(target_size)));
  }

  env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);

  return cf_paths;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorOptions_setCfPaths(JNIEnv *env, jclass,
                                               jlong jhandle,
                                               jobjectArray path_array,
                                               jlongArray size_array) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    vector_options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_cfPathsLen(JNIEnv *, jclass,
                                                jlong jhandle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vector_options->cf_paths.size());
}

template <typename T>
static void rocksdb_convert_cf_paths_to_java_helper(JNIEnv *env, jlong jhandle,
                                                    jobjectArray jpaths,
                                                    jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong *ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto *opt = reinterpret_cast<T *>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath cf_path = opt->cf_paths[i];

    jstring jpath = env->NewStringUTF(cf_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(cf_path.target_size);

    env->DeleteLocalRef(jpath);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorOptions_cfPaths(JNIEnv *env, jclass, jlong jhandle,
                                            jobjectArray jpaths,
                                            jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions>(
      env, jhandle, jpaths, jtarget_sizes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionThreadLimiter(
    JNIEnv *, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter> *>(
      jlimiter_handle);
  vector_options->compaction_thread_limiter = *limiter;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAvoidUnnecessaryBlockingIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAvoidUnnecessaryBlockingIO(
    JNIEnv *, jclass, jlong jhandle, jboolean avoid_blocking_io) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->avoid_unnecessary_blocking_io = static_cast<bool>(avoid_blocking_io);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    avoidUnnecessaryBlockingIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_avoidUnnecessaryBlockingIO(
    JNIEnv *, jclass, jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->avoid_unnecessary_blocking_io);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setPersistStatsToDisk
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setPersistStatsToDisk(
    JNIEnv *, jclass, jlong jhandle, jboolean persist_stats_to_disk) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->persist_stats_to_disk = static_cast<bool>(persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    persistStatsToDisk
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_persistStatsToDisk(JNIEnv *, jclass,
                                                           jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWriteDbidToManifest
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setWriteDbidToManifest(
    JNIEnv *, jclass, jlong jhandle, jboolean jwrite_dbid_to_manifest) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->write_dbid_to_manifest = static_cast<bool>(jwrite_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    writeDbidToManifest
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_writeDbidToManifest(JNIEnv *, jclass,
                                                            jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->write_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLogReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setLogReadaheadSize(
    JNIEnv *, jclass, jlong jhandle, jlong jlog_readahead_size) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->log_readahead_size = static_cast<size_t>(jlog_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    logReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_logReadaheadSize(JNIEnv *, jclass,
                                                      jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->log_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBestEffortsRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setBestEffortsRecovery(
    JNIEnv *, jclass, jlong jhandle, jboolean jbest_efforts_recovery) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->best_efforts_recovery = static_cast<bool>(jbest_efforts_recovery);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    bestEffortsRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_bestEffortsRecovery(JNIEnv *, jclass,
                                                            jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->best_efforts_recovery);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBgErrorResumeCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBgErrorResumeCount(
    JNIEnv *, jclass, jlong jhandle, jint jmax_bgerror_resume_count) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->max_bgerror_resume_count = static_cast<int>(jmax_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBgerrorResumeCount
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxBgerrorResumeCount(JNIEnv *, jclass,
                                                          jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jint>(vopt->max_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBgerrorResumeRetryInterval
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setBgerrorResumeRetryInterval(
    JNIEnv *, jclass, jlong jhandle, jlong jbgerror_resume_retry_interval) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->bgerror_resume_retry_interval =
      static_cast<uint64_t>(jbgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    bgerrorResumeRetryInterval
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_bgerrorResumeRetryInterval(JNIEnv *,
                                                                jclass,
                                                                jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->bgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_VectorOptions_disposeInternal(JNIEnv *, jobject,
                                                    jlong handle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(handle);
  assert(vopt != nullptr);
  delete vopt;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setEnv(JNIEnv *, jobject, jlong jhandle,
                                           jlong jenv) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->env = reinterpret_cast<ROCKSDB_NAMESPACE::Env *>(jenv);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setIncreaseParallelism(JNIEnv *, jobject,
                                                           jlong jhandle,
                                                           jint totalThreads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->IncreaseParallelism(static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setCreateIfMissing(JNIEnv *, jobject,
                                                       jlong jhandle,
                                                       jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->create_if_missing = flag;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_createIfMissing(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->create_if_missing;
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setCreateMissingColumnFamilies(
    JNIEnv *, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_createMissingColumnFamilies(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setErrorIfExists(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jboolean error_if_exists) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->error_if_exists = static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_errorIfExists(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->error_if_exists;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setParanoidChecks(
    JNIEnv *, jobject, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->paranoid_checks = static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_paranoidChecks(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->paranoid_checks;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setRateLimiter(JNIEnv *, jobject,
                                                   jlong jhandle,
                                                   jlong jrate_limiter_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter> *pRateLimiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter> *>(
          jrate_limiter_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->rate_limiter = *pRateLimiter;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setSstFileManager(
    JNIEnv *, jobject, jlong jhandle, jlong jsst_file_manager_handle) {
  auto *sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager> *>(
          jsst_file_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->sst_file_manager = *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLogger
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setLogger(JNIEnv *, jobject, jlong jhandle,
                                              jlong jlogger_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback> *pLogger =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback> *>(
          jlogger_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->info_log = *pLogger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setInfoLogLevel(JNIEnv *, jobject,
                                                    jlong jhandle,
                                                    jbyte jlog_level) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->info_log_level =
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_infoLogLevel(JNIEnv *, jobject,
                                                  jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->info_log_level);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxOpenFiles(JNIEnv *, jobject,
                                                    jlong jhandle,
                                                    jint max_open_files) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_open_files = static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxOpenFiles(JNIEnv *, jobject,
                                                 jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_open_files;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxTotalWalSize(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_total_wal_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_total_wal_size = static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxFileOpeningThreads(
    JNIEnv *, jobject, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_file_opening_threads = static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxFileOpeningThreads(JNIEnv *, jobject,
                                                          jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxTotalWalSize(JNIEnv *, jobject,
                                                     jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setStatistics(JNIEnv *, jobject,
                                                  jlong jhandle,
                                                  jlong jstatistics_handle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *pSptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni> *>(
          jstatistics_handle);
  vopt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_statistics(JNIEnv *, jobject,
                                                jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr = vopt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> *pSptr =
        new std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>(sptr);
    return GET_CPLUSPLUS_POINTER(pSptr);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_useFsync(JNIEnv *, jobject,
                                                 jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->use_fsync;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setUseFsync(JNIEnv *, jobject,
                                                jlong jhandle,
                                                jboolean use_fsync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->use_fsync = static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorOptions_setDbPaths(JNIEnv *env, jobject,
                                               jlong jhandle,
                                               jobjectArray jpaths,
                                               jlongArray jtarget_sizes) {
  std::vector<ROCKSDB_NAMESPACE::DbPath> db_paths;
  jlong *ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        ROCKSDB_NAMESPACE::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_dbPathsLen(JNIEnv *, jobject,
                                                jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorOptions_dbPaths(JNIEnv *env, jobject, jlong jhandle,
                                            jobjectArray jpaths,
                                            jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong *ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath db_path = vopt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy == JNI_TRUE ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_VectorOptions_setDbLogDir(JNIEnv *env, jobject,
                                                jlong jhandle,
                                                jstring jdb_log_dir) {
  const char *log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->db_log_dir.assign(log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorOptions_dbLogDir(JNIEnv *env, jobject,
                                                jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalDir
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_VectorOptions_setWalDir(JNIEnv *env, jobject,
                                              jlong jhandle, jstring jwal_dir) {
  const char *wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->wal_dir.assign(wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    walDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorOptions_walDir(JNIEnv *env, jobject,
                                              jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv *, jobject, jlong jhandle, jlong micros) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_deleteObsoleteFilesPeriodMicros(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBackgroundCompactions(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle,
                                                                jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxBackgroundCompactions(JNIEnv *, jobject,
                                                             jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxSubcompactions(JNIEnv *, jobject,
                                                         jlong jhandle,
                                                         jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_subcompactions = static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxSubcompactions(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_subcompactions;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBackgroundFlushes(
    JNIEnv *, jobject, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_background_flushes = static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxBackgroundFlushes(JNIEnv *, jobject,
                                                         jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_background_flushes;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBackgroundJobs(
    JNIEnv *, jobject, jlong jhandle, jint max_background_jobs) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_background_jobs = static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxBackgroundJobs(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_background_jobs;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxLogFileSize(JNIEnv *env, jobject,
                                                      jlong jhandle,
                                                      jlong max_log_file_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->max_log_file_size = max_log_file_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxLogFileSize(JNIEnv *, jobject,
                                                    jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_log_file_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setLogFileTimeToRoll(
    JNIEnv *env, jobject, jlong jhandle, jlong log_file_time_to_roll) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->log_file_time_to_roll = log_file_time_to_roll;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_logFileTimeToRoll(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setKeepLogFileNum(JNIEnv *env, jobject,
                                                      jlong jhandle,
                                                      jlong keep_log_file_num) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->keep_log_file_num = keep_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_keepLogFileNum(JNIEnv *, jobject,
                                                    jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setRecycleLogFileNum(
    JNIEnv *env, jobject, jlong jhandle, jlong recycle_log_file_num) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->recycle_log_file_num = recycle_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_recycleLogFileNum(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxManifestFileSize(
    JNIEnv *, jobject, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_manifest_file_size = static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxManifestFileSize(JNIEnv *, jobject,
                                                         jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_manifest_file_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxTableFilesSizeFIFO(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxTableFilesSizeFIFO(JNIEnv *, jobject,
                                                           jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setTableCacheNumshardbits(
    JNIEnv *, jobject, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->table_cache_numshardbits = static_cast<int>(table_cache_numshardbits);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_tableCacheNumshardbits(JNIEnv *, jobject,
                                                           jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWalTtlSeconds(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jlong WAL_ttl_seconds) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->WAL_ttl_seconds = static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_walTtlSeconds(JNIEnv *, jobject,
                                                   jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWalSizeLimitMB(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jlong WAL_size_limit_MB) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->WAL_size_limit_MB = static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    walSizeLimitMB
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_walSizeLimitMB(JNIEnv *, jobject,
                                                    jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setManifestPreallocationSize(
    JNIEnv *env, jobject, jlong jhandle, jlong preallocation_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      preallocation_size);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_manifestPreallocationSize(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setUseDirectReads(
    JNIEnv *, jobject, jlong jhandle, jboolean use_direct_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->use_direct_reads = static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_useDirectReads(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->use_direct_reads;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setUseDirectIoForFlushAndCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setUseDirectIoForFlushAndCompaction(
    JNIEnv *, jobject, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_useDirectIoForFlushAndCompaction(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowFAllocate(
    JNIEnv *, jobject, jlong jhandle, jboolean jallow_fallocate) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->allow_fallocate = static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowFAllocate(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowMmapReads(
    JNIEnv *, jobject, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->allow_mmap_reads = static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowMmapReads(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowMmapWrites(
    JNIEnv *, jobject, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->allow_mmap_writes = static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowMmapWrites(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setIsFdCloseOnExec(
    JNIEnv *, jobject, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->is_fd_close_on_exec = static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_isFdCloseOnExec(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setStatsDumpPeriodSec(
    JNIEnv *, jobject, jlong jhandle, jint jstats_dump_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->stats_dump_period_sec =
      static_cast<unsigned int>(jstats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_statsDumpPeriodSec(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setStatsPersistPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setStatsPersistPeriodSec(
    JNIEnv *, jobject, jlong jhandle, jint jstats_persist_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->stats_persist_period_sec =
      static_cast<unsigned int>(jstats_persist_period_sec);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    statsPersistPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_statsPersistPeriodSec(JNIEnv *, jobject,
                                                          jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->stats_persist_period_sec;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setStatsHistoryBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setStatsHistoryBufferSize(
    JNIEnv *, jobject, jlong jhandle, jlong jstats_history_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->stats_history_buffer_size =
      static_cast<size_t>(jstats_history_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    statsHistoryBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_statsHistoryBufferSize(JNIEnv *, jobject,
                                                            jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->stats_history_buffer_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAdviseRandomOnOpen(
    JNIEnv *, jobject, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->advise_random_on_open = static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_adviseRandomOnOpen(JNIEnv *, jobject,
                                                           jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setDbWriteBufferSize(
    JNIEnv *, jobject, jlong jhandle, jlong jdb_write_buffer_size) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWriteBufferManager(
    JNIEnv *, jobject, jlong joptions_handle,
    jlong jwrite_buffer_manager_handle) {
  auto *write_buffer_manager = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> *>(
      jwrite_buffer_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      joptions_handle)
      ->write_buffer_manager = *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_dbWriteBufferSize(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAccessHintOnCompactionStart
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setAccessHintOnCompactionStart(
    JNIEnv *, jobject, jlong jhandle, jbyte jaccess_hint_value) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->access_hint_on_compaction_start =
      ROCKSDB_NAMESPACE::AccessHintJni::toCppAccessHint(jaccess_hint_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    accessHintOnCompactionStart
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_accessHintOnCompactionStart(
    JNIEnv *, jobject, jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::AccessHintJni::toJavaAccessHint(
      opt->access_hint_on_compaction_start);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionReadaheadSize(
    JNIEnv *, jobject, jlong jhandle, jlong jcompaction_readahead_size) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_compactionReadaheadSize(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setRandomAccessMaxBufferSize(
    JNIEnv *, jobject, jlong jhandle, jlong jrandom_access_max_buffer_size) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->random_access_max_buffer_size =
      static_cast<size_t>(jrandom_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_randomAccessMaxBufferSize(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWritableFileMaxBufferSize(
    JNIEnv *, jobject, jlong jhandle, jlong jwritable_file_max_buffer_size) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_writableFileMaxBufferSize(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setUseAdaptiveMutex(
    JNIEnv *, jobject, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->use_adaptive_mutex = static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_useAdaptiveMutex(JNIEnv *, jobject,
                                                         jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setBytesPerSync(JNIEnv *, jobject,
                                                    jlong jhandle,
                                                    jlong bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->bytes_per_sync = static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_bytesPerSync(JNIEnv *, jobject,
                                                  jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWalBytesPerSync(
    JNIEnv *, jobject, jlong jhandle, jlong jwal_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->wal_bytes_per_sync = static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_walBytesPerSync(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setStrictBytesPerSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setStrictBytesPerSync(
    JNIEnv *, jobject, jlong jhandle, jboolean jstrict_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->strict_bytes_per_sync = jstrict_bytes_per_sync == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    strictBytesPerSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_strictBytesPerSync(JNIEnv *, jobject,
                                                           jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(opt->strict_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setEnableThreadTracking(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_thread_tracking) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->enable_thread_tracking = static_cast<bool>(jenable_thread_tracking);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_enableThreadTracking(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setDelayedWriteRate(
    JNIEnv *, jobject, jlong jhandle, jlong jdelayed_write_rate) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_delayedWriteRate(JNIEnv *, jobject,
                                                      jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setEnablePipelinedWrite(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_pipelined_write) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_enablePipelinedWrite(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setUnorderedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setUnorderedWrite(
    JNIEnv *, jobject, jlong jhandle, jboolean unordered_write) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->unordered_write = static_cast<bool>(unordered_write);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    unorderedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_unorderedWrite(JNIEnv *, jobject,
                                                       jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->unordered_write;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowConcurrentMemtableWrite(
    JNIEnv *, jobject, jlong jhandle, jboolean allow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowConcurrentMemtableWrite(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setEnableWriteThreadAdaptiveYield(
    JNIEnv *, jobject, jlong jhandle, jboolean yield) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_enableWriteThreadAdaptiveYield(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWriteThreadMaxYieldUsec(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle,
                                                               jlong max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->write_thread_max_yield_usec = static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_writeThreadMaxYieldUsec(JNIEnv *, jobject,
                                                             jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWriteThreadSlowYieldUsec(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle,
                                                                jlong slow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->write_thread_slow_yield_usec = static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_writeThreadSlowYieldUsec(JNIEnv *, jobject,
                                                              jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setSkipStatsUpdateOnDbOpen(
    JNIEnv *, jobject, jlong jhandle, jboolean jskip_stats_update_on_db_open) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_skipStatsUpdateOnDbOpen(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setWalRecoveryMode(
    JNIEnv *, jobject, jlong jhandle, jbyte jwal_recovery_mode_value) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->wal_recovery_mode =
      ROCKSDB_NAMESPACE::WALRecoveryModeJni::toCppWALRecoveryMode(
          jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_walRecoveryMode(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::WALRecoveryModeJni::toJavaWALRecoveryMode(
      vopt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllow2pc(JNIEnv *, jobject,
                                                jlong jhandle,
                                                jboolean jallow_2pc) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allow2pc(JNIEnv *, jobject,
                                                 jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->allow_2pc);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setRowCache(JNIEnv *, jobject,
                                                jlong jhandle,
                                                jlong jrow_cache_handle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *row_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *>(
          jrow_cache_handle);
  vopt->row_cache = *row_cache;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWalFilter(JNIEnv *, jobject,
                                                 jlong jhandle,
                                                 jlong jwal_filter_handle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *wal_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::WalFilterJniCallback *>(
          jwal_filter_handle);
  vopt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setFailIfOptionsFileError
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setFailIfOptionsFileError(
    JNIEnv *, jobject, jlong jhandle, jboolean jfail_if_options_file_error) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->fail_if_options_file_error =
      static_cast<bool>(jfail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    failIfOptionsFileError
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_failIfOptionsFileError(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->fail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setDumpMallocStats(
    JNIEnv *, jobject, jlong jhandle, jboolean jdump_malloc_stats) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_dumpMallocStats(JNIEnv *, jobject,
                                                        jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAvoidFlushDuringRecovery(
    JNIEnv *, jobject, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_avoidFlushDuringRecovery(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAvoidFlushDuringShutdown(
    JNIEnv *, jobject, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_avoidFlushDuringShutdown(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowIngestBehind(
    JNIEnv *, jobject, jlong jhandle, jboolean jallow_ingest_behind) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowIngestBehind(JNIEnv *, jobject,
                                                          jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setTwoWriteQueues(
    JNIEnv *, jobject, jlong jhandle, jboolean jtwo_write_queues) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_twoWriteQueues(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->two_write_queues);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setManualWalFlush(
    JNIEnv *, jobject, jlong jhandle, jboolean jmanual_wal_flush) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_manualWalFlush(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_VectorOptions_optimizeForSmallDb__J(JNIEnv *, jobject,
                                                          jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options *>(jhandle)->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_optimizeForPointLookup(
    JNIEnv *, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_optimizeLevelStyleCompaction(
    JNIEnv *, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_optimizeUniversalStyleCompaction(
    JNIEnv *, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setComparatorHandle__JI(
    JNIEnv *, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_VectorOptions_setComparatorHandle__JJB(
    JNIEnv *, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator *comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback *>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator *>(jcomparator_handle);
      break;
  }
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jopt_handle);
  vopt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMergeOperatorName
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_VectorOptions_setMergeOperatorName(JNIEnv *env, jobject,
                                                         jlong jhandle,
                                                         jstring jop_name) {
  const char *op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);

  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMergeOperator
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMergeOperator(
    JNIEnv *, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator> *>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionFilterHandle(
    JNIEnv *, jobject, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter *>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void JNICALL Java_org_rocksdb_VectorOptions_setCompactionFilterFactoryHandle(
    JNIEnv *, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto *cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactory> *>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setWriteBufferSize(
    JNIEnv *env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->write_buffer_size = jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_writeBufferSize(JNIEnv *, jobject,
                                                     jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxWriteBufferNumber(
    JNIEnv *, jobject, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxWriteBufferNumber(JNIEnv *, jobject,
                                                         jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMinWriteBufferNumberToMerge(
    JNIEnv *, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_minWriteBufferNumberToMerge(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_compressionType(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vopts->compression);
}

/**
 * Helper method to convert a Java byte array of compression levels
 * to a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A std::unique_ptr to the vector, or std::unique_ptr(nullptr) if a JNI
 * exception occurs
 */
std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
rocksdb_vector_compression_vector_helper(JNIEnv *env,
                                         jbyteArray jcompression_levels) {
  jsize len = env->GetArrayLength(jcompression_levels);
  jbyte *jcompression_level =
      env->GetByteArrayElements(jcompression_levels, nullptr);
  if (jcompression_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>();
  }

  auto *compression_levels =
      new std::vector<ROCKSDB_NAMESPACE::CompressionType>();
  std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
      uptr_compression_levels(compression_levels);

  for (jsize i = 0; i < len; i++) {
    jbyte jcl = jcompression_level[i];
    compression_levels->push_back(
        static_cast<ROCKSDB_NAMESPACE::CompressionType>(jcl));
  }

  env->ReleaseByteArrayElements(jcompression_levels, jcompression_level,
                                JNI_ABORT);

  return uptr_compression_levels;
}

/**
 * Helper method to convert a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 * to a Java byte array of compression levels
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A jbytearray or nullptr if an exception occurs
 */
jbyteArray rocksdb_vector_compression_list_helper(
    JNIEnv *env,
    std::vector<ROCKSDB_NAMESPACE::CompressionType> compression_levels) {
  const size_t len = compression_levels.size();
  jbyte *jbuf = new jbyte[len];

  for (size_t i = 0; i < len; i++) {
    jbuf[i] = compression_levels[i];
  }

  // insert in java array
  jbyteArray jcompression_levels = env->NewByteArray(static_cast<jsize>(len));
  if (jcompression_levels == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] jbuf;
    return nullptr;
  }
  env->SetByteArrayRegion(jcompression_levels, 0, static_cast<jsize>(len),
                          jbuf);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jcompression_levels);
    delete[] jbuf;
    return nullptr;
  }

  delete[] jbuf;

  return jcompression_levels;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_VectorOptions_setCompressionPerLevel(
    JNIEnv *env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto uptr_compression_levels =
      rocksdb_vector_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vector_options->compression_per_level = *(uptr_compression_levels.get());
};

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_VectorOptions_compressionPerLevel(JNIEnv *env,
                                                              jobject,
                                                              jlong jhandle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return rocksdb_vector_compression_list_helper(
      env, vector_options->compression_per_level);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setBottommostCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vector_options->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_bottommostCompressionType(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vector_options->bottommost_compression);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setBottommostCompressionOptions(
    JNIEnv *, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions *>(
          jbottommost_compression_options_handle);
  vector_options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompressionOptions(
    JNIEnv *, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions *>(
          jcompression_options_handle);
  vector_options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_useFixedLengthPrefixExtractor(
    JNIEnv *, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_useCappedPrefixExtractor(
    JNIEnv *, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setNumLevels(JNIEnv *, jobject,
                                                 jlong jhandle,
                                                 jint jnum_levels) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->num_levels = static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_numLevels(JNIEnv *, jobject,
                                              jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->num_levels;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevelZeroFileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_levelZeroFileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevelZeroSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevelZeroSlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_levelZeroSlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevelZeroStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevelZeroStopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_levelZeroStopWritesTrigger(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setTargetFileSizeBase(
    JNIEnv *, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_targetFileSizeBase(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setTargetFileSizeMultiplier(
    JNIEnv *, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_targetFileSizeMultiplier(JNIEnv *, jobject,
                                                             jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBytesForLevelBase(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxBytesForLevelBase(JNIEnv *, jobject,
                                                          jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setLevelCompactionDynamicLevelBytes(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_levelCompactionDynamicLevelBytes(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBytesForLevelMultiplier(
    JNIEnv *, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorOptions_maxBytesForLevelMultiplier(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxCompactionBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxCompactionBytes(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_compaction_bytes = static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxCompactionBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxCompactionBytes(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setArenaBlockSize(JNIEnv *env, jobject,
                                                      jlong jhandle,
                                                      jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->arena_block_size = jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_arenaBlockSize(JNIEnv *, jobject,
                                                    jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setDisableAutoCompactions(
    JNIEnv *, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_disableAutoCompactions(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->disable_auto_compactions;
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionStyle(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vector_options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_compactionStyle(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      vector_options->compaction_style);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxSequentialSkipInIterations(
    JNIEnv *, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxSequentialSkipInIterations(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMemTableFactory(
    JNIEnv *, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorOptions_memTableFactoryName(JNIEnv *env, jobject,
                                                           jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory *tf = vopt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setTableFactory(
    JNIEnv *, jobject, jlong jhandle, jlong jtable_factory_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *table_factory = reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory *>(
      jtable_factory_handle);
  options->table_factory.reset(table_factory);
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorOptions_tableFactoryName(JNIEnv *env, jobject,
                                                        jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  ROCKSDB_NAMESPACE::TableFactory *tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setInplaceUpdateSupport(
    JNIEnv *, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_inplaceUpdateSupport(JNIEnv *, jobject,
                                                             jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setInplaceUpdateNumLocks(
    JNIEnv *env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_inplaceUpdateNumLocks(JNIEnv *, jobject,
                                                           jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorOptions_setMemtablePrefixBloomSizeRatio(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorOptions_memtablePrefixBloomSizeRatio(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setExperimentalMempurgeThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorOptions_setExperimentalMempurgeThreshold(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jexperimental_mempurge_threshold) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->experimental_mempurge_threshold =
      static_cast<double>(jexperimental_mempurge_threshold);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    experimentalMempurgeThreshold
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorOptions_experimentalMempurgeThreshold(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->experimental_mempurge_threshold;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMemtableWholeKeyFiltering
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setMemtableWholeKeyFiltering(
    JNIEnv *, jobject, jlong jhandle, jboolean jmemtable_whole_key_filtering) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->memtable_whole_key_filtering =
      static_cast<bool>(jmemtable_whole_key_filtering);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    memtableWholeKeyFiltering
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_memtableWholeKeyFiltering(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->memtable_whole_key_filtering;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setBloomLocality(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jint jbloom_locality) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->bloom_locality = static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_bloomLocality(JNIEnv *, jobject,
                                                  jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->bloom_locality;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxSuccessiveMerges(
    JNIEnv *env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxSuccessiveMerges(JNIEnv *, jobject,
                                                         jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setOptimizeFiltersForHits(
    JNIEnv *, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_optimizeFiltersForHits(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMemtableHugePageSize(
    JNIEnv *env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<
        ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_memtableHugePageSize(JNIEnv *, jobject,
                                                          jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setSoftPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_softPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setHardPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    hardPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_hardPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevel0FileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_level0FileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevel0SlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_level0SlowdownWritesTrigger(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setLevel0StopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_level0StopWritesTrigger(JNIEnv *, jobject,
                                                            jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_VectorOptions_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv *env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint *additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_VectorOptions_maxBytesForLevelMultiplierAdditional(
    JNIEnv *env, jobject, jlong jhandle) {
  auto mbflma =
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle)
          ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint *additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }

  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setParanoidFileChecks(
    JNIEnv *, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->paranoid_file_checks = static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_paranoidFileChecks(JNIEnv *, jobject,
                                                           jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setMaxWriteBufferNumberToMaintain(
    JNIEnv *, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
      jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_maxWriteBufferNumberToMaintain(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(
             jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionPriority(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_compactionPriority(JNIEnv *, jobject,
                                                        jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      vopts->compaction_pri);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setReportBgIoStats(
    JNIEnv *, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_reportBgIoStats(JNIEnv *, jobject,
                                                        jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<bool>(vopts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setTtl(JNIEnv *, jobject, jlong jhandle,
                                           jlong jttl) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    ttl
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_ttl(JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopts->ttl);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setPeriodicCompactionSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setPeriodicCompactionSeconds(
    JNIEnv *, jobject, jlong jhandle, jlong jperiodicCompactionSeconds) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->periodic_compaction_seconds =
      static_cast<uint64_t>(jperiodicCompactionSeconds);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    periodicCompactionSeconds
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_VectorOptions_periodicCompactionSeconds(JNIEnv *, jobject,
                                                         jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopts->periodic_compaction_seconds);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionOptionsUniversal(
    JNIEnv *, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal *>(
          jcompaction_options_universal_handle);
  vopts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setCompactionOptionsFIFO(
    JNIEnv *, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto *opts_fifo =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO *>(
          jcompaction_options_fifo_handle);
  vopts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setForceConsistencyChecks(
    JNIEnv *, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->force_consistency_checks =
      static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_forceConsistencyChecks(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<bool>(vopts->force_consistency_checks);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAtomicFlush(JNIEnv *, jobject,
                                                   jlong jhandle,
                                                   jboolean jatomic_flush) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_atomicFlush(JNIEnv *, jobject,
                                                    jlong jhandle) {
  auto *vopt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopt->atomic_flush);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setSstPartitionerFactory(
    JNIEnv *, jobject, jlong jhandle, jlong factory_handle) {
  auto *vector_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  auto factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory> *>(
      factory_handle);
  vector_options->sst_partitioner_factory = *factory;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnableBlobFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setEnableBlobFiles(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_blob_files) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->enable_blob_files = static_cast<bool>(jenable_blob_files);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    enableBlobFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_enableBlobFiles(JNIEnv *, jobject,
                                                        jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopts->enable_blob_files);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMinBlobSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMinBlobSize(JNIEnv *, jobject,
                                                   jlong jhandle,
                                                   jlong jmin_blob_size) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->min_blob_size = static_cast<uint64_t>(jmin_blob_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    minBlobSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_minBlobSize(JNIEnv *, jobject,
                                                 jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopts->min_blob_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setBlobFileSize(JNIEnv *, jobject,
                                                    jlong jhandle,
                                                    jlong jblob_file_size) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_file_size = static_cast<uint64_t>(jblob_file_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_blobFileSize(JNIEnv *, jobject,
                                                  jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopts->blob_file_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setBlobCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jblob_compression_type_value) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_compression_type =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jblob_compression_type_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_blobCompressionType(JNIEnv *, jobject,
                                                         jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vopts->blob_compression_type);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEnableBlobGarbageCollection
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setEnableBlobGarbageCollection(
    JNIEnv *, jobject, jlong jhandle,
    jboolean jenable_blob_garbage_collection) {
  auto *opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  opts->enable_blob_garbage_collection =
      static_cast<bool>(jenable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    enableBlobGarbageCollection
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_enableBlobGarbageCollection(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jboolean>(vopts->enable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobGarbageCollectionAgeCutoff
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorOptions_setBlobGarbageCollectionAgeCutoff(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jblob_garbage_collection_age_cutoff) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_garbage_collection_age_cutoff =
      static_cast<double>(jblob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobGarbageCollectionAgeCutoff
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorOptions_blobGarbageCollectionAgeCutoff(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jdouble>(vopts->blob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobGarbageCollectionForceThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorOptions_setBlobGarbageCollectionForceThreshold(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jblob_garbage_collection_force_threshold) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_garbage_collection_force_threshold =
      static_cast<double>(jblob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobGarbageCollectionForceThreshold
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorOptions_blobGarbageCollectionForceThreshold(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jdouble>(vopts->blob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setBlobCompactionReadaheadSize(
    JNIEnv *, jobject, jlong jhandle, jlong jblob_compaction_readahead_size) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_compaction_readahead_size =
      static_cast<uint64_t>(jblob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobCompactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_blobCompactionReadaheadSize(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jlong>(vopts->blob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setBlobFileStartingLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setBlobFileStartingLevel(
    JNIEnv *, jobject, jlong jhandle, jint jblob_file_starting_level) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->blob_file_starting_level = jblob_file_starting_level;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    blobFileStartingLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_blobFileStartingLevel(JNIEnv *, jobject,
                                                          jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jint>(vopts->blob_file_starting_level);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setPrepopulateBlobCache
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setPrepopulateBlobCache(
    JNIEnv *, jobject, jlong jhandle, jbyte jprepopulate_blob_cache_value) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->prepopulate_blob_cache =
      ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toCppPrepopulateBlobCache(
          jprepopulate_blob_cache_value);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    prepopulateBlobCache
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_prepopulateBlobCache(JNIEnv *, jobject,
                                                          jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toJavaPrepopulateBlobCache(
      vopts->prepopulate_blob_cache);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setMaxElements
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setMaxElements(JNIEnv *, jobject,
                                                   jlong jhandle,
                                                   jlong jmax_elements) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->max_elements = jmax_elements;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    maxElements
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_maxElements(JNIEnv *, jobject,
                                                 jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->max_elements;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setM
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setM(JNIEnv *, jobject, jlong jhandle,
                                         jlong jm) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->M = jm;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    M
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_M(JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->M;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setEfConstruction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setEfConstruction(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jlong jef_construction) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->ef_construction = jef_construction;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    efConstruction
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_efConstruction(JNIEnv *, jobject,
                                                    jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->ef_construction;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setRandomSeed
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setRandomSeed(JNIEnv *, jobject,
                                                  jlong jhandle,
                                                  jlong jrandom_seed) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->random_seed = jrandom_seed;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    randomSeed
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_randomSeed(JNIEnv *, jobject,
                                                jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->random_seed;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setVisitListPoolSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setVisitListPoolSize(JNIEnv *, jobject,
                                                         jlong jhandle,
                                                         jlong jpool_size) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->visit_list_pool_size = jpool_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    visitListPoolSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_visitListPoolSize(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->visit_list_pool_size;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTerminationThreshold
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorOptions_setTerminationThreshold(JNIEnv *, jobject,
                                                            jlong jhandle,
                                                            jfloat jthreshold) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->termination_threshold = jthreshold;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    terminationThreshold
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorOptions_terminationThreshold(JNIEnv *, jobject,
                                                           jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->termination_threshold;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTerminationWeight
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorOptions_setTerminationWeight(JNIEnv *, jobject,
                                                         jlong jhandle,
                                                         jfloat jweight) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->termination_weight = jweight;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    terminationWeight
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorOptions_terminationWeight(JNIEnv *, jobject,
                                                        jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->termination_weight;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setTerminationLowerBound
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorOptions_setTerminationLowerBound(
    JNIEnv *, jobject, jlong jhandle, jfloat jtermination_lower_bound) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->termination_lower_bound = jtermination_lower_bound;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    terminationLowerBound
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorOptions_terminationLowerBound(JNIEnv *, jobject,
                                                            jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->termination_lower_bound;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setDim
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorOptions_setDim(JNIEnv *, jobject, jlong jhandle,
                                           jlong jdim) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->dim = jdim;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    dim
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorOptions_dim(JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->dim;
}
/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setSpace
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorOptions_setSpace(JNIEnv *, jobject, jlong jhandle,
                                             jbyte jspace_type) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  switch (jspace_type) {
    // L2
    case 0x0:
      vopts->space = hnswlib::L2;
      break;

    // IP
    case 0x1:
      vopts->space = hnswlib::IP;
      break;

    default:
      throw std::invalid_argument("Invalid space type");
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    space
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorOptions_space(JNIEnv *, jobject, jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  switch (vopts->space) {
    // L2
    case hnswlib::L2:
      return 0x0;

    // IP
    case hnswlib::IP:
      return 0x1;

    default:
      throw std::invalid_argument("Invalid space type");
  }
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setAllowReplaceDeleted
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorOptions_setAllowReplaceDeleted(
    JNIEnv *, jobject, jlong jhandle, jboolean jallow_replace_deleted) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->allow_replace_deleted = jallow_replace_deleted;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    allowReplaceDeleted
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorOptions_allowReplaceDeleted(JNIEnv *, jobject,
                                                            jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return vopts->allow_replace_deleted;
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    setFlushThreshold
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorOptions_setFlushThreshold(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jint jflush_threshold) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  vopts->flush_threshold = static_cast<int>(jflush_threshold);
}

/*
 * Class:     org_rocksdb_VectorOptions
 * Method:    flushThreshold
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorOptions_flushThreshold(JNIEnv *, jobject,
                                                   jlong jhandle) {
  auto *vopts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorOptions *>(jhandle);
  return static_cast<jint>(vopts->flush_threshold);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    getVectorColumnFamilyOptionsFromProps
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_getVectorColumnFamilyOptionsFromProps__JLjava_lang_String_2(
    JNIEnv *env, jclass, jlong cfg_handle, jstring jopt_string) {
  const char *opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  auto *config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(cfg_handle);
  auto *vcf_options = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
      VectorColumnFamilyOptions();
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::
      VECTORBACKEND_NAMESPACE::GetVectorColumnFamilyOptionsFromString(
          *config_options,
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
              VectorColumnFamilyOptions(),
          opt_string, vcf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(vcf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete vcf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    getVectorColumnFamilyOptionsFromProps
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_getVectorColumnFamilyOptionsFromProps__Ljava_lang_String_2(
    JNIEnv *env, jclass, jstring jopt_string) {
  const char *opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto *vcf_options = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
      VectorColumnFamilyOptions();
  ROCKSDB_NAMESPACE::ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::
      VECTORBACKEND_NAMESPACE::GetVectorColumnFamilyOptionsFromString(
          config_options,
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
              VectorColumnFamilyOptions(),
          opt_string, vcf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = GET_CPLUSPLUS_POINTER(vcf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete vcf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    newVectorColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_newVectorColumnFamilyOptions(
    JNIEnv *, jclass) {
  auto *op = new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
      VectorColumnFamilyOptions();
  op->disable_auto_compactions = true;
  op->check_flush_compaction_key_order = false;
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    copyColumnFamilyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_copyColumnFamilyOptions(
    JNIEnv *, jclass, jlong jhandle) {
  auto new_opt =
      new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions(
          *(reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                                 VectorColumnFamilyOptions *>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    copyVectorColumnFamilyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_copyVectorColumnFamilyOptions(
    JNIEnv *, jclass, jlong jhandle) {
  auto new_opt =
      new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions(
          *(reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                                 VectorColumnFamilyOptions *>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    newVectorColumnFamilyOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_newVectorColumnFamilyOptionsFromOptions(
    JNIEnv *, jclass, jlong jhandle) {
  auto new_opt =
      new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions(
          *(reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                                 VectorColumnFamilyOptions *>(jhandle)));
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_disposeInternal(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  assert(vcfo != nullptr);
  delete vcfo;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxElements
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxElements(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_elements) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->max_elements = jmax_elements;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxElements
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxElements(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->max_elements;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setM
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setM(JNIEnv *, jobject,
                                                     jlong jhandle, jlong jm) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->M = jm;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    M
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_M(JNIEnv *, jobject,
                                                   jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->M;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setEfConstruction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setEfConstruction(
    JNIEnv *, jobject, jlong jhandle, jlong jef_construction) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->ef_construction = jef_construction;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    efConstruction
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_efConstruction(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->ef_construction;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setRandomSeed
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setRandomSeed(
    JNIEnv *, jobject, jlong jhandle, jlong jrandom_seed) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->random_seed = jrandom_seed;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    randomSeed
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_randomSeed(JNIEnv *, jobject,
                                                            jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->random_seed;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setVisitListPoolSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setVisitListPoolSize(
    JNIEnv *, jobject, jlong jhandle, jlong jvisit_list_pool_size) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->visit_list_pool_size = jvisit_list_pool_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    visitListPoolSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_visitListPoolSize(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->visit_list_pool_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTerminationThreshold
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTerminationThreshold(
    JNIEnv *, jobject, jlong jhandle, jfloat jtermination_threshold) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->termination_threshold = jtermination_threshold;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    terminationThreshold
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorColumnFamilyOptions_terminationThreshold(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->termination_threshold;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTerminationWeight
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTerminationWeight(
    JNIEnv *, jobject, jlong jhandle, jfloat jtermination_weight) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->termination_weight = jtermination_weight;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    terminationWeight
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorColumnFamilyOptions_terminationWeight(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->termination_weight;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTerminationLowerBound
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTerminationLowerBound(
    JNIEnv *, jobject, jlong jhandle, jfloat jtermination_lower_bound) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->termination_lower_bound = jtermination_lower_bound;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    terminationWeight
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorColumnFamilyOptions_terminationLowerBound(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->termination_lower_bound;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setDim
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setDim(JNIEnv *, jobject,
                                                       jlong jhandle,
                                                       jlong jdim) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->dim = jdim;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    dim
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_dim(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->dim;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setSpace
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setSpace(JNIEnv *, jobject,
                                                         jlong jhandle,
                                                         jbyte jspace_type) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  switch (jspace_type) {
      // L2
    case 0x0:
      vcfo->space = hnswlib::L2;
      break;

    // IP
    case 0x1:
      vcfo->space = hnswlib::IP;
      break;

    default:
      throw std::invalid_argument("Invalid space type");
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    space
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_space(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  switch (vcfo->space) {
      // L2
    case hnswlib::L2:
      return 0x0;
      // IP
    case hnswlib::IP:
      return 0x1;
    default:
      throw std::invalid_argument("Invalid space type");
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setAllowReplaceDeleted
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setAllowReplaceDeleted(
    JNIEnv *, jobject, jlong jhandle, jboolean jallow_replace_deleted) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcfo->allow_replace_deleted = jallow_replace_deleted;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    allowReplaceDeleted
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_allowReplaceDeleted(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcfo = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcfo->allow_replace_deleted;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    oldDefaults
 * Signature: (JII)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_oldDefaults(
    JNIEnv *, jclass, jlong jhandle, jint major_version, jint minor_version) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OldDefaults(major_version, minor_version);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_optimizeForSmallDb__J(
    JNIEnv *, jobject, jlong jhandle) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_optimizeForSmallDb__JJ(
    JNIEnv *, jclass, jlong jhandle, jlong cache_handle) {
  auto *cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *>(
          cache_handle);
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OptimizeForSmallDb(cache_sptr_ptr);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_optimizeForPointLookup(
    JNIEnv *, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_optimizeLevelStyleCompaction(
    JNIEnv *, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_optimizeUniversalStyleCompaction(
    JNIEnv *, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setComparatorHandle__JI(
    JNIEnv *, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                           VectorColumnFamilyOptions *>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                           VectorColumnFamilyOptions *>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setComparatorHandle__JJB(
    JNIEnv *, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator *comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback *>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator *>(jcomparator_handle);
      break;
  }
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMergeOperatorName
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMergeOperatorName(
    JNIEnv *env, jobject, jlong jhandle, jstring jop_name) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  const char *op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);
  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMergeOperator
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMergeOperator(
    JNIEnv *, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator> *>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionFilterHandle(
    JNIEnv *, jobject, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter *>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionFilterFactoryHandle(
    JNIEnv *, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto *cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactoryJniCallback> *>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setWriteBufferSize(
    JNIEnv *env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                         VectorColumnFamilyOptions *>(jhandle)
        ->write_buffer_size = jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_writeBufferSize(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxWriteBufferNumber(
    JNIEnv *, jobject, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_maxWriteBufferNumber(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMinWriteBufferNumberToMerge(
    JNIEnv *, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_minWriteBufferNumberToMerge(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_compressionType(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vcf_opts->compression);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompressionPerLevel(
    JNIEnv *env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto uptr_compression_levels =
      rocksdb_vector_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_VectorColumnFamilyOptions_compressionPerLevel(
    JNIEnv *env, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return rocksdb_vector_compression_list_helper(
      env, vcf_opts->compression_per_level);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBottommostCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_bottommostCompressionType(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vcf_opts->bottommost_compression);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBottommostCompressionOptions(
    JNIEnv *, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto *bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions *>(
          jbottommost_compression_options_handle);
  vcf_opts->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompressionOptions(
    JNIEnv *, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto *compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions *>(
          jcompression_options_handle);
  vcf_opts->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_useFixedLengthPrefixExtractor(
    JNIEnv *, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_useCappedPrefixExtractor(
    JNIEnv *, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setNumLevels(JNIEnv *, jobject,
                                                             jlong jhandle,
                                                             jint jnum_levels) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->num_levels = jnum_levels;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_numLevels(JNIEnv *, jobject,
                                                          jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->num_levels;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevelZeroFileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_file_num_compaction_trigger =
      jlevel0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_levelZeroFileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevelZeroSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevelZeroSlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_slowdown_writes_trigger = jlevel0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_levelZeroSlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevelZeroStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevelZeroStopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_levelZeroStopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTargetFileSizeBase(
    JNIEnv *, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_targetFileSizeBase(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTargetFileSizeMultiplier(
    JNIEnv *, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_targetFileSizeMultiplier(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxBytesForLevelBase(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxBytesForLevelBase(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevelCompactionDynamicLevelBytes(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean
Java_org_rocksdb_VectorColumnFamilyOptions_levelCompactionDynamicLevelBytes(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxBytesForLevelMultiplier(
    JNIEnv *, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorColumnFamilyOptions_maxBytesForLevelMultiplier(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxCompactionBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxCompactionBytes(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_compaction_bytes = static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxCompactionBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxCompactionBytes(
    JNIEnv *, jobject, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                           VectorColumnFamilyOptions *>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setArenaBlockSize(
    JNIEnv *env, jobject, jlong jhandle, jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                         VectorColumnFamilyOptions *>(jhandle)
        ->arena_block_size = jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_arenaBlockSize(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setDisableAutoCompactions(
    JNIEnv *, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_disableAutoCompactions(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionStyle(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto *vcf_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_compactionStyle(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      vcf_options->compaction_style);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxTableFilesSizeFIFO(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}
/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxTableFilesSizeFIFO(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxSequentialSkipInIterations(
    JNIEnv *, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxSequentialSkipInIterations(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMemTableFactory(
    JNIEnv *, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::MemTableRepFactory *>(
              jfactory_handle));
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorColumnFamilyOptions_memTableFactoryName(
    JNIEnv *env, jobject, jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory *tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTableFactory(
    JNIEnv *, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->table_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory *>(jfactory_handle));
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_VectorColumnFamilyOptions_tableFactoryName(
    JNIEnv *env, jobject, jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  ROCKSDB_NAMESPACE::TableFactory *tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCfPaths(
    JNIEnv *env, jclass, jlong jhandle, jobjectArray path_array,
    jlongArray size_array) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_cfPathsLen(JNIEnv *, jclass,
                                                            jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(opt->cf_paths.size());
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_cfPaths(
    JNIEnv *env, jclass, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions>(
      env, jhandle, jpaths, jtarget_sizes);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setInplaceUpdateSupport(
    JNIEnv *, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}
/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_inplaceUpdateSupport(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setInplaceUpdateNumLocks(
    JNIEnv *env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                         VectorColumnFamilyOptions *>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_inplaceUpdateNumLocks(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMemtablePrefixBloomSizeRatio(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_VectorColumnFamilyOptions_memtablePrefixBloomSizeRatio(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setExperimentalMempurgeThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setExperimentalMempurgeThreshold(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jexperimental_mempurge_threshold) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->experimental_mempurge_threshold =
      static_cast<double>(jexperimental_mempurge_threshold);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    experimentalMempurgeThreshold
 * Signature: (J)D
 */
jdouble
Java_org_rocksdb_VectorColumnFamilyOptions_experimentalMempurgeThreshold(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->experimental_mempurge_threshold;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMemtableWholeKeyFiltering
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMemtableWholeKeyFiltering(
    JNIEnv *, jobject, jlong jhandle, jboolean jmemtable_whole_key_filtering) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->memtable_whole_key_filtering =
      static_cast<bool>(jmemtable_whole_key_filtering);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    memtableWholeKeyFiltering
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_memtableWholeKeyFiltering(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->memtable_whole_key_filtering;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBloomLocality(
    JNIEnv *, jobject, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->bloom_locality = static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_bloomLocality(JNIEnv *, jobject,
                                                              jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->bloom_locality;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxSuccessiveMerges(
    JNIEnv *env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                         VectorColumnFamilyOptions *>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_maxSuccessiveMerges(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setOptimizeFiltersForHits(
    JNIEnv *, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_optimizeFiltersForHits(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMemtableHugePageSize(
    JNIEnv *env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                         VectorColumnFamilyOptions *>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_memtableHugePageSize(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setSoftPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_softPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setHardPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    hardPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_hardPendingCompactionBytesLimit(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevel0FileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_level0FileNumCompactionTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevel0SlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_level0SlowdownWritesTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setLevel0StopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_level0StopWritesTrigger(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv *env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint *additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto *cf_opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  cf_opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray
Java_org_rocksdb_VectorColumnFamilyOptions_maxBytesForLevelMultiplierAdditional(
    JNIEnv *env, jobject, jlong jhandle) {
  auto mbflma = reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                                     VectorColumnFamilyOptions *>(jhandle)
                    ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint *additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setParanoidFileChecks(
    JNIEnv *, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->paranoid_file_checks = static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_paranoidFileChecks(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMaxWriteBufferNumberToMaintain(
    JNIEnv *, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_maxWriteBufferNumberToMaintain(
    JNIEnv *, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::
                              VectorColumnFamilyOptions *>(jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionPriority(
    JNIEnv *, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_compactionPriority(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      vcf_opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setReportBgIoStats(
    JNIEnv *, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_reportBgIoStats(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcf_opts->report_bg_io_stats;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setTtl(JNIEnv *, jobject,
                                                       jlong jhandle,
                                                       jlong jttl) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    ttl
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_ttl(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(vcf_opts->ttl);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setPeriodicCompactionSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setPeriodicCompactionSeconds(
    JNIEnv *, jobject, jlong jhandle, jlong jperiodic_compaction_seconds) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->periodic_compaction_seconds =
      static_cast<uint64_t>(jperiodic_compaction_seconds);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    periodicCompactionSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_periodicCompactionSeconds(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(vcf_opts->periodic_compaction_seconds);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionOptionsUniversal(
    JNIEnv *, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto *opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal *>(
          jcompaction_options_universal_handle);
  vcf_opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionOptionsFIFO(
    JNIEnv *, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto *opts_fifo =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO *>(
          jcompaction_options_fifo_handle);
  vcf_opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setForceConsistencyChecks(
    JNIEnv *, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->force_consistency_checks =
      static_cast<bool>(jforce_consistency_checks);
}
/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_forceConsistencyChecks(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcf_opts->force_consistency_checks;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setSstPartitionerFactory(
    JNIEnv *, jobject, jlong jhandle, jlong factory_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory> *>(
      factory_handle);
  options->sst_partitioner_factory = *factory;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setCompactionThreadLimiter(
    JNIEnv *, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  auto *limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter> *>(
      jlimiter_handle);
  options->compaction_thread_limiter = *limiter;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setEnableBlobFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setEnableBlobFiles(
    JNIEnv *, jobject, jlong jhandle, jboolean jenable_blob_files) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->enable_blob_files = static_cast<bool>(jenable_blob_files);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    enableBlobFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_enableBlobFiles(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return vcf_opts->enable_blob_files;
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setMinBlobSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setMinBlobSize(
    JNIEnv *, jobject, jlong jhandle, jlong jmin_blob_size) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->min_blob_size = static_cast<uint64_t>(jmin_blob_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    minBlobSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_minBlobSize(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(vcf_opts->min_blob_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobFileSize(
    JNIEnv *, jobject, jlong jhandle, jlong jblob_file_size) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_file_size = static_cast<uint64_t>(jblob_file_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_blobFileSize(JNIEnv *, jobject,
                                                              jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(vcf_opts->blob_file_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobCompressionType(
    JNIEnv *, jobject, jlong jhandle, jbyte jblob_compression_type_value) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_compression_type =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jblob_compression_type_value);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_blobCompressionType(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      vcf_opts->blob_compression_type);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setEnableBlobGarbageCollection
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setEnableBlobGarbageCollection(
    JNIEnv *, jobject, jlong jhandle,
    jboolean jenable_blob_garbage_collection) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->enable_blob_garbage_collection =
      static_cast<bool>(jenable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    enableBlobGarbageCollection
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorColumnFamilyOptions_enableBlobGarbageCollection(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jboolean>(vcf_opts->enable_blob_garbage_collection);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobGarbageCollectionAgeCutoff
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobGarbageCollectionAgeCutoff(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jblob_garbage_collection_age_cutoff) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_garbage_collection_age_cutoff =
      static_cast<double>(jblob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobGarbageCollectionAgeCutoff
 * Signature: (J)D
 */
jdouble
Java_org_rocksdb_VectorColumnFamilyOptions_blobGarbageCollectionAgeCutoff(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jdouble>(vcf_opts->blob_garbage_collection_age_cutoff);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobGarbageCollectionForceThreshold
 * Signature: (JD)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobGarbageCollectionForceThreshold(
    JNIEnv *, jobject, jlong jhandle,
    jdouble jblob_garbage_collection_force_threshold) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_garbage_collection_force_threshold =
      static_cast<double>(jblob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobGarbageCollectionForceThreshold
 * Signature: (J)D
 */
jdouble
Java_org_rocksdb_VectorColumnFamilyOptions_blobGarbageCollectionForceThreshold(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jdouble>(
      vcf_opts->blob_garbage_collection_force_threshold);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobCompactionReadaheadSize(
    JNIEnv *, jobject, jlong jhandle, jlong jblob_compaction_readahead_size) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_compaction_readahead_size =
      static_cast<uint64_t>(jblob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobCompactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorColumnFamilyOptions_blobCompactionReadaheadSize(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jlong>(vcf_opts->blob_compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setBlobFileStartingLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setBlobFileStartingLevel(
    JNIEnv *, jobject, jlong jhandle, jint jblob_file_starting_level) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->blob_file_starting_level =
      static_cast<int>(jblob_file_starting_level);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    blobFileStartingLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_blobFileStartingLevel(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jint>(vcf_opts->blob_file_starting_level);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setPrepopulateBlobCache
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setPrepopulateBlobCache(
    JNIEnv *, jobject, jlong jhandle, jbyte jprepopulate_blob_cache_value) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->prepopulate_blob_cache =
      ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toCppPrepopulateBlobCache(
          jprepopulate_blob_cache_value);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    prepopulateBlobCache
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorColumnFamilyOptions_prepopulateBlobCache(
    JNIEnv *, jobject, jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return ROCKSDB_NAMESPACE::PrepopulateBlobCacheJni::toJavaPrepopulateBlobCache(
      vcf_opts->prepopulate_blob_cache);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    setFlushThreshold
 * Signature: (JI)V
 */
void Java_org_rocksdb_VectorColumnFamilyOptions_setFlushThreshold(
    JNIEnv *, jobject, jlong jhandle, jint jflush_threshold) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  vcf_opts->flush_threshold = static_cast<int>(jflush_threshold);
}

/*
 * Class:     org_rocksdb_VectorColumnFamilyOptions
 * Method:    flushThreshold
 * Signature: (J)I
 */
jint Java_org_rocksdb_VectorColumnFamilyOptions_flushThreshold(JNIEnv *,
                                                               jobject,
                                                               jlong jhandle) {
  auto *vcf_opts = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorColumnFamilyOptions *>(
      jhandle);
  return static_cast<jint>(vcf_opts->flush_threshold);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    newVectorSearchOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_VectorSearchOptions_newVectorSearchOptions__(JNIEnv *,
                                                                    jclass) {
  auto *vector_search_options =
      new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions();
  return GET_CPLUSPLUS_POINTER(vector_search_options);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    newVectorSearchOptions
 * Signature: (ZZ)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_newVectorSearchOptions__ZZ(
    JNIEnv *, jclass, jboolean jverify_checksums, jboolean jfill_cache) {
  auto *read_options =
      new ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions();
  read_options->verify_checksums = static_cast<bool>(jverify_checksums);
  read_options->fill_cache = static_cast<bool>(jfill_cache);
  return GET_CPLUSPLUS_POINTER(read_options);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    copyVectorSearchOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_copyVectorSearchOptions(
    JNIEnv *, jclass, jlong jhandle) {
  auto *new_opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return GET_CPLUSPLUS_POINTER(new_opt);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    k
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_k(JNIEnv *, jclass, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->k;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setK
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setK(JNIEnv *, jclass, jlong jhandle,
                                               jlong jk) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->k = static_cast<size_t>(jk);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    ts
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_ts(JNIEnv *, jclass, jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->ts;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTs
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTs(JNIEnv *, jclass, jlong jhandle,
                                                jlong jts) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->ts = static_cast<size_t>(jts);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    evict
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_evict(JNIEnv *, jclass,
                                                    jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->is_evict;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setEvict
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setEvict(JNIEnv *, jclass,
                                                   jlong jhandle,
                                                   jboolean jevcit) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->is_evict = static_cast<bool>(jevcit);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    triggerSort
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_triggerSort(JNIEnv *, jclass,
                                                          jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->trigger_sort;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTriggerSort
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTriggerSort(
    JNIEnv *, jclass, jlong jhandle, jboolean jtrigger_sort) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->trigger_sort = static_cast<bool>(jtrigger_sort);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    terminationFactor
 * Signature: (J)F
 */
jfloat Java_org_rocksdb_VectorSearchOptions_terminationFactor(JNIEnv *, jclass,
                                                              jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->termination_factor;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTerminationFactor
 * Signature: (JF)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTerminationFactor(
    JNIEnv *, jclass, jlong jhandle, jfloat jtermination_factor) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->termination_factor =
      static_cast<float>(jtermination_factor);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    searchSST
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_searchSST(JNIEnv *, jclass,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->search_sst;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setSearchSST
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setSearchSST(JNIEnv *, jclass,
                                                       jlong jhandle,
                                                       jboolean jsearch_sst) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->search_sst = static_cast<bool>(jsearch_sst);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_VectorSearchOptions_disposeInternal(JNIEnv *, jobject,
                                                          jlong jhandle) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  assert(vector_search_options != nullptr);
  delete vector_search_options;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    verifyChecksums
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_verifyChecksums(JNIEnv *, jobject,
                                                              jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->verify_checksums;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setVerifyChecksums
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setVerifyChecksums(
    JNIEnv *, jobject, jlong jhandle, jboolean jverify_checksums) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->verify_checksums =
      static_cast<bool>(jverify_checksums);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    fillCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_fillCache(JNIEnv *, jobject,
                                                        jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->fill_cache;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setFillCache
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setFillCache(JNIEnv *, jobject,
                                                       jlong jhandle,
                                                       jboolean jfill_cache) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->fill_cache = static_cast<bool>(jfill_cache);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    snapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_snapshot(JNIEnv *, jobject,
                                                    jlong jhandle) {
  auto &snapshot =
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
          jhandle)
          ->snapshot;
  return GET_CPLUSPLUS_POINTER(snapshot);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setSnapshot(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jlong jsnapshot) {
  auto *vector_search_options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  vector_search_options->snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::Snapshot *>(jsnapshot);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    readTier
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_VectorSearchOptions_readTier(JNIEnv *, jobject,
                                                    jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
          jhandle)
          ->read_tier);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setReadTier
 * Signature: (JB)V
 */
void Java_org_rocksdb_VectorSearchOptions_setReadTier(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jbyte jread_tier) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->read_tier = static_cast<ROCKSDB_NAMESPACE::ReadTier>(jread_tier);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    tailing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_tailing(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->tailing;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTailing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTailing(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jboolean jtailing) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->tailing = static_cast<bool>(jtailing);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    managed
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_managed(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->managed;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setManaged
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setManaged(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jboolean jmanaged) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->managed = static_cast<bool>(jmanaged);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    totalOrderSeek
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_totalOrderSeek(JNIEnv *, jobject,
                                                             jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->total_order_seek;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTotalOrderSeek
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTotalOrderSeek(
    JNIEnv *, jobject, jlong jhandle, jboolean jtotal_order_seek) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->total_order_seek = static_cast<bool>(jtotal_order_seek);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    prefixSameAsStart
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_prefixSameAsStart(JNIEnv *,
                                                                jobject,
                                                                jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->prefix_same_as_start;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setPrefixSameAsStart
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setPrefixSameAsStart(
    JNIEnv *, jobject, jlong jhandle, jboolean jprefix_same_as_start) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->prefix_same_as_start = static_cast<bool>(jprefix_same_as_start);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    pinData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_pinData(JNIEnv *, jobject,
                                                      jlong jhandle) {
  return reinterpret_cast<
             ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
             jhandle)
      ->pin_data;
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setPinData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setPinData(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jboolean jpin_data) {
  reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle)
      ->pin_data = static_cast<bool>(jpin_data);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    backgroundPurgeOnIteratorCleanup
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_backgroundPurgeOnIteratorCleanup(
    JNIEnv *, jobject, jlong jhandle) {
  auto options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jboolean>(options->background_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setBackgroundPurgeOnIteratorCleanup
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setBackgroundPurgeOnIteratorCleanup(
    JNIEnv *, jobject, jlong jhandle,
    jboolean jbackground_purge_on_iterator_cleanup) {
  auto options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->background_purge_on_iterator_cleanup =
      static_cast<bool>(jbackground_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    readaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_readaheadSize(JNIEnv *, jobject,
                                                         jlong jhandle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jlong>(options->readahead_size);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setReadaheadSize(
    JNIEnv *, jobject, jlong jhandle, jlong jreadahead_size) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->readahead_size = static_cast<size_t>(jreadahead_size);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    maxSkippableInternalKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_maxSkippableInternalKeys(
    JNIEnv *, jobject, jlong jhandle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jlong>(options->max_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setMaxSkippableInternalKeys
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setMaxSkippableInternalKeys(
    JNIEnv *, jobject, jlong jhandle, jlong jmax_skippable_internal_keys) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->max_skippable_internal_keys =
      static_cast<uint64_t>(jmax_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    ignoreRangeDeletions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_ignoreRangeDeletions(
    JNIEnv *, jobject, jlong jhandle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jboolean>(options->ignore_range_deletions);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setIgnoreRangeDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setIgnoreRangeDeletions(
    JNIEnv *, jobject, jlong jhandle, jboolean jignore_range_deletions) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->ignore_range_deletions = static_cast<bool>(jignore_range_deletions);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setIterateUpperBound
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setIterateUpperBound(
    JNIEnv *, jobject, jlong jhandle, jlong jupper_bound_slice_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->iterate_upper_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice *>(jupper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    iterateUpperBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_iterateUpperBound(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto &upper_bound_slice_handle =
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
          jhandle)
          ->iterate_upper_bound;
  return GET_CPLUSPLUS_POINTER(upper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setIterateLowerBound
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setIterateLowerBound(
    JNIEnv *, jobject, jlong jhandle, jlong jlower_bound_slice_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->iterate_lower_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice *>(jlower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    iterateLowerBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_iterateLowerBound(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto &lower_bound_slice_handle =
      reinterpret_cast<
          ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
          jhandle)
          ->iterate_lower_bound;
  return GET_CPLUSPLUS_POINTER(lower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTableFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTableFilter(
    JNIEnv *, jobject, jlong jhandle, jlong jjni_table_filter_handle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  auto *jni_table_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::TableFilterJniCallback *>(
          jjni_table_filter_handle);
  options->table_filter = jni_table_filter->GetTableFilterFunction();
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    autoPrefixMode
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_autoPrefixMode(JNIEnv *, jobject,
                                                             jlong jhandle) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jboolean>(options->auto_prefix_mode);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setAutoPrefixMode
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setAutoPrefixMode(
    JNIEnv *, jobject, jlong jhandle, jboolean jauto_prefix_mode) {
  auto *options = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  options->auto_prefix_mode = static_cast<bool>(jauto_prefix_mode);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    timestamp
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_timestamp(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  auto &timestamp_slice_handle = opt->timestamp;
  return reinterpret_cast<jlong>(timestamp_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setTimestamp
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setTimestamp(
    JNIEnv *, jobject, jlong jhandle, jlong jtimestamp_slice_handle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->timestamp =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice *>(jtimestamp_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    iterStartTs
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_iterStartTs(JNIEnv *, jobject,
                                                       jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  auto &iter_start_ts_handle = opt->iter_start_ts;
  return reinterpret_cast<jlong>(iter_start_ts_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setIterStartTs
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setIterStartTs(
    JNIEnv *, jobject, jlong jhandle, jlong jiter_start_ts_slice_handle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->iter_start_ts =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice *>(jiter_start_ts_slice_handle);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    deadline
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_deadline(JNIEnv *, jobject,
                                                    jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jlong>(opt->deadline.count());
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setDeadline
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setDeadline(JNIEnv *, jobject,
                                                      jlong jhandle,
                                                      jlong jdeadline) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->deadline = std::chrono::milliseconds(static_cast<int64_t>(jdeadline));
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    ioTimeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_ioTimeout(JNIEnv *, jobject,
                                                     jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jlong>(opt->io_timeout.count());
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setIoTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setIoTimeout(JNIEnv *, jobject,
                                                       jlong jhandle,
                                                       jlong jio_timeout) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->io_timeout =
      std::chrono::milliseconds(static_cast<int64_t>(jio_timeout));
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    valueSizeSoftLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorSearchOptions_valueSizeSoftLimit(JNIEnv *, jobject,
                                                              jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jlong>(opt->value_size_soft_limit);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setValueSizeSoftLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setValueSizeSoftLimit(
    JNIEnv *, jobject, jlong jhandle, jlong jvalue_size_soft_limit) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->value_size_soft_limit = static_cast<uint64_t>(jvalue_size_soft_limit);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    asyncIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_VectorSearchOptions_asyncIO(JNIEnv *, jobject,
                                                      jlong jhandle) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  return static_cast<jboolean>(opt->async_io);
}

/*
 * Class:     org_rocksdb_VectorSearchOptions
 * Method:    setAsyncIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_VectorSearchOptions_setAsyncIO(JNIEnv *, jobject,
                                                     jlong jhandle,
                                                     jboolean jasync_io) {
  auto *opt = reinterpret_cast<
      ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE::VectorSearchOptions *>(
      jhandle);
  opt->async_io = static_cast<bool>(jasync_io);
}
