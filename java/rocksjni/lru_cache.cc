// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::LRUCache.

#include <jni.h>
#include <vector>

#include "cache/lru_cache.h"
#include "include/org_rocksdb_LRUCache.h"

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    newLRUCache
 * Signature: (JIZD)J
 */
jlong Java_org_rocksdb_LRUCache_newLRUCache(JNIEnv* /*env*/, jclass /*jcls*/,
                                            jlong jcapacity,
                                            jint jnum_shard_bits,
                                            jboolean jstrict_capacity_limit,
                                            jdouble jhigh_pri_pool_ratio) {
  auto* sptr_lru_cache = new std::shared_ptr<ROCKSDB_NAMESPACE::Cache>(
      ROCKSDB_NAMESPACE::NewLRUCache(
          static_cast<size_t>(jcapacity), static_cast<int>(jnum_shard_bits),
          static_cast<bool>(jstrict_capacity_limit),
          static_cast<double>(jhigh_pri_pool_ratio)));
  return reinterpret_cast<jlong>(sptr_lru_cache);
}

jlong Java_org_rocksdb_LRUCache_newLRUCacheWithQuickMRC(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jcapacity, jint jnum_shard_bits,
    jboolean jstrict_capacity_limit, jdouble jhigh_pri_pool_ratio,
    jboolean jquick_mrc_enabled, jint jquick_mrc_max_bucket_size,
    jint jquick_mrc_ghost_cache_multiplier, jdouble jquick_mrc_sampling_rate,
    jint jquick_mrc_histogram_bin_size) {
  auto* sptr_lru_cache = new std::shared_ptr<ROCKSDB_NAMESPACE::Cache>(
      ROCKSDB_NAMESPACE::NewLRUCache(
          static_cast<size_t>(jcapacity), static_cast<int>(jnum_shard_bits),
          static_cast<bool>(jstrict_capacity_limit),
          static_cast<double>(jhigh_pri_pool_ratio), nullptr,
          ROCKSDB_NAMESPACE::kDefaultToAdaptiveMutex,
          ROCKSDB_NAMESPACE::kDefaultCacheMetadataChargePolicy,
          static_cast<bool>(jquick_mrc_enabled),
          static_cast<uint32_t>(jquick_mrc_max_bucket_size),
          static_cast<uint32_t>(jquick_mrc_ghost_cache_multiplier),
          static_cast<double>(jquick_mrc_sampling_rate),
          static_cast<uint32_t>(jquick_mrc_histogram_bin_size)));
  return reinterpret_cast<jlong>(sptr_lru_cache);
}

jlongArray Java_org_rocksdb_LRUCache_getStackDistanceHistogram(
    JNIEnv* env, jclass /*jcls*/, jlong jhandle) {
  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  std::vector<uint64_t> histogram =
      (*sptr_lru_cache)->GetQuickMRCStackDistanceHistogram();
  jlongArray result = env->NewLongArray(static_cast<jsize>(histogram.size()));
  if (result == nullptr || histogram.empty()) {
    return result;
  }
  std::vector<jlong> java_histogram(histogram.begin(), histogram.end());
  env->SetLongArrayRegion(result, 0, static_cast<jsize>(java_histogram.size()),
                          java_histogram.data());
  return result;
}

void Java_org_rocksdb_LRUCache_resetQuickMRCStats(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jlong jhandle) {
  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  (*sptr_lru_cache)->ResetQuickMRCStats();
}

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_LRUCache_disposeInternal(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jhandle) {
  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  delete sptr_lru_cache;  // delete std::shared_ptr
}
