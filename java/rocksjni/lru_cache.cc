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
#include "rocksjni/portal.h"

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

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    enableGhostCache
 * Signature: (JJ[J)V
 */
void Java_org_rocksdb_LRUCache_enableGhostCache(JNIEnv* env, jobject /*jobj*/,
                                                jlong jhandle, jlong jghost_capacity,
                                                jlongArray jdistance_buckets) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  auto* lru_cache = dynamic_cast<ROCKSDB_NAMESPACE::LRUCache*>(sptr_cache->get());
  if (lru_cache == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Cache is not an LRUCache instance");
    return;
  }

  // Convert Java array to C++ vector
  jsize len = env->GetArrayLength(jdistance_buckets);
  jlong* buckets = env->GetLongArrayElements(jdistance_buckets, nullptr);
  std::vector<uint64_t> distance_buckets;
  for (jsize i = 0; i < len; i++) {
    distance_buckets.push_back(static_cast<uint64_t>(buckets[i]));
  }
  env->ReleaseLongArrayElements(jdistance_buckets, buckets, JNI_ABORT);

  // Enable ghost cache on all shards
  int num_shard_bits = lru_cache->GetNumShardBits();
  int num_shards = (num_shard_bits > 0) ? (1 << num_shard_bits) : 1;
  for (int i = 0; i < num_shards; i++) {
    auto* shard = dynamic_cast<ROCKSDB_NAMESPACE::LRUCacheShard*>(
        lru_cache->GetShard(i));
    if (shard != nullptr) {
      shard->EnableGhostCache(static_cast<size_t>(jghost_capacity),
                              distance_buckets);
    }
  }
}

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    getMissRateCurve
 * Signature: (J)Lorg/rocksdb/MissRateCurveResult;
 */
jobject Java_org_rocksdb_LRUCache_getMissRateCurve(JNIEnv* env, jobject /*jobj*/,
                                                   jlong jhandle) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  auto* lru_cache = dynamic_cast<ROCKSDB_NAMESPACE::LRUCache*>(sptr_cache->get());
  if (lru_cache == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Cache is not an LRUCache instance");
    return nullptr;
  }

  // Use GetBucketStatistics for proper weighted aggregation
  ROCKSDB_NAMESPACE::LRUCacheShard::BucketStatistics stats =
      lru_cache->GetBucketStatistics();

  if (stats.cache_sizes.empty()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Ghost cache is not enabled or no statistics available");
    return nullptr;
  }

  std::vector<uint64_t> cache_sizes;
  std::vector<double> miss_rates;

  // Calculate miss rates from aggregated bucket statistics
  for (size_t i = 0; i < stats.cache_sizes.size(); i++) {
    uint64_t total_accesses = stats.hits[i] + stats.misses[i];
    if (total_accesses > 0) {
      cache_sizes.push_back(stats.cache_sizes[i]);
      miss_rates.push_back(static_cast<double>(stats.misses[i]) / total_accesses);
    }
  }

  // Create Java MissRateCurveResult object
  jclass jcls = env->FindClass("org/rocksdb/MissRateCurveResult");
  if (jcls == nullptr) {
    return nullptr;
  }

  jmethodID jconstructor = env->GetMethodID(jcls, "<init>", "([J[D)V");
  if (jconstructor == nullptr) {
    return nullptr;
  }

  // Create Java arrays
  jlongArray jcache_sizes = env->NewLongArray(static_cast<jsize>(cache_sizes.size()));
  jdoubleArray jmiss_rates = env->NewDoubleArray(static_cast<jsize>(miss_rates.size()));

  if (jcache_sizes == nullptr || jmiss_rates == nullptr) {
    return nullptr;
  }

  jlong* cache_sizes_data = new jlong[cache_sizes.size()];
  jdouble* miss_rates_data = new jdouble[miss_rates.size()];
  
  for (size_t i = 0; i < cache_sizes.size(); i++) {
    cache_sizes_data[i] = static_cast<jlong>(cache_sizes[i]);
  }
  for (size_t i = 0; i < miss_rates.size(); i++) {
    miss_rates_data[i] = miss_rates[i];
  }

  env->SetLongArrayRegion(jcache_sizes, 0, static_cast<jsize>(cache_sizes.size()),
                          cache_sizes_data);
  env->SetDoubleArrayRegion(jmiss_rates, 0, static_cast<jsize>(miss_rates.size()),
                            miss_rates_data);

  delete[] cache_sizes_data;
  delete[] miss_rates_data;

  return env->NewObject(jcls, jconstructor, jcache_sizes, jmiss_rates);
}

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    getBucketStatistics
 * Signature: (J)Lorg/rocksdb/BucketStatistics;
 */
jobject Java_org_rocksdb_LRUCache_getBucketStatistics(JNIEnv* env, jobject /*jobj*/,
                                                      jlong jhandle) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  auto* lru_cache = dynamic_cast<ROCKSDB_NAMESPACE::LRUCache*>(sptr_cache->get());
  if (lru_cache == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Cache is not an LRUCache instance");
    return nullptr;
  }

  // Get aggregated bucket statistics from all shards
  ROCKSDB_NAMESPACE::LRUCacheShard::BucketStatistics stats =
      lru_cache->GetBucketStatistics();

  if (stats.cache_sizes.empty()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Ghost cache is not enabled or no statistics available");
    return nullptr;
  }

  // Create Java BucketStatistics object
  jclass jcls = env->FindClass("org/rocksdb/BucketStatistics");
  if (jcls == nullptr) {
    return nullptr;
  }

  jmethodID jconstructor =
      env->GetMethodID(jcls, "<init>", "([J[J[J)V");
  if (jconstructor == nullptr) {
    return nullptr;
  }

  // Create Java arrays
  jsize size = static_cast<jsize>(stats.cache_sizes.size());
  jlongArray jcache_sizes = env->NewLongArray(size);
  jlongArray jhits = env->NewLongArray(size);
  jlongArray jmisses = env->NewLongArray(size);

  if (jcache_sizes == nullptr || jhits == nullptr || jmisses == nullptr) {
    return nullptr;
  }

  jlong* cache_sizes_data = new jlong[size];
  jlong* hits_data = new jlong[size];
  jlong* misses_data = new jlong[size];

  for (jsize i = 0; i < size; i++) {
    cache_sizes_data[i] = static_cast<jlong>(stats.cache_sizes[i]);
    hits_data[i] = static_cast<jlong>(stats.hits[i]);
    misses_data[i] = static_cast<jlong>(stats.misses[i]);
  }

  env->SetLongArrayRegion(jcache_sizes, 0, size, cache_sizes_data);
  env->SetLongArrayRegion(jhits, 0, size, hits_data);
  env->SetLongArrayRegion(jmisses, 0, size, misses_data);

  delete[] cache_sizes_data;
  delete[] hits_data;
  delete[] misses_data;

  return env->NewObject(jcls, jconstructor, jcache_sizes, jhits, jmisses);
}

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    resetMRCStats
 * Signature: (J)V
 */
void Java_org_rocksdb_LRUCache_resetMRCStats(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  auto* lru_cache = dynamic_cast<ROCKSDB_NAMESPACE::LRUCache*>(sptr_cache->get());
  if (lru_cache == nullptr) {
    return;  // Not an LRUCache, silently ignore
  }

  // Reset statistics on all shards
  int num_shard_bits = lru_cache->GetNumShardBits();
  int num_shards = (num_shard_bits > 0) ? (1 << num_shard_bits) : 1;
  for (int i = 0; i < num_shards; i++) {
    auto* shard = dynamic_cast<ROCKSDB_NAMESPACE::LRUCacheShard*>(
        lru_cache->GetShard(i));
    if (shard != nullptr) {
      shard->ResetMRCStats();
    }
  }
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
