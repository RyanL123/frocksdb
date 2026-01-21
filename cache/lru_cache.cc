//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/lru_cache.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <string>

#include "rocksdb/slice.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

LRUHandleTable::LRUHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

LRUHandleTable::~LRUHandleTable() {
  ApplyToAllCacheEntries([](LRUHandle* h) {
    if (!h->HasRefs()) {
      h->Free();
    }
  });
  delete[] list_;
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
  LRUHandle** ptr = FindPointer(h->key(), h->hash);
  LRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

LRUHandle** LRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void LRUHandleTable::Resize() {
  uint32_t new_length = 16;
  while (new_length < elems_ * 1.5) {
    new_length *= 2;
  }
  LRUHandle** new_list = new LRUHandle*[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  uint32_t count = 0;
  for (uint32_t i = 0; i < length_; i++) {
    LRUHandle* h = list_[i];
    while (h != nullptr) {
      LRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      LRUHandle** ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

LRUCacheShard::LRUCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy)
    : ghost_cache_enabled_(false),
      ghost_lru_tail_(nullptr),
      ghost_cache_size_(0),
      ghost_cache_capacity_(0),
      evict_sequence_(0),
      access_sequence_(0),
      capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
  // Initialize ghost cache list
  ghost_lru_head_.next = &ghost_lru_head_;
  ghost_lru_head_.prev = &ghost_lru_head_;
  ghost_lru_tail_ = &ghost_lru_head_;
  SetCapacity(capacity);
}

void LRUCacheShard::EraseUnRefEntries() {
  autovector<LRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      // LRU list contains only elements which can be evicted
      assert(old->InCache() && !old->HasRefs());
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      size_t total_charge = old->CalcTotalCharge(metadata_charge_policy_);
      assert(usage_ >= total_charge);
      usage_ -= total_charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                           bool thread_safe) {
  const auto applyCallback = [&]() {
    table_.ApplyToAllCacheEntries(
        [callback](LRUHandle* h) { callback(h->value, h->charge); });
  };

  if (thread_safe) {
    MutexLock l(&mutex_);
    applyCallback();
  } else {
    applyCallback();
  }
}

void LRUCacheShard::TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri) {
  MutexLock l(&mutex_);
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
}

size_t LRUCacheShard::TEST_GetLRUSize() {
  MutexLock l(&mutex_);
  LRUHandle* lru_handle = lru_.next;
  size_t lru_size = 0;
  while (lru_handle != &lru_) {
    lru_size++;
    lru_handle = lru_handle->next;
  }
  return lru_size;
}

double LRUCacheShard::GetHighPriPoolRatio() {
  MutexLock l(&mutex_);
  return high_pri_pool_ratio_;
}

void LRUCacheShard::LRU_Remove(LRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(lru_usage_ >= total_charge);
  lru_usage_ -= total_charge;
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void LRUCacheShard::LRU_Insert(LRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += total_charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of LRU list.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    lru_low_pri_ = e;
  }
  lru_usage_ += total_charge;
}

void LRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    assert(lru_low_pri_ != &lru_);
    lru_low_pri_->SetInHighPriPool(false);
    size_t total_charge =
        lru_low_pri_->CalcTotalCharge(metadata_charge_policy_);
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void LRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<LRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    // LRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    
    // Insert into ghost cache before eviction
    if (ghost_cache_enabled_) {
      uint64_t seq = evict_sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
      InsertIntoGhostCache(old->key(), old->hash,
                          old->CalcTotalCharge(metadata_charge_policy_),
                          seq);
    }
    
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    assert(usage_ >= old_total_charge);
    usage_ -= old_total_charge;
    deleted->push_back(old);
  }
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  autovector<LRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromLRU(0, &last_reference_list);
  }

  // Free the entries outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Cache::Handle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  
  if (ghost_cache_enabled_) {
    uint64_t seq = access_sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (e != nullptr) {
      // Cache hit
      RecordAccessInBuckets(key, hash, seq, true);
    } else {
      // Cache miss - check ghost cache for stack distance estimation
      RecordAccessInBuckets(key, hash, seq, false);
    }
  }
  
  if (e != nullptr) {
    assert(e->InCache());
    if (!e->HasRefs()) {
      // The entry is in LRU since it's in hash and has no external references
      LRU_Remove(e);
    }
    e->Ref();
    e->SetHit();
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool LRUCacheShard::Ref(Cache::Handle* h) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void LRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  MutexLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool LRUCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = e->Unref();
    if (last_reference && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // The LRU list must be empty since the cache is full
        assert(lru_.next == &lru_ || force_erase);
        // Take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
      } else {
        // Put the item back on the LRU list, and don't free it
        LRU_Insert(e);
        last_reference = false;
      }
    }
    if (last_reference) {
      size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
      assert(usage_ >= total_charge);
      usage_ -= total_charge;
    }
  }

  // Free the entry here outside of mutex for performance reasons
  if (last_reference) {
    e->Free();
  }
  return last_reference;
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value),
                             Cache::Handle** handle, Cache::Priority priority) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      new char[sizeof(LRUHandle) - 1 + key.size()]);
  Status s = Status::OK();
  autovector<LRUHandle*> last_reference_list;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->flags = 0;
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);

  {
    MutexLock l(&mutex_);

    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty
    EvictFromLRU(total_charge, &last_reference_list);

    if ((usage_ + total_charge) > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        e->SetInCache(false);
        last_reference_list.push_back(e);
      } else {
        delete[] reinterpret_cast<char*>(e);
        *handle = nullptr;
        s = Status::Incomplete("Insert failed due to LRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      LRUHandle* old = table_.Insert(e);
      usage_ += total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on LRU because it's in cache and its reference count is 0
          LRU_Remove(old);
          size_t old_total_charge =
              old->CalcTotalCharge(metadata_charge_policy_);
          assert(usage_ >= old_total_charge);
          usage_ -= old_total_charge;
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        LRU_Insert(e);
      } else {
        e->Ref();
        *handle = reinterpret_cast<Cache::Handle*>(e);
      }
    }
  }

  // Free the entries here outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }

  return s;
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      if (!e->HasRefs()) {
        // The entry is in LRU since it's in hash and has no external references
        LRU_Remove(e);
        size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
        assert(usage_ >= total_charge);
        usage_ -= total_charge;
        last_reference = true;
      }
    }
  }

  // Free the entry here outside of mutex for performance reasons
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    e->Free();
  }
}

size_t LRUCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t LRUCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

std::string LRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(LRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LRUCacheShard(per_shard, strict_capacity_limit, high_pri_pool_ratio,
                      use_adaptive_mutex, metadata_charge_policy);
  }
}

LRUCache::~LRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~LRUCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* LRUCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LRUCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LRUCache::Value(Handle* handle) {
  return reinterpret_cast<const LRUHandle*>(handle)->value;
}

size_t LRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->charge;
}

uint32_t LRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->hash;
}

void LRUCache::DisownData() {
// Do not drop data if compile with ASAN to suppress leak warning.
#if defined(__clang__)
#if !defined(__has_feature) || !__has_feature(address_sanitizer)
  shards_ = nullptr;
  num_shards_ = 0;
#endif
#else  // __clang__
#ifndef __SANITIZE_ADDRESS__
  shards_ = nullptr;
  num_shards_ = 0;
#endif  // !__SANITIZE_ADDRESS__
#endif  // __clang__
}

size_t LRUCache::TEST_GetLRUSize() {
  size_t lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lru_size_of_all_shards += shards_[i].TEST_GetLRUSize();
  }
  return lru_size_of_all_shards;
}

double LRUCache::GetHighPriPoolRatio() {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

LRUCacheShard::BucketStatistics LRUCache::GetBucketStatistics() const {
  LRUCacheShard::BucketStatistics aggregated;
  
  if (shards_ == nullptr || num_shards_ == 0) {
    return aggregated;
  }
  
  // Collect statistics from first shard to get bucket structure
  LRUCacheShard::BucketStatistics first = shards_[0].GetBucketStatistics();
  if (first.cache_sizes.empty()) {
    return aggregated;
  }
  
  // Initialize aggregated statistics with bucket structure
  aggregated.cache_sizes = first.cache_sizes;
  aggregated.hits.resize(first.cache_sizes.size(), 0);
  aggregated.misses.resize(first.cache_sizes.size(), 0);
  
  // Aggregate statistics from all shards
  for (int i = 0; i < num_shards_; i++) {
    LRUCacheShard::BucketStatistics shard_stats = shards_[i].GetBucketStatistics();
    // Verify bucket structure matches
    if (shard_stats.cache_sizes.size() != aggregated.cache_sizes.size()) {
      continue;  // Skip if structure doesn't match
    }
    
    for (size_t j = 0; j < aggregated.cache_sizes.size(); j++) {
      if (shard_stats.cache_sizes[j] == aggregated.cache_sizes[j]) {
        aggregated.hits[j] += shard_stats.hits[j];
        aggregated.misses[j] += shard_stats.misses[j];
      }
    }
  }
  
  return aggregated;
}

std::shared_ptr<Cache> NewLRUCache(const LRUCacheOptions& cache_opts) {
  return NewLRUCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
                     cache_opts.metadata_charge_policy);
}

std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      std::move(memory_allocator), use_adaptive_mutex, metadata_charge_policy);
}

// Ghost cache implementation functions
void LRUCacheShard::EnableGhostCache(size_t ghost_capacity,
                                     const std::vector<uint64_t>& distance_buckets) {
  MutexLock l(&mutex_);
  ghost_cache_enabled_ = true;
  ghost_cache_capacity_ = ghost_capacity;
  distance_buckets_ = distance_buckets;
  bucket_hits_.resize(distance_buckets_.size(), 0);
  bucket_misses_.resize(distance_buckets_.size(), 0);
  evict_sequence_.store(0, std::memory_order_relaxed);
  access_sequence_.store(0, std::memory_order_relaxed);
}

void LRUCacheShard::InsertIntoGhostCache(const Slice& key, uint32_t hash,
                                         size_t charge, uint64_t sequence) {
  // This function is called from EvictFromLRU which already holds the mutex
  mutex_.AssertHeld();
  
  // Check if key already exists in ghost cache
  auto it = ghost_table_.find(hash);
  if (it != ghost_table_.end()) {
    GhostCacheEntry* entry = it->second;
    // Check if it's actually the same key (hash collision)
    if (entry->key == key.ToString()) {
      // Move to front (most recently evicted)
      RemoveFromGhostList(entry);
      InsertAtGhostHead(entry);
      entry->evict_sequence = sequence;
      entry->charge = charge;
      return;
    }
  }
  
  // Create new ghost entry (no value stored, just metadata)
  GhostCacheEntry* entry = new GhostCacheEntry();
  entry->key = key.ToString();
  entry->hash = hash;
  entry->charge = charge;
  entry->evict_sequence = sequence;
  
  // Insert at head (most recently evicted)
  InsertAtGhostHead(entry);
  ghost_table_[hash] = entry;
  ghost_cache_size_++;
  
  // Evict oldest ghost entry if capacity exceeded
  while (ghost_cache_size_ > ghost_cache_capacity_ &&
         ghost_lru_tail_ != &ghost_lru_head_) {
    EvictFromGhostCache();
  }
}

bool LRUCacheShard::LookupInGhostCache(const Slice& key, uint32_t hash,
                                       uint64_t current_sequence,
                                       uint64_t* estimated_stack_distance) {
  mutex_.AssertHeld();
  
  auto it = ghost_table_.find(hash);
  if (it == ghost_table_.end()) {
    return false;  // Not in ghost cache
  }
  
  GhostCacheEntry* entry = it->second;
  // Check if it's actually the same key (hash collision)
  if (entry->key != key.ToString()) {
    return false;
  }
  
  // Estimate stack distance based on sequence difference
  // Stack distance = number of items evicted since this item
  if (current_sequence > entry->evict_sequence) {
    *estimated_stack_distance = current_sequence - entry->evict_sequence;
  } else {
    *estimated_stack_distance = 0;
  }
  
  // Move to head (recently accessed in ghost)
  RemoveFromGhostList(entry);
  InsertAtGhostHead(entry);
  entry->evict_sequence = current_sequence;
  
  return true;
}

void LRUCacheShard::RemoveFromGhostList(GhostCacheEntry* entry) {
  mutex_.AssertHeld();
  entry->prev->next = entry->next;
  entry->next->prev = entry->prev;
  if (ghost_lru_tail_ == entry) {
    ghost_lru_tail_ = entry->prev;
  }
}

void LRUCacheShard::InsertAtGhostHead(GhostCacheEntry* entry) {
  mutex_.AssertHeld();
  entry->next = ghost_lru_head_.next;
  entry->prev = &ghost_lru_head_;
  ghost_lru_head_.next->prev = entry;
  ghost_lru_head_.next = entry;
  if (ghost_lru_tail_ == &ghost_lru_head_) {
    ghost_lru_tail_ = entry;
  }
}

void LRUCacheShard::EvictFromGhostCache() {
  mutex_.AssertHeld();
  if (ghost_lru_tail_ == &ghost_lru_head_) {
    return;
  }
  
  GhostCacheEntry* to_remove = ghost_lru_tail_;
  ghost_lru_tail_ = ghost_lru_tail_->prev;
  ghost_table_.erase(to_remove->hash);
  RemoveFromGhostList(to_remove);
  delete to_remove;
  ghost_cache_size_--;
}

uint64_t LRUCacheShard::EstimateStackDistanceFromBuckets(
    const Slice& key, uint32_t hash, uint64_t current_sequence) {
  mutex_.AssertHeld();
  
  uint64_t estimated_distance = 0;
  
  // Check if key is in ghost cache (recently evicted)
  if (LookupInGhostCache(key, hash, current_sequence, &estimated_distance)) {
    // Found in ghost cache - estimate based on sequence difference
    return estimated_distance;
  }
  
  // Not in ghost cache - estimate based on bucket sizes
  // Approach: Count items in ghost cache to estimate position
  // Stack distance ≈ (number of items in ghost cache) + (current cache size)
  estimated_distance = ghost_cache_size_ + usage_;
  
  return estimated_distance;
}

void LRUCacheShard::RecordAccessInBuckets(const Slice& key, uint32_t hash,
                                          uint64_t current_sequence, bool is_hit) {
  mutex_.AssertHeld();
  
  if (distance_buckets_.empty()) {
    return;
  }
  
  uint64_t stack_dist = EstimateStackDistanceFromBuckets(key, hash, current_sequence);
  
  // Find which bucket this belongs to
  size_t bucket_idx = distance_buckets_.size();  // Default to last bucket (overflow)
  for (size_t i = 0; i < distance_buckets_.size(); i++) {
    if (stack_dist < distance_buckets_[i]) {
      bucket_idx = i;
      break;
    }
  }
  
  if (is_hit) {
    // Cache hit - this cache size would have been a hit
    if (bucket_idx < bucket_hits_.size()) {
      bucket_hits_[bucket_idx]++;
    }
  } else {
    // Cache miss - determine which bucket sizes would have been hits/misses
    for (size_t i = 0; i < distance_buckets_.size(); i++) {
      if (stack_dist < distance_buckets_[i]) {
        // This cache size would have been a hit
        bucket_hits_[i]++;
      } else {
        // This cache size would have been a miss
        bucket_misses_[i]++;
      }
    }
  }
}

void LRUCacheShard::GetMissRateCurve(std::vector<uint64_t>* cache_sizes,
                                     std::vector<double>* miss_rates) const {
  MutexLock l(&mutex_);
  
  cache_sizes->clear();
  miss_rates->clear();
  
  if (!ghost_cache_enabled_ || distance_buckets_.empty()) {
    return;
  }
  
  for (size_t i = 0; i < distance_buckets_.size(); i++) {
    uint64_t total_accesses = bucket_hits_[i] + bucket_misses_[i];
    if (total_accesses > 0) {
      double miss_rate = static_cast<double>(bucket_misses_[i]) / total_accesses;
      cache_sizes->push_back(distance_buckets_[i]);
      miss_rates->push_back(miss_rate);
    }
  }
}

void LRUCacheShard::ResetMRCStats() {
  MutexLock l(&mutex_);
  std::fill(bucket_hits_.begin(), bucket_hits_.end(), 0);
  std::fill(bucket_misses_.begin(), bucket_misses_.end(), 0);
  evict_sequence_.store(0, std::memory_order_relaxed);
  access_sequence_.store(0, std::memory_order_relaxed);
}

LRUCacheShard::BucketStatistics LRUCacheShard::GetBucketStatistics() const {
  MutexLock l(&mutex_);
  BucketStatistics stats;
  
  if (!ghost_cache_enabled_ || distance_buckets_.empty()) {
    return stats;
  }
  
  for (size_t i = 0; i < distance_buckets_.size(); i++) {
    stats.cache_sizes.push_back(distance_buckets_[i]);
    stats.hits.push_back(bucket_hits_[i]);
    stats.misses.push_back(bucket_misses_[i]);
  }
  
  return stats;
}

}  // namespace ROCKSDB_NAMESPACE
