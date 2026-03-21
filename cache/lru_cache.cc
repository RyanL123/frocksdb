//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/lru_cache.h"

#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <string>

#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {
constexpr size_t kQuickMRCBlockSize = 4096;
}  // namespace

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
                             CacheMetadataChargePolicy metadata_charge_policy,
                             bool quick_mrc_enabled,
                             uint32_t quick_mrc_max_bucket_size,
                             uint32_t quick_mrc_ghost_cache_multiplier,
                             double quick_mrc_sampling_rate,
                             uint32_t quick_mrc_histogram_bin_size)
    : capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex),
      quick_mrc_enabled_(quick_mrc_enabled),
      quick_mrc_max_bucket_size_(quick_mrc_max_bucket_size),
      quick_mrc_ghost_cache_multiplier_(quick_mrc_ghost_cache_multiplier),
      quick_mrc_sampling_rate_(quick_mrc_sampling_rate),
      quick_mrc_sampling_denominator_(0),
      quick_mrc_histogram_bin_size_(quick_mrc_histogram_bin_size),
      quick_mrc_rng_state_(0x9e3779b97f4a7c15ULL),
      quick_mrc_next_bucket_id_(1),
      quick_mrc_resident_entries_(0),
      quick_mrc_resident_units_(0),
      quick_mrc_complete_miss_count_(0) {
  set_metadata_charge_policy(metadata_charge_policy);
  if (quick_mrc_sampling_rate_ >= 1.0) {
    quick_mrc_sampling_denominator_ = 1;
  } else if (quick_mrc_sampling_rate_ > 0.0) {
    quick_mrc_sampling_denominator_ =
        static_cast<uint32_t>(std::ceil(1.0 / quick_mrc_sampling_rate_));
    if (quick_mrc_sampling_denominator_ == 0) {
      quick_mrc_sampling_denominator_ = 1;
    }
  }
  // Mix cache/shard-specific parameters into the PRNG seed.
  quick_mrc_rng_state_ ^= static_cast<uint64_t>(capacity);
  quick_mrc_rng_state_ ^=
      static_cast<uint64_t>(quick_mrc_max_bucket_size_) << 32;
  quick_mrc_rng_state_ ^=
      static_cast<uint64_t>(quick_mrc_ghost_cache_multiplier_) << 1;
  if (quick_mrc_rng_state_ == 0) {
    quick_mrc_rng_state_ = 1;
  }
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
  SetCapacity(capacity);
}

void LRUCacheShard::QuickMRCEnsureFrontBucket(
    std::deque<QuickMRCBucket>* buckets) {
  if (buckets->empty() || buckets->front().size >= quick_mrc_max_bucket_size_) {
    QuickMRCBucket bucket;
    bucket.id = quick_mrc_next_bucket_id_++;
    bucket.size = 0;
    buckets->push_front(bucket);
  }
}

size_t LRUCacheShard::QuickMRCEstimateDistance(
    uint64_t bucket_id, const std::deque<QuickMRCBucket>& buckets,
    bool* found) const {
  size_t distance = 0;
  *found = false;
  for (const auto& bucket : buckets) {
    distance += bucket.size;
    if (bucket.id == bucket_id) {
      *found = true;
      break;
    }
  }
  return distance;
}

void LRUCacheShard::QuickMRCRecordDistance(size_t stack_distance,
                                           uint64_t weight) {
  if (quick_mrc_histogram_bin_size_ == 0) {
    return;
  }
  const size_t bin = stack_distance / quick_mrc_histogram_bin_size_;
  if (quick_mrc_histogram_.size() <= bin) {
    quick_mrc_histogram_.resize(bin + 1, 0);
  }
  quick_mrc_histogram_[bin] += weight;
}

size_t LRUCacheShard::QuickMRCChargeUnits(size_t charge) {
  if (charge == 0) {
    return 0;
  }
  return (charge + kQuickMRCBlockSize - 1) / kQuickMRCBlockSize;
}

bool LRUCacheShard::QuickMRCShouldSample() {
  if (!quick_mrc_enabled_ || quick_mrc_sampling_denominator_ == 0) {
    return false;
  }
  // xorshift64*; cheap per-access pseudo-randomness under shard mutex.
  quick_mrc_rng_state_ ^= quick_mrc_rng_state_ >> 12;
  quick_mrc_rng_state_ ^= quick_mrc_rng_state_ << 25;
  quick_mrc_rng_state_ ^= quick_mrc_rng_state_ >> 27;
  const uint64_t random_value = quick_mrc_rng_state_ * 2685821657736338717ULL;
  return (random_value % quick_mrc_sampling_denominator_) == 0;
}

void LRUCacheShard::QuickMRCRemoveCacheHandleFromBucket(LRUHandle* e) {
  if (!quick_mrc_enabled_ || !e->quick_mrc_in_bucket) {
    return;
  }
  for (auto it = quick_mrc_cache_buckets_.begin();
       it != quick_mrc_cache_buckets_.end(); ++it) {
    if (it->id == e->quick_mrc_bucket_id) {
      assert(it->size >= e->quick_mrc_charge_units);
      it->size -= e->quick_mrc_charge_units;
      if (it->size == 0) {
        quick_mrc_cache_buckets_.erase(it);
      }
      break;
    }
  }
  e->quick_mrc_in_bucket = false;
}

void LRUCacheShard::QuickMRCTouchCacheHandle(LRUHandle* e) {
  if (!quick_mrc_enabled_) {
    return;
  }
  e->quick_mrc_charge_units = QuickMRCChargeUnits(e->charge);
  QuickMRCRemoveCacheHandleFromBucket(e);
  QuickMRCEnsureFrontBucket(&quick_mrc_cache_buckets_);
  e->quick_mrc_bucket_id = quick_mrc_cache_buckets_.front().id;
  e->quick_mrc_in_bucket = true;
  quick_mrc_cache_buckets_.front().size += e->quick_mrc_charge_units;
}

void LRUCacheShard::QuickMRCInsertGhost(const Slice& key, size_t charge) {
  if (!quick_mrc_enabled_ || quick_mrc_ghost_cache_multiplier_ == 0) {
    return;
  }
  const size_t charge_units = QuickMRCChargeUnits(charge);

  const std::string key_str = key.ToString();
  auto existing = quick_mrc_ghost_index_.find(key_str);
  if (existing != quick_mrc_ghost_index_.end()) {
    for (auto it = quick_mrc_ghost_buckets_.begin();
         it != quick_mrc_ghost_buckets_.end(); ++it) {
      if (it->id == existing->second.bucket_id) {
        assert(it->size >= existing->second.charge_units);
        it->size -= existing->second.charge_units;
        if (it->size == 0) {
          quick_mrc_ghost_buckets_.erase(it);
        }
        break;
      }
    }
    quick_mrc_ghost_lru_.erase(existing->second.lru_iter);
    quick_mrc_ghost_index_.erase(existing);
  }

  QuickMRCEnsureFrontBucket(&quick_mrc_ghost_buckets_);
  quick_mrc_ghost_buckets_.front().size += charge_units;
  quick_mrc_ghost_lru_.push_front(key_str);
  QuickMRCGhostEntry ghost_entry;
  ghost_entry.bucket_id = quick_mrc_ghost_buckets_.front().id;
  ghost_entry.charge_units = charge_units;
  ghost_entry.lru_iter = quick_mrc_ghost_lru_.begin();
  quick_mrc_ghost_index_[key_str] = ghost_entry;
  QuickMRCEnforceGhostCapacity();
}

bool LRUCacheShard::QuickMRCProbeGhost(const Slice& key) {
  if (!quick_mrc_enabled_ || quick_mrc_ghost_cache_multiplier_ == 0) {
    return false;
  }
  auto it = quick_mrc_ghost_index_.find(key.ToString());
  if (it == quick_mrc_ghost_index_.end()) {
    return false;
  }
  // Snapshot the ghost entry metadata before mutating/erasing the map entry.
  const uint64_t bucket_id = it->second.bucket_id;
  const size_t charge_units = it->second.charge_units;
  const auto lru_iter = it->second.lru_iter;

  bool found = false;
  size_t ghost_distance =
      QuickMRCEstimateDistance(bucket_id, quick_mrc_ghost_buckets_, &found);
  if (found) {
    // Ghost cache observations are always recorded (not sampled).
    QuickMRCRecordDistance(quick_mrc_resident_units_ + ghost_distance);
  }

  for (auto bucket_it = quick_mrc_ghost_buckets_.begin();
       bucket_it != quick_mrc_ghost_buckets_.end(); ++bucket_it) {
    if (bucket_it->id == bucket_id) {
      assert(bucket_it->size >= charge_units);
      bucket_it->size -= charge_units;
      if (bucket_it->size == 0) {
        quick_mrc_ghost_buckets_.erase(bucket_it);
      }
      break;
    }
  }

  quick_mrc_ghost_lru_.erase(lru_iter);
  quick_mrc_ghost_index_.erase(it);

  QuickMRCEnsureFrontBucket(&quick_mrc_ghost_buckets_);
  quick_mrc_ghost_buckets_.front().size += charge_units;
  quick_mrc_ghost_lru_.push_front(key.ToString());
  QuickMRCGhostEntry ghost_entry;
  ghost_entry.bucket_id = quick_mrc_ghost_buckets_.front().id;
  ghost_entry.charge_units = charge_units;
  ghost_entry.lru_iter = quick_mrc_ghost_lru_.begin();
  quick_mrc_ghost_index_[key.ToString()] = ghost_entry;
  return true;
}

void LRUCacheShard::QuickMRCEnforceGhostCapacity() {
  if (!quick_mrc_enabled_ || quick_mrc_ghost_cache_multiplier_ == 0) {
    return;
  }
  const size_t ghost_capacity =
      quick_mrc_resident_entries_ * quick_mrc_ghost_cache_multiplier_;
  while (quick_mrc_ghost_index_.size() > ghost_capacity &&
         !quick_mrc_ghost_lru_.empty()) {
    const std::string& key = quick_mrc_ghost_lru_.back();
    auto it = quick_mrc_ghost_index_.find(key);
    if (it != quick_mrc_ghost_index_.end()) {
      for (auto bucket_it = quick_mrc_ghost_buckets_.begin();
           bucket_it != quick_mrc_ghost_buckets_.end(); ++bucket_it) {
        if (bucket_it->id == it->second.bucket_id) {
          assert(bucket_it->size >= it->second.charge_units);
          bucket_it->size -= it->second.charge_units;
          if (bucket_it->size == 0) {
            quick_mrc_ghost_buckets_.erase(bucket_it);
          }
          break;
        }
      }
      quick_mrc_ghost_index_.erase(it);
    }
    quick_mrc_ghost_lru_.pop_back();
  }
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
      if (quick_mrc_enabled_) {
        QuickMRCRemoveCacheHandleFromBucket(old);
        if (quick_mrc_resident_entries_ > 0) {
          quick_mrc_resident_entries_--;
        }
        if (quick_mrc_resident_units_ >= old->quick_mrc_charge_units) {
          quick_mrc_resident_units_ -= old->quick_mrc_charge_units;
        }
        QuickMRCEnforceGhostCapacity();
      }
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
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    if (quick_mrc_enabled_) {
      QuickMRCRemoveCacheHandleFromBucket(old);
      if (quick_mrc_resident_entries_ > 0) {
        quick_mrc_resident_entries_--;
      }
      if (quick_mrc_resident_units_ >= old->quick_mrc_charge_units) {
        quick_mrc_resident_units_ -= old->quick_mrc_charge_units;
      }
      QuickMRCInsertGhost(old->key(), old->charge);
    }
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
  if (e != nullptr) {
    if (quick_mrc_enabled_ && QuickMRCShouldSample()) {
      bool found = false;
      size_t stack_distance = QuickMRCEstimateDistance(
          e->quick_mrc_bucket_id, quick_mrc_cache_buckets_, &found);
      if (found) {
        // Scale sampled LRU observations by K; stack distance is unchanged.
        const uint64_t sample_weight =
            (quick_mrc_sampling_denominator_ == 0)
                ? 1
                : static_cast<uint64_t>(quick_mrc_sampling_denominator_);
        QuickMRCRecordDistance(stack_distance, sample_weight);
        // Emit sampled charge immediately for external log collection.
        fprintf(stderr,
                "quick_mrc_sampled_charge: charge=%" ROCKSDB_PRIszt
                " sample_weight=%" PRIu64 " weighted_charge=%" PRIu64 "\n",
                e->charge, sample_weight,
                static_cast<uint64_t>(e->charge) * sample_weight);
      }
    }
    if (quick_mrc_enabled_) {
      QuickMRCTouchCacheHandle(e);
    }
    assert(e->InCache());
    if (!e->HasRefs()) {
      // The entry is in LRU since it's in hash and has no external references
      LRU_Remove(e);
    }
    e->Ref();
    e->SetHit();
  } else if (quick_mrc_enabled_) {
    if (!QuickMRCProbeGhost(key)) {
      quick_mrc_complete_miss_count_++;
    }
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
        if (quick_mrc_enabled_) {
          QuickMRCRemoveCacheHandleFromBucket(e);
          if (quick_mrc_resident_entries_ > 0) {
            quick_mrc_resident_entries_--;
          }
          if (quick_mrc_resident_units_ >= e->quick_mrc_charge_units) {
            quick_mrc_resident_units_ -= e->quick_mrc_charge_units;
          }
          QuickMRCInsertGhost(e->key(), e->charge);
        }
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
      if (quick_mrc_enabled_) {
        quick_mrc_resident_entries_++;
        quick_mrc_resident_units_ += QuickMRCChargeUnits(e->charge);
        QuickMRCTouchCacheHandle(e);
      }
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (quick_mrc_enabled_) {
          QuickMRCRemoveCacheHandleFromBucket(old);
          if (quick_mrc_resident_entries_ > 0) {
            quick_mrc_resident_entries_--;
          }
          if (quick_mrc_resident_units_ >= old->quick_mrc_charge_units) {
            quick_mrc_resident_units_ -= old->quick_mrc_charge_units;
          }
          QuickMRCInsertGhost(old->key(), old->charge);
        }
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
      if (quick_mrc_enabled_) {
        QuickMRCRemoveCacheHandleFromBucket(e);
        if (quick_mrc_resident_entries_ > 0) {
          quick_mrc_resident_entries_--;
        }
        if (quick_mrc_resident_units_ >= e->quick_mrc_charge_units) {
          quick_mrc_resident_units_ -= e->quick_mrc_charge_units;
        }
        QuickMRCInsertGhost(e->key(), e->charge);
      }
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
  const int kBufferSize = 512;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize,
             "    high_pri_pool_ratio: %.3lf\n"
             "    quick_mrc_enabled: %d\n"
             "    quick_mrc_max_bucket_size: %u\n"
             "    quick_mrc_ghost_cache_multiplier: %u\n"
             "    quick_mrc_sampling_rate: %.6lf\n"
             "    quick_mrc_histogram_bin_size: %u\n",
             high_pri_pool_ratio_, static_cast<int>(quick_mrc_enabled_),
             quick_mrc_max_bucket_size_, quick_mrc_ghost_cache_multiplier_,
             quick_mrc_sampling_rate_, quick_mrc_histogram_bin_size_);
  }
  return std::string(buffer);
}

std::vector<uint64_t> LRUCacheShard::GetQuickMRCStackDistanceHistogram() const {
  MutexLock l(&mutex_);
  return quick_mrc_histogram_;
}

uint64_t LRUCacheShard::GetQuickMRCCompleteMissCount() const {
  MutexLock l(&mutex_);
  return quick_mrc_complete_miss_count_;
}

void LRUCacheShard::ResetQuickMRCStats() {
  MutexLock l(&mutex_);
  quick_mrc_histogram_.clear();
  quick_mrc_complete_miss_count_ = 0;
  quick_mrc_cache_buckets_.clear();
  quick_mrc_ghost_buckets_.clear();
  quick_mrc_ghost_index_.clear();
  quick_mrc_ghost_lru_.clear();
  quick_mrc_resident_units_ = 0;
  quick_mrc_resident_entries_ = 0;
  quick_mrc_next_bucket_id_ = 1;
  table_.ApplyToAllCacheEntries([this](LRUHandle* h) {
    h->quick_mrc_in_bucket = false;
    h->quick_mrc_bucket_id = 0;
    h->quick_mrc_charge_units = 0;
    if (h->InCache()) {
      quick_mrc_resident_entries_++;
      quick_mrc_resident_units_ += QuickMRCChargeUnits(h->charge);
      QuickMRCTouchCacheHandle(h);
    }
  });
}

LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy,
                   bool quick_mrc_enabled, uint32_t quick_mrc_max_bucket_size,
                   uint32_t quick_mrc_ghost_cache_multiplier,
                   double quick_mrc_sampling_rate,
                   uint32_t quick_mrc_histogram_bin_size)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(LRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LRUCacheShard(per_shard, strict_capacity_limit, high_pri_pool_ratio,
                      use_adaptive_mutex, metadata_charge_policy,
                      quick_mrc_enabled, quick_mrc_max_bucket_size,
                      quick_mrc_ghost_cache_multiplier, quick_mrc_sampling_rate,
                      quick_mrc_histogram_bin_size);
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

std::vector<uint64_t> LRUCache::GetQuickMRCStackDistanceHistogram() const {
  std::vector<uint64_t> merged;
  uint64_t complete_miss_count = 0;
  for (int i = 0; i < num_shards_; i++) {
    std::vector<uint64_t> shard_hist =
        shards_[i].GetQuickMRCStackDistanceHistogram();
    complete_miss_count += shards_[i].GetQuickMRCCompleteMissCount();
    if (merged.size() < shard_hist.size()) {
      merged.resize(shard_hist.size(), 0);
    }
    for (size_t j = 0; j < shard_hist.size(); j++) {
      merged[j] += shard_hist[j];
    }
  }
  merged.push_back(complete_miss_count);
  return merged;
}

void LRUCache::ResetQuickMRCStats() {
  for (int i = 0; i < num_shards_; i++) {
    shards_[i].ResetQuickMRCStats();
  }
}

std::shared_ptr<Cache> NewLRUCache(const LRUCacheOptions& cache_opts) {
  return NewLRUCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
                     cache_opts.metadata_charge_policy,
                     cache_opts.quick_mrc_enabled,
                     cache_opts.quick_mrc_max_bucket_size,
                     cache_opts.quick_mrc_ghost_cache_multiplier,
                     cache_opts.quick_mrc_sampling_rate,
                     cache_opts.quick_mrc_histogram_bin_size);
}

std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy, bool quick_mrc_enabled,
    uint32_t quick_mrc_max_bucket_size,
    uint32_t quick_mrc_ghost_cache_multiplier, double quick_mrc_sampling_rate,
    uint32_t quick_mrc_histogram_bin_size) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (quick_mrc_max_bucket_size == 0 ||
      quick_mrc_ghost_cache_multiplier == 0 ||
      quick_mrc_histogram_bin_size == 0 || quick_mrc_sampling_rate < 0.0 ||
      quick_mrc_sampling_rate > 1.0) {
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      std::move(memory_allocator), use_adaptive_mutex, metadata_charge_policy,
      quick_mrc_enabled, quick_mrc_max_bucket_size,
      quick_mrc_ghost_cache_multiplier, quick_mrc_sampling_rate,
      quick_mrc_histogram_bin_size);
}

}  // namespace ROCKSDB_NAMESPACE
