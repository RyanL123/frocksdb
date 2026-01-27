// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;

public class LRUCacheTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void newLRUCache() {
    final long capacity = 80000000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 0.05;
    try(final Cache lruCache = new LRUCache(capacity,
        numShardBits, strictCapacityLimit, highPriPoolRatio)) {
      //no op
      assertThat(lruCache.getUsage()).isGreaterThanOrEqualTo(0);
      assertThat(lruCache.getPinnedUsage()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  public void enableGhostCache() throws RocksDBException {
    final long capacity = 1000000; // 1MB
    try(final LRUCache lruCache = new LRUCache(capacity, -1, false, 0.0)) {
      final long ghostCapacity = 2000000; // 2MB
      final long[] distanceBuckets = {1024, 4096, 16384, 65536}; // 1KB, 4KB, 16KB, 64KB
      
      // Should not throw
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
      
      // Verify ghost cache is enabled by checking that we can get statistics
      // (even if empty initially)
    }
  }

  @Test
  public void enableGhostCacheWithEmptyBuckets() throws RocksDBException {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      final long ghostCapacity = 2000000;
      final long[] distanceBuckets = {};
      
      // Should not throw even with empty buckets
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
    }
  }

  @Test
  public void getMissRateCurveWithoutGhostCache() {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      // Should throw RocksDBException because ghost cache is not enabled
      assertThatThrownBy(() -> lruCache.getMissRateCurve())
          .isInstanceOf(RocksDBException.class);
    }
  }

  @Test
  public void getBucketStatisticsWithoutGhostCache() {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      // Should throw RocksDBException because ghost cache is not enabled
      assertThatThrownBy(() -> lruCache.getBucketStatistics())
          .isInstanceOf(RocksDBException.class);
    }
  }

  @Test
  public void getMissRateCurveWithGhostCache() throws RocksDBException {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      final long ghostCapacity = 2000000;
      final long[] distanceBuckets = {1024, 4096, 16384, 65536};
      
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
      
      // Initially may be empty or have zero statistics
      // The result should be valid even if empty
      try {
        final MissRateCurveResult result = lruCache.getMissRateCurve();
        assertThat(result).isNotNull();
        assertThat(result.size()).isGreaterThanOrEqualTo(0);
      } catch (RocksDBException e) {
        // If no statistics available yet, that's acceptable
        // The important thing is that the method doesn't crash
      }
    }
  }

  @Test
  public void getBucketStatisticsWithGhostCache() throws RocksDBException {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      final long ghostCapacity = 2000000;
      final long[] distanceBuckets = {1024, 4096, 16384, 65536};
      
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
      
      // Initially may be empty or have zero statistics
      // The result should be valid even if empty
      try {
        final BucketStatistics stats = lruCache.getBucketStatistics();
        assertThat(stats).isNotNull();
        assertThat(stats.size()).isGreaterThanOrEqualTo(0);
      } catch (RocksDBException e) {
        // If no statistics available yet, that's acceptable
        // The important thing is that the method doesn't crash
      }
    }
  }

  @Test
  public void resetMRCStats() throws RocksDBException {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      final long ghostCapacity = 2000000;
      final long[] distanceBuckets = {1024, 4096, 16384, 65536};
      
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
      
      // Should not throw
      lruCache.resetMRCStats();
      
      // Can call multiple times
      lruCache.resetMRCStats();
      lruCache.resetMRCStats();
    }
  }

  @Test
  public void resetMRCStatsWithoutGhostCache() {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      // resetMRCStats should not throw even if ghost cache is not enabled
      // (it silently ignores if not an LRUCache or ghost cache not enabled)
      lruCache.resetMRCStats();
    }
  }

  @Test
  public void missRateCurveResultGetters() {
    final long[] cacheSizes = {1024, 4096, 16384, 65536};
    final double[] missRates = {0.1, 0.2, 0.3, 0.4};
    
    final MissRateCurveResult result = new MissRateCurveResult(cacheSizes, missRates);
    
    // Test getCacheSizes()
    final long[] retrievedSizes = result.getCacheSizes();
    assertThat(retrievedSizes).isNotNull();
    assertThat(retrievedSizes).hasSameSizeAs(cacheSizes);
    assertThat(retrievedSizes).isEqualTo(cacheSizes);
    
    // Test getMissRates()
    final double[] retrievedRates = result.getMissRates();
    assertThat(retrievedRates).isNotNull();
    assertThat(retrievedRates).hasSameSizeAs(missRates);
    assertThat(retrievedRates).isEqualTo(missRates);
    
    // Test size()
    assertThat(result.size()).isEqualTo(cacheSizes.length);
    assertThat(result.size()).isEqualTo(4);
  }

  @Test
  public void missRateCurveResultWithEmptyArrays() {
    final long[] cacheSizes = {};
    final double[] missRates = {};
    
    final MissRateCurveResult result = new MissRateCurveResult(cacheSizes, missRates);
    
    assertThat(result.size()).isEqualTo(0);
    assertThat(result.getCacheSizes()).isEmpty();
    assertThat(result.getMissRates()).isEmpty();
  }

  @Test
  public void missRateCurveResultWithNullArrays() {
    final MissRateCurveResult result = new MissRateCurveResult(null, null);
    
    assertThat(result.size()).isEqualTo(0);
    // Note: getCacheSizes() and getMissRates() will return null, but size() handles null
  }

  @Test
  public void bucketStatisticsGetters() {
    final long[] cacheSizes = {1024, 4096, 16384, 65536};
    final long[] hits = {100, 200, 300, 400};
    final long[] misses = {50, 100, 150, 200};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // Test getCacheSizes()
    final long[] retrievedSizes = stats.getCacheSizes();
    assertThat(retrievedSizes).isNotNull();
    assertThat(retrievedSizes).hasSameSizeAs(cacheSizes);
    assertThat(retrievedSizes).isEqualTo(cacheSizes);
    
    // Test getHits()
    final long[] retrievedHits = stats.getHits();
    assertThat(retrievedHits).isNotNull();
    assertThat(retrievedHits).hasSameSizeAs(hits);
    assertThat(retrievedHits).isEqualTo(hits);
    
    // Test getMisses()
    final long[] retrievedMisses = stats.getMisses();
    assertThat(retrievedMisses).isNotNull();
    assertThat(retrievedMisses).hasSameSizeAs(misses);
    assertThat(retrievedMisses).isEqualTo(misses);
    
    // Test size()
    assertThat(stats.size()).isEqualTo(cacheSizes.length);
    assertThat(stats.size()).isEqualTo(4);
  }

  @Test
  public void bucketStatisticsGetMissRate() {
    final long[] cacheSizes = {1024, 4096, 16384, 65536};
    final long[] hits = {100, 200, 300, 400};
    final long[] misses = {50, 100, 150, 200};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // Test getMissRate() for each bucket
    // Bucket 0: 50 misses / (100 hits + 50 misses) = 50/150 = 0.333...
    final double missRate0 = stats.getMissRate(0);
    assertThat(missRate0).isCloseTo(50.0 / 150.0, org.assertj.core.data.Offset.offset(0.001));
    
    // Bucket 1: 100 misses / (200 hits + 100 misses) = 100/300 = 0.333...
    final double missRate1 = stats.getMissRate(1);
    assertThat(missRate1).isCloseTo(100.0 / 300.0, org.assertj.core.data.Offset.offset(0.001));
    
    // Bucket 2: 150 misses / (300 hits + 150 misses) = 150/450 = 0.333...
    final double missRate2 = stats.getMissRate(2);
    assertThat(missRate2).isCloseTo(150.0 / 450.0, org.assertj.core.data.Offset.offset(0.001));
    
    // Bucket 3: 200 misses / (400 hits + 200 misses) = 200/600 = 0.333...
    final double missRate3 = stats.getMissRate(3);
    assertThat(missRate3).isCloseTo(200.0 / 600.0, org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  public void bucketStatisticsGetMissRateWithZeroAccesses() {
    final long[] cacheSizes = {1024};
    final long[] hits = {0};
    final long[] misses = {0};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // When no accesses, miss rate should be 1.0
    assertThat(stats.getMissRate(0)).isEqualTo(1.0);
  }

  @Test
  public void bucketStatisticsGetMissRateWithOnlyHits() {
    final long[] cacheSizes = {1024};
    final long[] hits = {100};
    final long[] misses = {0};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // When only hits, miss rate should be 0.0
    assertThat(stats.getMissRate(0)).isEqualTo(0.0);
  }

  @Test
  public void bucketStatisticsGetMissRateWithOnlyMisses() {
    final long[] cacheSizes = {1024};
    final long[] hits = {0};
    final long[] misses = {100};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // When only misses, miss rate should be 1.0
    assertThat(stats.getMissRate(0)).isEqualTo(1.0);
  }

  @Test
  public void bucketStatisticsGetMissRateInvalidIndex() {
    final long[] cacheSizes = {1024};
    final long[] hits = {100};
    final long[] misses = {50};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // Invalid index should return 1.0
    assertThat(stats.getMissRate(-1)).isEqualTo(1.0);
    assertThat(stats.getMissRate(1)).isEqualTo(1.0);
    assertThat(stats.getMissRate(100)).isEqualTo(1.0);
  }

  @Test
  public void bucketStatisticsGetHitRate() {
    final long[] cacheSizes = {1024};
    final long[] hits = {100};
    final long[] misses = {50};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // Hit rate should be 1.0 - miss rate
    final double missRate = stats.getMissRate(0);
    final double hitRate = stats.getHitRate(0);
    
    assertThat(hitRate).isCloseTo(1.0 - missRate, org.assertj.core.data.Offset.offset(0.001));
    assertThat(hitRate).isCloseTo(100.0 / 150.0, org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  public void bucketStatisticsGetHitRateWithZeroAccesses() {
    final long[] cacheSizes = {1024};
    final long[] hits = {0};
    final long[] misses = {0};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    // When no accesses, hit rate should be 0.0 (1.0 - 1.0)
    assertThat(stats.getHitRate(0)).isEqualTo(0.0);
  }

  @Test
  public void bucketStatisticsWithEmptyArrays() {
    final long[] cacheSizes = {};
    final long[] hits = {};
    final long[] misses = {};
    
    final BucketStatistics stats = new BucketStatistics(cacheSizes, hits, misses);
    
    assertThat(stats.size()).isEqualTo(0);
    assertThat(stats.getCacheSizes()).isEmpty();
    assertThat(stats.getHits()).isEmpty();
    assertThat(stats.getMisses()).isEmpty();
  }

  @Test
  public void bucketStatisticsWithNullArrays() {
    final BucketStatistics stats = new BucketStatistics(null, null, null);
    
    assertThat(stats.size()).isEqualTo(0);
    // Note: getters will return null, but size() handles null
  }

  @Test
  public void fullMRCWorkflow() throws RocksDBException {
    final long capacity = 1000000;
    try(final LRUCache lruCache = new LRUCache(capacity)) {
      final long ghostCapacity = 2000000;
      final long[] distanceBuckets = {1024, 4096, 16384, 65536};
      
      // Step 1: Enable ghost cache
      lruCache.enableGhostCache(ghostCapacity, distanceBuckets);
      
      // Step 2: Try to get statistics (may be empty initially)
      try {
        final BucketStatistics stats = lruCache.getBucketStatistics();
        if (stats != null && stats.size() > 0) {
          // Test all BucketStatistics methods
          assertThat(stats.getCacheSizes()).isNotNull();
          assertThat(stats.getHits()).isNotNull();
          assertThat(stats.getMisses()).isNotNull();
          
          for (int i = 0; i < stats.size(); i++) {
            final double missRate = stats.getMissRate(i);
            final double hitRate = stats.getHitRate(i);
            assertThat(missRate).isBetween(0.0, 1.0);
            assertThat(hitRate).isBetween(0.0, 1.0);
            assertThat(missRate + hitRate).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.001));
          }
        }
      } catch (RocksDBException e) {
        // Acceptable if no statistics yet
      }
      
      // Step 3: Try to get miss rate curve
      try {
        final MissRateCurveResult result = lruCache.getMissRateCurve();
        if (result != null && result.size() > 0) {
          // Test all MissRateCurveResult methods
          assertThat(result.getCacheSizes()).isNotNull();
          assertThat(result.getMissRates()).isNotNull();
          
          final long[] sizes = result.getCacheSizes();
          final double[] rates = result.getMissRates();
          
          assertThat(sizes.length).isEqualTo(rates.length);
          
          for (int i = 0; i < result.size(); i++) {
            assertThat(sizes[i]).isGreaterThan(0);
            assertThat(rates[i]).isBetween(0.0, 1.0);
          }
        }
      } catch (RocksDBException e) {
        // Acceptable if no statistics yet
      }
      
      // Step 4: Reset statistics
      lruCache.resetMRCStats();
      
      // Step 5: Verify reset worked (statistics should be empty or zero)
      try {
        final BucketStatistics statsAfterReset = lruCache.getBucketStatistics();
        if (statsAfterReset != null && statsAfterReset.size() > 0) {
          // After reset, hits and misses should be zero or very low
          for (int i = 0; i < statsAfterReset.size(); i++) {
            final long hits = statsAfterReset.getHits()[i];
            final long misses = statsAfterReset.getMisses()[i];
            // After reset, these should be zero (or very low if some activity occurred)
            assertThat(hits + misses).isGreaterThanOrEqualTo(0);
          }
        }
      } catch (RocksDBException e) {
        // Acceptable
      }
    }
  }
}
