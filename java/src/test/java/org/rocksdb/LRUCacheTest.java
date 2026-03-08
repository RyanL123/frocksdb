// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

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
  public void newLRUCacheWithQuickMRC() {
    final long capacity = 80000000;
    final int numShardBits = 4;
    final boolean strictCapacityLimit = false;
    final double highPriPoolRatio = 0.0;
    final boolean quickMrcEnabled = true;
    final int quickMrcMaxBucketSize = 60;
    final int quickMrcGhostCacheMultiplier = 1;
    final double quickMrcSamplingRate = 0.01;
    final int quickMrcHistogramBinSize = 1024;
    try (final LRUCache lruCache = new LRUCache(capacity, numShardBits,
        strictCapacityLimit, highPriPoolRatio, quickMrcEnabled,
        quickMrcMaxBucketSize, quickMrcGhostCacheMultiplier,
        quickMrcSamplingRate, quickMrcHistogramBinSize)) {
      final long[] histogram = lruCache.getStackDistanceHistogram();
      assertThat(histogram).isNotNull();
      lruCache.resetQuickMRCStats();
      assertThat(lruCache.getStackDistanceHistogram()).isNotNull();
    }
  }
}
