// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Bucket statistics for miss-rate curve generation.
 * Used for merging statistics from multiple tasks in distributed systems
 * (section 3.4.2 - Generating Miss-Rate Curve from merged stack distances).
 */
public class BucketStatistics {
  private final long[] cacheSizes;
  private final long[] hits;
  private final long[] misses;

  /**
   * Create bucket statistics.
   *
   * @param cacheSizes Array of cache size boundaries (in bytes) for each bucket
   * @param hits Array of hit counts for each bucket
   * @param misses Array of miss counts for each bucket
   */
  public BucketStatistics(final long[] cacheSizes, final long[] hits, final long[] misses) {
    this.cacheSizes = cacheSizes;
    this.hits = hits;
    this.misses = misses;
  }

  /**
   * Get the cache size boundaries for each bucket.
   *
   * @return Array of cache sizes in bytes
   */
  public long[] getCacheSizes() {
    return cacheSizes;
  }

  /**
   * Get the hit counts for each bucket.
   *
   * @return Array of hit counts
   */
  public long[] getHits() {
    return hits;
  }

  /**
   * Get the miss counts for each bucket.
   *
   * @return Array of miss counts
   */
  public long[] getMisses() {
    return misses;
  }

  /**
   * Get the number of buckets.
   *
   * @return Number of buckets
   */
  public int size() {
    return cacheSizes != null ? cacheSizes.length : 0;
  }

  /**
   * Calculate miss rate for a specific bucket.
   *
   * @param bucketIndex The index of the bucket
   * @return Miss rate (0.0 to 1.0), or 1.0 if no accesses
   */
  public double getMissRate(final int bucketIndex) {
    if (bucketIndex < 0 || bucketIndex >= size()) {
      return 1.0;
    }
    long totalAccesses = hits[bucketIndex] + misses[bucketIndex];
    if (totalAccesses == 0) {
      return 1.0;
    }
    return (double) misses[bucketIndex] / totalAccesses;
  }

  /**
   * Calculate hit rate for a specific bucket.
   *
   * @param bucketIndex The index of the bucket
   * @return Hit rate (0.0 to 1.0), or 0.0 if no accesses
   */
  public double getHitRate(final int bucketIndex) {
    return 1.0 - getMissRate(bucketIndex);
  }
}
