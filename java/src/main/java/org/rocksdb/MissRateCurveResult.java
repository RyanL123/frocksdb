// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Result object containing miss-rate curve data.
 */
public class MissRateCurveResult {
  private final long[] cacheSizes;
  private final double[] missRates;
  
  /**
   * Create a miss-rate curve result.
   *
   * @param cacheSizes Array of cache sizes in bytes
   * @param missRates Array of miss rates (0.0 to 1.0) corresponding to each cache size
   */
  public MissRateCurveResult(final long[] cacheSizes, final double[] missRates) {
    this.cacheSizes = cacheSizes;
    this.missRates = missRates;
  }
  
  /**
   * Get the cache sizes.
   *
   * @return Array of cache sizes in bytes
   */
  public long[] getCacheSizes() {
    return cacheSizes;
  }
  
  /**
   * Get the miss rates.
   *
   * @return Array of miss rates (0.0 to 1.0)
   */
  public double[] getMissRates() {
    return missRates;
  }
  
  /**
   * Get the number of data points in the curve.
   *
   * @return Number of cache size / miss rate pairs
   */
  public int size() {
    return cacheSizes != null ? cacheSizes.length : 0;
  }
}
