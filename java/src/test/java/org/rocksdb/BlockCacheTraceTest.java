// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for block cache tracing functionality
 */
public class BlockCacheTraceTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void startBlockCacheTrace() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))) {
      
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final TraceOptions traceOptions = new TraceOptions();

        try (final InMemoryTraceWriter traceWriter = new InMemoryTraceWriter()) {
          // Start block cache tracing
          db.startBlockCacheTrace(traceOptions, traceWriter);

          // Perform operations that will trigger block cache activity
          // Write some data
          db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
          db.put("key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
          db.put("key3".getBytes(UTF_8), "value3".getBytes(UTF_8));
          
          // Flush to create SST files (which will be cached in block cache)
          db.flush(new FlushOptions());
          
          // Read data to trigger block cache hits/misses
          final byte[] value1 = db.get("key1".getBytes(UTF_8));
          final byte[] value2 = db.get("key2".getBytes(UTF_8));
          final byte[] value3 = db.get("key3".getBytes(UTF_8));
          
          // Verify reads succeeded
          assertThat(value1).isEqualTo("value1".getBytes(UTF_8));
          assertThat(value2).isEqualTo("value2".getBytes(UTF_8));
          assertThat(value3).isEqualTo("value3".getBytes(UTF_8));
          
          // Stop block cache tracing
          db.endBlockCacheTrace();

          // Verify that trace data was captured
          final List<byte[]> writes = traceWriter.getWrites();
          assertThat(writes.size()).isGreaterThan(0);
          assertThat(traceWriter.getFileSize()).isGreaterThan(0);
        }
      }
    }
  }

  @Test
  public void endBlockCacheTrace() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))) {
      
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final TraceOptions traceOptions = new TraceOptions();

        try (final InMemoryTraceWriter traceWriter = new InMemoryTraceWriter()) {
          // Start block cache tracing
          db.startBlockCacheTrace(traceOptions, traceWriter);

          // Perform some operations
          db.put("test_key".getBytes(UTF_8), "test_value".getBytes(UTF_8));
          db.flush(new FlushOptions());
          db.get("test_key".getBytes(UTF_8));

          // End tracing - should not throw exception
          db.endBlockCacheTrace();
          
          // Verify trace was captured
          assertThat(traceWriter.getWrites().size()).isGreaterThan(0);
        }
      }
    }
  }

  @Test
  public void blockCacheTraceWithMultipleOperations() throws RocksDBException {
    try (final Cache cache = new LRUCache(16 * 1024 * 1024);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))) {
      
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final TraceOptions traceOptions = new TraceOptions(64 * 1024 * 1024); // 64MB max

        try (final InMemoryTraceWriter traceWriter = new InMemoryTraceWriter()) {
          // Start tracing
          db.startBlockCacheTrace(traceOptions, traceWriter);

          // Write multiple keys to generate more block cache activity
          for (int i = 0; i < 100; i++) {
            final String key = "key" + i;
            final String value = "value" + i;
            db.put(key.getBytes(UTF_8), value.getBytes(UTF_8));
          }

          // Flush to create SST files
          db.flush(new FlushOptions());

          // Read all keys to trigger block cache operations
          for (int i = 0; i < 100; i++) {
            final String key = "key" + i;
            final byte[] value = db.get(key.getBytes(UTF_8));
            assertThat(value).isEqualTo(("value" + i).getBytes(UTF_8));
          }

          // End tracing
          db.endBlockCacheTrace();

          // Verify substantial trace data was captured
          final List<byte[]> writes = traceWriter.getWrites();
          assertThat(writes.size()).isGreaterThan(0);
          assertThat(traceWriter.getFileSize()).isGreaterThan(0);
        }
      }
    }
  }

  /**
   * In-memory trace writer for testing purposes
   */
  private static class InMemoryTraceWriter extends AbstractTraceWriter {
    private final List<byte[]> writes = new ArrayList<>();
    private volatile boolean closed = false;

    @Override
    public void write(final Slice slice) {
      if (closed) {
        return;
      }
      final byte[] data = slice.data();
      final byte[] dataCopy = new byte[data.length];
      System.arraycopy(data, 0, dataCopy, 0, data.length);
      writes.add(dataCopy);
    }

    @Override
    public void closeWriter() {
      closed = true;
    }

    @Override
    public long getFileSize() {
      long size = 0;
      for (int i = 0; i < writes.size(); i++) {
        size += writes.get(i).length;
      }
      return size;
    }

    public List<byte[]> getWrites() {
      return writes;
    }
  }
}
