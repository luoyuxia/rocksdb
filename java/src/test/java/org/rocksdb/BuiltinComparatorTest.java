// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.TestUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class BuiltinComparatorTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();


  @Test
  public void t1() throws Exception {
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
//         final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
//              .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
//         DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)
         final RocksDB rocksDb = RocksDB.open(options,
                 dbFolder.getRoot().getAbsolutePath())
    ) {
//      List<ColumnFamilyDescriptor> columnFamilyDescriptors =
//      Arrays.asList(new ColumnFamilyDescriptor("name".getBytes(UTF_8), cfOptions),
//              new ColumnFamilyDescriptor("default".getBytes(UTF_8), cfOptions));
//      RocksDB rocksDb = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
//              Collections.singletonList(new ColumnFamilyDescriptor("name".getBytes(UTF_8), cfOptions)), columnFamilyHandles);
      ReadOptions readOptions  = new ReadOptions();
      readOptions.setTimestamp(new Slice("s"));
      assertThat(rocksDb.get(rocksDb.getDefaultColumnFamily(),
              readOptions, "abc1".getBytes())).isEqualTo("abc1".getBytes());
    }
  }

  @Test
  public void t4() throws Exception {
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    final RocksDB rocksDb = RocksDB.open(options,
            dbFolder.getRoot().getAbsolutePath());
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    ColumnFamilyHandle columnFamilyHandle =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));

    System.out.println(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes()));

    rocksDb.put(columnFamilyHandle,
            "abc1".getBytes(), "seeeeeee".getBytes(),  "abc1".getBytes());

    System.out.println(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes()));

    readOptions.setTimestamp(new Slice("aeeeeeee"));
    System.out.println(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes()));
  }

  @Test
  public void t4r() throws Exception {
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
    DBOptions dbOptions = new DBOptions(options);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));

    final RocksDB rocksDb = RocksDB.open(dbOptions,
            dbFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);

    ReadOptions readOptions = new ReadOptions();
    rocksDb.put(columnFamilyHandles.get(1), "av".getBytes(UTF_8), "vs".getBytes());
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "av".getBytes())));

    rocksDb.delete(columnFamilyHandles.get(1), "av".getBytes(UTF_8));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "av".getBytes())));
  }

  @Test
  public void t2() throws Exception {
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    DBOptions dbOptions = new DBOptions(options);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));

    final RocksDB rocksDb = RocksDB.open(dbOptions,
            dbFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("aeeeeeee"));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    rocksDb.put(columnFamilyHandles.get(1),
            "abc1".getBytes(), "beeeeeee".getBytes(),  "abc1".getBytes());

    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    readOptions.setTimestamp(new Slice("beeeeeee"));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    rocksDb.delete(columnFamilyHandles.get(1), "abc1".getBytes(), "ceeeeeee".getBytes());
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    readOptions.setTimestamp(new Slice("feeeeeee"));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    readOptions.setTimestamp(new Slice("aeeeeeee"));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandles.get(1),
            readOptions, "abc1".getBytes())));

    rocksDb.multiGetAsList(readOptions,null,null);
  }

  @Test
  public void tr() throws Exception {
    System.out.println(Arrays.toString("aeeeeeee".getBytes()));
    System.out.println(Arrays.toString("ceeeeeee".getBytes()));
  }

  @Test
  public void onceGet() throws Exception {
    int n = 100000;
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    DBOptions dbOptions = new DBOptions(options);

    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final RocksDB rocksDb = RocksDB.open(dbOptions,
            dbFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);

    byte[] timestamps = "seeeeeee".getBytes();
    for (int i = 0; i < n; i++) {
      rocksDb.put(rocksDb.getDefaultColumnFamily(),
              String.format("key%d", i).getBytes(),timestamps, String.format("val%d", i).getBytes());
    }

    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));

    long startTs = System.nanoTime();
    for (int i = 0; i < n; i++) {
      rocksDb.get(readOptions, String.format("key%d", i).getBytes());
    }
    long endTs = System.nanoTime();
    System.out.println(endTs - startTs);
  }

  @Test
  public void mulgetCompare() throws Exception {
    int n = 100000;
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts)
            .setWriteBufferSize(1);
    Statistics statistics = new Statistics();
    options.setStatistics(statistics);
    DBOptions dbOptions = new DBOptions(options);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);

    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final RocksDB rocksDb = RocksDB.open(dbOptions,
            dbFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);

    byte[] timestamps = "seeeeeee".getBytes();
    for (int i = 0; i < n; i++) {
      rocksDb.put(rocksDb.getDefaultColumnFamily(),
              String.format("key%d", i).getBytes(),timestamps,
              String.format("val%d", i).getBytes());
    }
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));

    List<ByteBuffer> keys = new ArrayList<>();
    List<ByteBuffer> values = new ArrayList<>();
    for (int i = 0; i < 512; i++) {
      byte[] keyBytes = String.format("key%d", i).getBytes();
      ByteBuffer key = ByteBuffer.allocateDirect(keyBytes.length).put(keyBytes);
      key.flip();
      keys.add(key);
      values.add(ByteBuffer.allocateDirect(24));
    }

    long start = System.nanoTime();
    rocksDb.multiGetByteBuffers(readOptions, keys, values);
    long end = System.nanoTime();
    System.out.println(end - start);
    System.out.println(rocksDb.getProperty("rocksdb.stats"));
  }

  @Test
  public void mulgetCompare1() throws Exception {
    int n = 100000;
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts)
            .setWriteBufferSize(1);
    Statistics statistics = new Statistics();
    options.setStatistics(statistics);
    DBOptions dbOptions = new DBOptions(options);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);

    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final RocksDB rocksDb = RocksDB.open(dbOptions,
            dbFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);

    byte[] timestamps = "seeeeeee".getBytes();
    for (int i = 0; i < n; i++) {
      rocksDb.put(rocksDb.getDefaultColumnFamily(),
              String.format("key%d", i).getBytes(),timestamps,
              String.format("val%d", i).getBytes());
    }
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));

    long start = System.nanoTime();
    for (int i = 0; i < 512; i++) {
      rocksDb.get(readOptions, String.format("key%d", i).getBytes());
    }
    long end = System.nanoTime();
    System.out.println(end - start);
    System.out.println(rocksDb.getProperty("rocksdb.stats"));
  }

  @Test
  public void mulget() throws Exception {
    int n = 100000;
    final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);

    final RocksDB rocksDb = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
    ColumnFamilyHandle columnFamilyHandle =
            rocksDb.createColumnFamily(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));

    byte[] timestamps = "seeeeeee".getBytes();
    for (int i = 0; i < n; i++) {
      rocksDb.put(columnFamilyHandle, String.format("key%d", i).getBytes(),timestamps,
              String.format("val%d", i).getBytes());
    }
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));

    List<ByteBuffer> keys = new ArrayList<>();
    List<ByteBuffer> values = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      byte[] keyBytes = String.format("key%d", i).getBytes();
      ByteBuffer key = ByteBuffer.allocateDirect(keyBytes.length).put(keyBytes);
      key.flip();
      keys.add(key);
      values.add(ByteBuffer.allocateDirect(24));
    }

    long startTs = System.nanoTime();

    rocksDb.multiGetByteBuffers(readOptions,
            Collections.singletonList(columnFamilyHandle), keys, values);
    long endTs = System.nanoTime();
    System.out.println(endTs - startTs);
  }

  @Test
  public void g() {
    ByteBuffer expected =
            ByteBuffer.allocateDirect(24).put(Arrays.copyOf("valueX".getBytes(), 4));
    expected.flip();
    System.out.println(Arrays.toString(TestUtil.bufferBytes(expected)));
  }

  @Test
  public void builtinForwardComparator()
      throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try(final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        // Seek for abc
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
      }
    }
  }

  @Test
  public void builtinReverseComparator()
      throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {

      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try (final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        // Will be invalid because abc is after abc1
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isFalse();
        // Will be abc3 because the next one after abc999
        // is abc3
        rocksIterator.seek("abc999".getBytes());
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
      }
    }
  }

  @Test
  public void builtinComparatorEnum(){
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(0);
    assertThat(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(1);
    assertThat(BuiltinComparator.values().length).isEqualTo(2);
    assertThat(BuiltinComparator.valueOf("BYTEWISE_COMPARATOR")).
        isEqualTo(BuiltinComparator.BYTEWISE_COMPARATOR);
  }
}
