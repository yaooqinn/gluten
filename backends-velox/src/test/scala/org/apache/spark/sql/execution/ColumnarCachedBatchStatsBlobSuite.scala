/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.scalatest.funsuite.AnyFunSuite

import java.nio.{ByteBuffer, ByteOrder}

/**
 * PA-3.2 RED tests for statsBlob binary framing (replace PA-1 Java Serialization placeholder with
 * cpp-aligned LE marshal).
 *
 * Refs:
 *   - todos/features/gluten-inmemory-cache-stats/docs/0002-cpp-stats-contract.md rev 4 sec 3
 *     (statsBlob layout)
 *   - todos/features/gluten-inmemory-cache-stats/docs/0003-jni-binary-framing-reference.md rev 4
 *     sec 3 + sec 4 (per-type marshal)
 *   - cpp/velox/operators/serializer/VeloxColumnarBatchSerializer.cc PA-2.5b (cpp end of the wire)
 *
 * Wire format (LE throughout, BIGINT 1-col scope per PA-3.2):
 *
 * [ numCols: uint32 LE ] per col: [ supported: uint8 ] [ nullCount: uint32 LE ] [ count: uint32 LE
 * ] [ sizeInBytes: uint64 LE ] if supported: [ lowerBoundLen: uint32 LE = 8 ] [ lowerBound: int64
 * LE ] [ upperBoundLen: uint32 LE = 8 ] [ upperBound: int64 LE ]
 */
class ColumnarCachedBatchStatsBlobSuite extends AnyFunSuite {

  // PA-3.2.A RED: statsBlob produced by serializeStats matches the cpp-aligned
  // binary layout byte-for-byte. Pre-PA-3.2, serializeStats wrote Java
  // Serialization output (gibberish to a binary parser).
  //
  // Expected RED failure: the first few bytes of serializeStats output will be
  // the Java Serialization magic [0xAC, 0xED, ...], not the LE numCols=1 we
  // expect. assert fails with a clear diff.
  test("PA-3.2.A statsBlob LE numCols + BIGINT cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](42L, 100L, 0, 10, 64L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)

    // Hand-compute expected wire:
    //   numCols=1 LE = 01 00 00 00
    //   supported=1 = 01
    //   nullCount=0 LE = 00 00 00 00
    //   count=10 LE = 0A 00 00 00
    //   sizeInBytes=64 LE = 40 00 00 00 00 00 00 00
    //   lowerBoundLen=8 LE = 08 00 00 00
    //   lowerBound=42 LE  = 2A 00 00 00 00 00 00 00
    //   upperBoundLen=8 LE = 08 00 00 00
    //   upperBound=100 LE = 64 00 00 00 00 00 00 00
    val expected = Array[Byte](
      0x01, 0x00, 0x00, 0x00,
      0x01,
      0x00, 0x00, 0x00, 0x00,
      0x0a, 0x00, 0x00, 0x00,
      0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x00, 0x00,
      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x00, 0x00,
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
    assert(
      blob === expected,
      "statsBlob must match cpp-aligned LE wire format byte-for-byte")
  }

  // PA-3.2.B RED: round-trip serializeStats -> deserializeStats yields an
  // InternalRow with 5 slots in vanilla order (lowerBound, upperBound,
  // nullCount, count, sizeInBytes).
  test("PA-3.2.B serializeStats then deserializeStats round-trip BIGINT 1-col") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](-7L, 999L, 3, 100, 1024L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)

    assert(read.numFields === 5)
    assert(read.getLong(0) === -7L, "lowerBound at slot 0")
    assert(read.getLong(1) === 999L, "upperBound at slot 1")
    assert(read.getInt(2) === 3, "nullCount at slot 2")
    assert(read.getInt(3) === 100, "count at slot 3")
    assert(read.getLong(4) === 1024L, "sizeInBytes at slot 4")
  }

  // PA-3.2.C RED: corrupt blob (numCols=huge) should fail eagerly with a clear
  // require error, NOT NPE / silent bad row. Guards against silent corruption
  // from a malformed cpp wire or truncated read.
  test("PA-3.2.C corrupt statsBlob (numCols out of range) fails eagerly") {
    // Craft a blob claiming numCols=Int.MaxValue: 0xFF FF FF 7F
    val corruptNumCols = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(Int.MaxValue).array()
    val ex = intercept[IllegalArgumentException] {
      CachedColumnarBatchKryoSerializer.deserializeStats(corruptNumCols, null)
    }
    assert(
      ex.getMessage.contains("numCols"),
      s"expected numCols range guard, got: ${ex.getMessage}")
  }

  // PA-3.2.D RED: unsupported col (supported=0) round-trips with null
  // lowerBound / upperBound and preserves nullCount / count / sizeInBytes.
  test("PA-3.2.D unsupported col round-trip preserves null bounds + metrics") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](null, null, 5, 50, 200L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)

    assert(read.isNullAt(0), "lowerBound must be null for unsupported col")
    assert(read.isNullAt(1), "upperBound must be null for unsupported col")
    assert(read.getInt(2) === 5)
    assert(read.getInt(3) === 50)
    assert(read.getLong(4) === 200L)
  }
}
