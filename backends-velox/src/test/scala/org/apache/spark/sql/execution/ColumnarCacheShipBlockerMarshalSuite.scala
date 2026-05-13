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
import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.funsuite.AnyFunSuite

/**
 * PA-5 ship blocker acceptance tests for FUTURE non-BIGINT marshal paths.
 *
 * Layer A scope (this PR): BIGINT min/max only. cpp PA-2.5b emits supported=0 for Decimal / String
 * / Float / Double columns, so the JVM marshal layer never sees non-Long objects in lo/hi slots --
 * the null-sentinel path is covered by PA-3.2.D ("unsupported col round-trip preserves null bounds
 * + metrics").
 *
 * These ignored tests are kept as ACCEPTANCE CRITERIA for the follow-up PR that adds Decimal /
 * String marshal on the cpp side. When that PR lands, flip ignore -> test and they MUST pass before
 * merge -- otherwise silent corruption (wrong Decimal scale, wrong UTF-8 byte order, lost
 * BigInteger sign) ships.
 *
 * Refs: todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-5
 * todos/features/gluten-inmemory-cache-stats/backlog.md (phase-2 marshal)
 */
class ColumnarCacheShipBlockerMarshalSuite extends AnyFunSuite {

  // PA-5.C ship blocker (NB3): Decimal(10, 2) -- precision <= 18 uses Long
  // backing in Spark. If a future marshal naively casts to Long, scale is lost
  // and downstream prune compares wrong magnitudes.
  // Trigger: when cpp marshals Decimal min/max, this MUST pass.
  ignore("PA-5.C Decimal(10, 2) round-trip preserves value (follow-up PR acceptance)") {
    val lo = Decimal(BigDecimal("1.50"), 10, 2)
    val hi = Decimal(BigDecimal("99.99"), 10, 2)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 800L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)
    val dt = DecimalType(10, 2)
    val readLo = read.get(0, dt)
    val readHi = read.get(1, dt)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }

  // PA-5.D ship blocker (NB3): Decimal(30, 5) -- precision > 18 uses
  // BigInteger / Int128 backing (16 bytes). cpp end must marshal in matching
  // byte order; JVM end must reconstruct correctly.
  // Trigger: when cpp marshals Decimal min/max for precision > 18.
  ignore("PA-5.D Decimal(30, 5) round-trip preserves big value (follow-up PR acceptance)") {
    val big = BigDecimal("12345678901234567890.12345")
    val lo = Decimal(big.bigDecimal.negate(), 30, 5)
    val hi = Decimal(big, 30, 5)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 1600L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)
    val dt = DecimalType(30, 5)
    val readLo = read.get(0, dt)
    val readHi = read.get(1, dt)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }

  // PA-5.E ship blocker (NS3): String byte-wise lex prune. UTF8String must
  // compare unsigned byte-by-byte; non-ASCII bytes (e.g. 0xE4 prefix for CJK)
  // test that the marshal preserves byte values exactly.
  // Trigger: when cpp marshals String min/max.
  ignore("PA-5.E String byte-wise lex round-trip preserves UTF-8 bytes (follow-up PR acceptance)") {
    val lo = UTF8String.fromString("apple")
    // UTF-8 bytes for two CJK code points (U+4E2D U+6587) constructed from hex
    // to keep this file ASCII-only (scalastyle nonascii filter).
    val hi = UTF8String.fromBytes(Array[Byte](
      0xe4.toByte,
      0xb8.toByte,
      0xad.toByte,
      0xe6.toByte,
      0x96.toByte,
      0x87.toByte))
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 1024L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)
    val readLo = read.getUTF8String(0)
    val readHi = read.getUTF8String(1)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }
}
