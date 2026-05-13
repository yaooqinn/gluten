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
import org.apache.spark.sql.types.{ByteType, DayTimeIntervalType, IntegerType, LongType, ShortType, StructField, StructType, YearMonthIntervalType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * PA-6 G1 integer-family marshal tests (JVM-only unit tests, no native build).
 *
 * The cpp side (PA-6.2) emits supported=0 for non-BIGINT integer columns until the typeKind
 * dispatch lands. These tests pin the JVM serialize/deserialize dispatch by source-column dataType
 * so that when cpp catches up, plumbing is already correct (and end-to-end prune in PA-6.D/E
 * becomes a pure cpp delta).
 *
 * Refs: todos/features/gluten-inmemory-cache-stats/docs/0008-layerA-fulltype-extension.md
 * todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-6
 */
class ColumnarCachedBatchIntFamilyMarshalSuite extends AnyFunSuite {

  // PA-6.A RED expected: serializeStats currently hard-calls stats.getLong(base)
  // for every column (BIGINT-only PA-3.2 path); supplying an Integer lo/hi will
  // throw ClassCastException at unboxToLong even though we now pass the schema.
  // GREEN: serializeStats branches on schema(col).dataType, writes 4-byte LE for
  // IntegerType, and deserializeStats reads it back as Int.
  test("PA-6.A INT round-trip 4B LE preserves value") {
    val lo: Integer = Int.box(-2147483)
    val hi: Integer = Int.box(2147483)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 400L))
    val schema = StructType(Seq(
      StructField("k.lowerBound", IntegerType, nullable = true),
      StructField("k.upperBound", IntegerType, nullable = true),
      StructField("k.nullCount", IntegerType, nullable = false),
      StructField("k.count", IntegerType, nullable = false),
      StructField("k.sizeInBytes", LongType, nullable = false)
    ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getInt(0) == lo, s"lower bound corrupted: expected $lo got ${read.getInt(0)}")
    assert(read.getInt(1) == hi, s"upper bound corrupted: expected $hi got ${read.getInt(1)}")
    assert(read.getInt(2) == 0, "nullCount roundtrip")
    assert(read.getInt(3) == 100, "count roundtrip")
    assert(read.getLong(4) == 400L, "sizeInBytes roundtrip")
  }

  // PA-6.B RED expected: ShortType not in dispatch -> UnsupportedOperationException.
  // GREEN: serializeStats + deserializeStats add ShortType branch writing 2 LE bytes.
  test("PA-6.B SMALLINT round-trip 2B LE preserves value (incl negative)") {
    val lo: java.lang.Short = Short.box((-12345).toShort)
    val hi: java.lang.Short = Short.box(12345.toShort)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 200, 400L))
    val schema = StructType(Seq(
      StructField("k.lowerBound", ShortType, nullable = true),
      StructField("k.upperBound", ShortType, nullable = true),
      StructField("k.nullCount", IntegerType, nullable = false),
      StructField("k.count", IntegerType, nullable = false),
      StructField("k.sizeInBytes", LongType, nullable = false)
    ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getShort(0) == lo, s"lower: expected $lo got ${read.getShort(0)}")
    assert(read.getShort(1) == hi, s"upper: expected $hi got ${read.getShort(1)}")
    assert(read.getInt(3) == 200, "count roundtrip")
  }

  // PA-6.C RED expected: ByteType not in dispatch -> UnsupportedOperationException.
  // GREEN: serializeStats + deserializeStats add ByteType branch writing 1 byte.
  test("PA-6.C TINYINT round-trip 1B preserves value (incl negative)") {
    val lo: java.lang.Byte = Byte.box((-128).toByte)
    val hi: java.lang.Byte = Byte.box(127.toByte)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 50, 50L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", ByteType, nullable = true),
        StructField("k.upperBound", ByteType, nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getByte(0) == lo, s"lower: expected $lo got ${read.getByte(0)}")
    assert(read.getByte(1) == hi, s"upper: expected $hi got ${read.getByte(1)}")
    assert(read.getInt(3) == 50, "count roundtrip")
  }

  // PA-6.F.1 RED expected: YearMonthIntervalType not in dispatch.
  // GREEN: YearMonthIntervalType reuses the IntegerType branch (4 LE bytes,
  // physical Int per spark/.../YearMonthIntervalType.scala defaultSize == 4).
  test("PA-6.F.1 YearMonthInterval round-trip 4B LE (months as Int)") {
    val lo: Integer = Int.box(-12)
    val hi: Integer = Int.box(36)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 10, 40L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", YearMonthIntervalType(), nullable = true),
        StructField("k.upperBound", YearMonthIntervalType(), nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getInt(0) == lo, s"lower: expected $lo got ${read.getInt(0)}")
    assert(read.getInt(1) == hi, s"upper: expected $hi got ${read.getInt(1)}")
  }

  // PA-6.F.2 RED expected: DayTimeIntervalType not in dispatch.
  // GREEN: DayTimeIntervalType reuses the LongType branch (8 LE bytes,
  // physical Long microseconds per spark DayTimeIntervalType defaultSize == 8).
  test("PA-6.F.2 DayTimeInterval round-trip 8B LE (microseconds as Long)") {
    val lo: java.lang.Long = Long.box(-86400000000L)
    val hi: java.lang.Long = Long.box(86400000000L)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 10, 80L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", DayTimeIntervalType(), nullable = true),
        StructField("k.upperBound", DayTimeIntervalType(), nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getLong(0) == lo, s"lower: expected $lo got ${read.getLong(0)}")
    assert(read.getLong(1) == hi, s"upper: expected $hi got ${read.getLong(1)}")
  }
}
