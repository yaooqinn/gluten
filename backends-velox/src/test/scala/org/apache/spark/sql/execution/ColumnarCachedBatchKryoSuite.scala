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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/**
 * PA-1 RED tests for CachedColumnarBatch stats field + Kryo magic-prefix sniff.
 *
 * Refs:
 *   - todos/features/gluten-inmemory-cache-stats/docs/0001-layerA-minmax-design.md
 *   - todos/features/gluten-inmemory-cache-stats/docs/0002-cpp-stats-contract.md
 *   - todos/features/gluten-inmemory-cache-stats/docs/0003-jni-binary-framing-reference.md
 *   - todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-1
 *
 * Pure JVM-only suite. No native lib required.
 */
class ColumnarCachedBatchKryoSuite extends AnyFunSuite {

  private def newKryo(): Kryo = {
    val kryo = new Kryo()
    kryo.register(classOf[CachedColumnarBatch], new CachedColumnarBatchKryoSerializer)
    kryo
  }

  private def roundTrip(batch: CachedColumnarBatch): CachedColumnarBatch = {
    val kryo = newKryo()
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    kryo.writeObject(out, batch)
    out.close()
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val read = kryo.readObject(in, classOf[CachedColumnarBatch])
    in.close()
    read
  }

  // RED case 1: case class currently has 3 fields (numRows, sizeInBytes, bytes).
  // Adding `stats: InternalRow` requires the case class to accept a 4th param.
  // Expected RED failure: compile error "too many arguments" until case class
  // gains the stats field.
  test("PA-1.1 testStatsFieldRoundTripV2") {
    val stats: InternalRow = new GenericInternalRow(Array[Any](42L, 100L))
    val batch = CachedColumnarBatch(
      numRows = 10,
      sizeInBytes = 64L,
      bytes = Array[Byte](1, 2, 3, 4),
      stats = stats)

    val read = roundTrip(batch)

    assert(read.numRows === 10)
    assert(read.sizeInBytes === 64L)
    assert(read.bytes === Array[Byte](1, 2, 3, 4))
    assert(read.stats !== null, "stats field must round-trip")
    assert(read.stats.numFields === 2)
    assert(read.stats.getLong(0) === 42L)
    assert(read.stats.getLong(1) === 100L)
  }

  // RED case 2: backward compat — v1 binary (without magic prefix) should be
  // readable by v2 reader, returning stats=null (graceful degrade for buildFilter
  // pass-through). v1 layout: numRows: Int LE, sizeInBytes: Long, bytes.length+1: Int, bytes.
  // Expected RED failure: v2 reader (currently sniffs first byte) chokes on v1
  // binary because there's no version marker mechanism yet.
  test("PA-1.2 testKryoV1Backwards") {
    // Manually craft v1 binary (pre-stats-field layout, mimicking pre-PA-1 writer).
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(7)          // numRows
    out.writeLong(123L)      // sizeInBytes
    val payload = Array[Byte](9, 8, 7)
    out.writeInt(payload.length + 1)  // length+1 (Kryo.NULL distinguisher)
    out.writeBytes(payload)
    out.close()
    val v1Binary = baos.toByteArray

    val kryo = newKryo()
    val in = new Input(new ByteArrayInputStream(v1Binary))
    val read = kryo.readObject(in, classOf[CachedColumnarBatch])
    in.close()

    assert(read.numRows === 7)
    assert(read.sizeInBytes === 123L)
    assert(read.bytes === payload)
    assert(read.stats === null, "v1 binary read should yield stats=null (graceful degrade)")
  }

  // RED case 3: the critical NB1 ship-blocker — when v1 numRows == 258 (0x102 LE),
  // first byte of v1 binary is 0x02. A naive first-byte version sniff would
  // mis-identify v1 as v2 → silent corruption. Magic-prefix 4-byte sniff
  // (V2_MAGIC = 0xFE 0xCA 0x53 0x02) avoids this because v1 binary's first 4
  // bytes are numRows (Int LE), and any numRows < 2^31 cannot produce 0xFECA5302
  // (high byte 0xFE requires numRows >= 4_274_614_272 which overflows Int).
  // Expected RED failure: naive impl crashes or returns garbage when reading
  // v1 with numRows=258. Magic-prefix impl reads it correctly.
  test("PA-1.3 testV1BinaryNumRowsHigh02NotMisidentified") {
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    // numRows = 258 = 0x102. Kryo Output.writeInt uses var-int by default,
    // but we want a fixed Int LE here to mimic the legacy raw layout. Use writeInt
    // with second arg false to force 4-byte LE encoding.
    out.writeInt(258, false)
    out.writeLong(456L)
    val payload = Array[Byte](5, 5, 5)
    out.writeInt(payload.length + 1, false)
    out.writeBytes(payload)
    out.close()
    val v1Binary = baos.toByteArray

    // First byte of v1 binary should be 0x02 (low byte of numRows=258 in LE).
    assert(v1Binary(0) === 0x02.toByte,
      s"sanity: v1 first byte expected 0x02, got 0x${v1Binary(0) & 0xff}%02X")

    val kryo = newKryo()
    val in = new Input(new ByteArrayInputStream(v1Binary))
    val read = kryo.readObject(in, classOf[CachedColumnarBatch])
    in.close()

    // Critical: numRows must be 258, NOT garbage from misidentifying as v2.
    assert(read.numRows === 258,
      "magic-prefix sniff must NOT mis-identify v1 binary with numRows==258 as v2")
    assert(read.sizeInBytes === 456L)
    assert(read.bytes === payload)
    assert(read.stats === null)
  }
}
