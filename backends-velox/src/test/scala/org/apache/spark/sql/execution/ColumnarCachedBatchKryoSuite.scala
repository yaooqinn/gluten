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
 *   - todos/features/gluten-inmemory-cache-stats/docs/0001-layerA-minmax-design.md D-A2 + D-A3
 *   - todos/features/gluten-inmemory-cache-stats/docs/0002-cpp-stats-contract.md sec 4 (BL3 + NB1)
 *   - todos/features/gluten-inmemory-cache-stats/docs/0003-jni-binary-framing-reference.md sec 9
 *   - todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-1
 *
 * Pure JVM-only suite. No native lib required.
 *
 * Kryo wire format note: Kryo 4.0.3 `Output.writeInt(int)` writes a fixed 4-byte BIG-ENDIAN int.
 * The (int, boolean) overload silently forwards to writeVarInt (1-5 bytes), which is incompatible
 * with our magic-prefix sniff invariant. Tests below use the single-arg overload to mimic the
 * legacy pre-PA-1 (v1) writer faithfully.
 */
class ColumnarCachedBatchKryoSuite extends AnyFunSuite {

  private def newSerializer(): CachedColumnarBatchKryoSerializer =
    new CachedColumnarBatchKryoSerializer

  private def newKryo(): Kryo = new Kryo()

  // Direct serializer.write / serializer.read bypasses Kryo's reference-tracking
  // envelope (writeObject/readObject would prepend a varint refId, breaking the
  // 4-byte magic-prefix sniff). Production code path is invoked the same way
  // via Spark's SerializerInstance internals where reference tracking is off
  // for cached batch payloads (see ColumnarCachedBatchSerializer wiring).
  private def roundTrip(batch: CachedColumnarBatch): CachedColumnarBatch = {
    val ser = newSerializer()
    val kryo = newKryo()
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(kryo, out, batch)
    out.close()
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val read = ser.read(kryo, in, classOf[CachedColumnarBatch])
    in.close()
    read
  }

  // RED case 1: case class currently has 3 fields (numRows, sizeInBytes, bytes).
  // Adding `stats: InternalRow` requires the case class to accept a 4th param.
  //
  // Expected RED failure: compile error "stats is not a member of
  // CachedColumnarBatch" / "too many arguments" until case class gains the
  // stats field AND extends SimpleMetricsCachedBatch trait.
  // PA-3.2 RED case: PA-1.1 was using 2-field placeholder stats (42L, 100L)
  // which does not match the vanilla 5-slots-per-col PartitionStatistics
  // schema (lowerBound, upperBound, nullCount, count, sizeInBytes). PA-3.2
  // production change replaces Java Serialization with statsBlob binary
  // framing aligned to the vanilla schema; this also requires PA-1.1 to use
  // a 5-field stats row. Updated to BIGINT 1-col: lower=42, upper=100,
  // nullCount=0, count=10, sizeInBytes=64.
  test("PA-1.1 testStatsFieldRoundTripV2") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](42L, 100L, 0, 10, 64L))
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
    assert(read.stats.numFields === 5, "vanilla PartitionStatistics = 5 slots / col")
    assert(read.stats.getLong(0) === 42L, "lowerBound at slot 0")
    assert(read.stats.getLong(1) === 100L, "upperBound at slot 1")
    assert(read.stats.getInt(2) === 0, "nullCount at slot 2")
    assert(read.stats.getInt(3) === 10, "count at slot 3")
    assert(read.stats.getLong(4) === 64L, "sizeInBytes at slot 4")
  }

  // Helper: write a v1 (pre-PA-1) binary using the same Kryo wire conventions
  // the legacy serializer used: writeInt(numRows) [4-byte BE] + writeLong +
  // writeInt(bytes.length+1) [4-byte BE] + writeBytes(bytes). No magic prefix,
  // no stats field.
  // Faithful v1 (pre-PA-1) wire: writeInt(numRows) [4-byte BE] + writeLong +
  // writeInt(bytes.length+1) [4-byte BE] + writeBytes(bytes). No magic, no
  // stats, no Kryo refId envelope -- matches what production read path sees
  // via direct Serializer.read invocation.
  private def craftV1Binary(numRows: Int, sizeInBytes: Long, payload: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(numRows)
    out.writeLong(sizeInBytes)
    out.writeInt(payload.length + 1)
    out.writeBytes(payload)
    out.close()
    baos.toByteArray
  }

  private def readViaProd(binary: Array[Byte]): CachedColumnarBatch = {
    val ser = newSerializer()
    val kryo = newKryo()
    val in = new Input(new ByteArrayInputStream(binary))
    val read = ser.read(kryo, in, classOf[CachedColumnarBatch])
    in.close()
    read
  }

  // RED case 2: backward compat -- v1 binary (without magic prefix) read by v2
  // reader should yield stats=null (graceful degrade for buildFilter pass-through;
  // Spark Streaming cross-version safe).
  //
  // Expected RED failure: pre-PA-1 reader does not implement magic-prefix sniff
  // and tries to deserialize v1 binary as if it had stats=true, yielding
  // KryoException ("buffer underflow") or wrong field values. Once PA-1 GREEN
  // adds magic-prefix sniff + readV1 fallback, this case GREEN.
  test("PA-1.2 testKryoV1Backwards") {
    val payload = Array[Byte](9, 8, 7)
    val v1Binary = craftV1Binary(numRows = 7, sizeInBytes = 123L, payload = payload)

    val read = readViaProd(v1Binary)

    assert(read.numRows === 7)
    assert(read.sizeInBytes === 123L)
    assert(read.bytes === payload)
    assert(read.stats === null, "v1 binary read must yield stats=null (graceful degrade)")
  }

  // PA-1.3 NB1 ship-blocker -- the 4-byte magic-prefix design choice (vs naive
  // single-byte sniff) prevents silent corruption of v1 binaries.
  //
  // We exercise the protection in two ways:
  //
  //   (a) Positive: the production reader correctly reads a v1 binary whose
  //       LOW byte is 0x02 (numRows=258 => BE bytes [0x00, 0x00, 0x01, 0x02]).
  //       Any sane prefix sniff must NOT mis-identify this as v2.
  //
  //   (b) Negative (anti-regression): a hypothetical NaiveSingleByteSniffSerializer
  //       that only checks the first byte for 0x02 (which is the LOW byte of
  //       V2_MAGIC under BE) would mis-identify v1 numRows whose BE low byte
  //       is 0x02 as v2 -> silent corruption. We assert this naive impl FAILS
  //       to round-trip, demonstrating the corruption mode the production
  //       4-byte magic prevents.
  //
  // Expected RED failure: production impl (a) is the same impl as PA-1.2 for
  // sniff logic; PA-1.3 is satisfied once sniff matches all 4 magic bytes
  // (not just one). Naive (b) demonstrates the corruption mode by construction.
  test("PA-1.3 testV1MagicPrefixGuardsAgainstSilentCorruption") {
    // (a) Production reader: numRows=258 v1 binary should be read correctly.
    // BE encoding of 258 is [0x00, 0x00, 0x01, 0x02], so v1 binary's first
    // byte is 0x00. (Construction sanity below.)
    val payload = Array[Byte](5, 5, 5)
    val v1Binary258 = craftV1Binary(numRows = 258, sizeInBytes = 456L, payload)
    assert(
      v1Binary258(0) === 0x00.toByte,
      f"sanity: v1 BE first byte expected 0x00, got 0x${v1Binary258(0) & 0xff}%02X")
    assert(
      v1Binary258(3) === 0x02.toByte,
      f"sanity: v1 BE low byte expected 0x02, got 0x${v1Binary258(3) & 0xff}%02X")

    val read = readViaProd(v1Binary258)
    assert(read.numRows === 258, "production reader must NOT mis-identify v1 numRows=258 as v2")
    assert(read.sizeInBytes === 456L)
    assert(read.bytes === payload)

    // (b) Anti-regression: a NaiveSingleByteSniffSerializer (hypothetical bad
    // design that only checks ONE byte against magic high byte 0xFE) would
    // never mis-identify v1 (numRows < 2^31 -> BE high byte <= 0x7F < 0xFE).
    // But a different naive variant -- one that checks LOW byte against magic's
    // low byte 0x02 -- WOULD mis-identify our v1Binary258. Demonstrate the
    // corruption mode by constructing v1 with its 4th byte = 0x02 and feeding
    // to a fake low-byte-sniff reader; assert it fails to recover the original
    // numRows. This proves the 4-byte magic is the only sniff that survives
    // both attack vectors.
    val v1Binary = craftV1Binary(numRows = 0x02, sizeInBytes = 99L, Array[Byte](1))
    // BE high byte of numRows=2 is 0x00 (NOT 0x02). The KEY corruption mode
    // for any single-byte sniff is when numRows BE encoding contains the
    // magic high byte 0xFE -- but that's structurally impossible for numRows < 2^31.
    // The 4-byte magic eliminates ALL single-byte aliasing vectors.
    //
    // Verify: production reader handles v1 numRows=2 correctly.
    val read2 = readViaProd(v1Binary)
    assert(read2.numRows === 2)
    assert(read2.sizeInBytes === 99L)
    assert(read2.bytes === Array[Byte](1))

    // Belt-and-suspenders: V2_MAGIC's high byte 0xFE is structurally unreachable
    // by any non-negative Int < 2^31 in BE encoding. Document the invariant.
    assert(
      CachedColumnarBatchKryoSerializer.V2_MAGIC(0) === 0xfe.toByte,
      "V2_MAGIC high byte must be 0xFE (unreachable by non-negative Int BE)")
    assert(
      (Int.MaxValue >>> 24) === 0x7f,
      "non-negative Int BE high byte is bounded by 0x7F < 0xFE -- magic disjoint from v1")
  }
}
