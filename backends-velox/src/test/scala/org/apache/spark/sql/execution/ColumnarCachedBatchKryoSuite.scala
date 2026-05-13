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
 * Tests for CachedColumnarBatch Kryo (de)serialization. Pure JVM, no native lib required.
 *
 * Wire-format invariant: Kryo `Output.writeInt(int)` writes a fixed 4-byte BIG-ENDIAN int; the
 * (int, boolean) overload silently forwards to writeVarInt (1-5 bytes) and would corrupt the
 * length-prefixed read path.
 */
class ColumnarCachedBatchKryoSuite extends AnyFunSuite {

  // Use serializer.write / serializer.read directly; writeObject/readObject would prepend a
  // varint refId envelope. Production goes through Spark SerializerInstance internals where
  // reference tracking is off for cached batches.
  private def roundTrip(batch: CachedColumnarBatch): CachedColumnarBatch = {
    val ser = new CachedColumnarBatchKryoSerializer
    val kryo = new Kryo()
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(kryo, out, batch)
    out.close()
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val read = ser.read(kryo, in, classOf[CachedColumnarBatch])
    in.close()
    read
  }

  test("stats field round-trip") {
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

  test("stats=null round-trip") {
    val batch = CachedColumnarBatch(
      numRows = 7,
      sizeInBytes = 123L,
      bytes = Array[Byte](9, 8, 7),
      stats = null)

    val read = roundTrip(batch)

    assert(read.numRows === 7)
    assert(read.sizeInBytes === 123L)
    assert(read.bytes === Array[Byte](9, 8, 7))
    assert(read.stats === null)
  }
}
