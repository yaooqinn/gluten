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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.io.ByteArrayOutputStream
import java.lang.{Double => JDouble, Float => JFloat}
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

/**
 * A Velox columnar cache batch carrying per-partition column statistics.
 *
 * `stats` follows the [[SimpleMetricsCachedBatch]] contract (SPARK-32274): per-column slots
 * `(lowerBound, upperBound, nullCount, count, sizeInBytes)`. `null` means stats are unavailable
 * (legacy V1 binary, or partition-stats SQLConf disabled); the serializer's `buildFilter` override
 * directs such batches through unchanged to avoid NPE in vanilla
 * `SimpleMetricsCachedBatchSerializer.buildFilter` on `partitionFilter.eval(null)`.
 *
 * `sizeInBytes` is the serialized blob length (Velox off-heap footprint), overriding the trait's
 * default per-column sum so cache-eviction accounting matches actual memory.
 *
 * Manual Kryo registration (Spark 4.1 doc TODO):
 * {{{
 *   spark.kryo.classesToRegister=org.apache.spark.sql.execution.CachedColumnarBatch
 * }}}
 */
@DefaultSerializer(classOf[CachedColumnarBatchKryoSerializer])
case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte],
    override val stats: InternalRow,
    // Schema is carried per-batch so Kryo read path can dispatch (de)serializeStats by source
    // dataType. Nullable for V1 binary back-compat.
    schema: StructType = null)
  extends SimpleMetricsCachedBatch

/**
 * Kryo serializer for [[CachedColumnarBatch]] with magic-prefix versioning.
 *
 * V3 layout (current, schema-bearing):
 * {{{
 *   [V3_MAGIC: 0xFE 0xCA 0x53 0x03]
 *   [numRows: Int]                  // Kryo fixed 4-byte BE (single-arg writeInt)
 *   [sizeInBytes: Long]
 *   [bytes.length + 1: Int]         // +1 distinguishes Kryo.NULL
 *   [bytes]
 *   [hasStats: Boolean]  if true: [statsLen: Int] [statsBlob]
 *   [hasSchema: Boolean] if true: [schemaLen: Int] [schemaJsonBytes]
 * }}}
 *
 * V2 layout (no schema tail) and V1 layout (no magic, no stats) are still readable for
 * rolling-deploy / cross-version compat. V2-read passes `schema=null` so deserializeStats falls
 * back to the legacy BIGINT-only path; V1-read produces `stats=null`.
 *
 * Magic disjointness from V1: a V1 stream's first 4 bytes are `numRows` (Kryo fixed 4-byte BE); any
 * non-negative Int < 2^31 has high byte in [0x00, 0x7F], so the magic's high byte 0xFE > 0x7F is
 * structurally unreachable.
 */
class CachedColumnarBatchKryoSerializer extends KryoSerializer[CachedColumnarBatch] {

  override def write(kryo: Kryo, output: Output, batch: CachedColumnarBatch): Unit = {
    output.writeBytes(CachedColumnarBatchKryoSerializer.V3_MAGIC)
    // Use the single-arg writeInt(int) overload: fixed 4-byte BE. The (int, boolean) overload
    // silently forwards to writeVarInt and would break magic-prefix sniffing on read.
    output.writeInt(batch.numRows)
    output.writeLong(batch.sizeInBytes)
    require(
      batch.bytes != null,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"serialize using ${this.getClass.getName}")
    output.writeInt(batch.bytes.length + 1) // +1 distinguishes Kryo.NULL
    output.writeBytes(batch.bytes)
    if (batch.stats == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val statsBytes = CachedColumnarBatchKryoSerializer.serializeStats(batch.stats, batch.schema)
      output.writeInt(statsBytes.length)
      output.writeBytes(statsBytes)
    }
    if (batch.schema == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val schemaBytes = batch.schema.json.getBytes(UTF_8)
      output.writeInt(schemaBytes.length)
      output.writeBytes(schemaBytes)
    }
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[CachedColumnarBatch]): CachedColumnarBatch = {
    val first4 = new Array[Byte](4)
    input.readBytes(first4)
    if (Arrays.equals(first4, CachedColumnarBatchKryoSerializer.V3_MAGIC)) {
      readV3(input)
    } else if (Arrays.equals(first4, CachedColumnarBatchKryoSerializer.V2_MAGIC)) {
      readV2NoSchema(input)
    } else {
      readV1(input, first4)
    }
  }

  private def readV3(input: Input): CachedColumnarBatch = {
    val numRows = input.readInt()
    val sizeInBytes = input.readLong()
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    val bytes = new Array[Byte](length - 1)
    input.readBytes(bytes)
    val hasStats = input.readBoolean()
    // Read schema *with* stats so deserializeStats can dispatch by dataType. Even when
    // hasStats=false we must consume the hasSchema tag to keep the stream aligned.
    // NB: avoid `val (a: T, b: U) = ...` -- Scala 2.13 erases Tuple2 generics and the typed
    // pattern match throws MatchError at runtime.
    val statsAndSchema: (InternalRow, StructType) = if (hasStats) {
      val statsLen = input.readInt()
      val statsBytes = new Array[Byte](statsLen)
      input.readBytes(statsBytes)
      val sch = readOptionalSchema(input)
      (CachedColumnarBatchKryoSerializer.deserializeStats(statsBytes, sch), sch)
    } else {
      (null, readOptionalSchema(input))
    }
    CachedColumnarBatch(numRows, sizeInBytes, bytes, statsAndSchema._1, statsAndSchema._2)
  }

  private def readOptionalSchema(input: Input): StructType = {
    if (!input.readBoolean()) {
      null
    } else {
      val schemaLen = input.readInt()
      val schemaBytes = new Array[Byte](schemaLen)
      input.readBytes(schemaBytes)
      DataType.fromJson(new String(schemaBytes, UTF_8)).asInstanceOf[StructType]
    }
  }

  // Pre-V3 layout (no schema tail). Kept readable for rolling-deploy compat: V2-emitted batches
  // from older executors must still deserialize. schema=null forces the legacy BIGINT-only path
  // in deserializeStats.
  private def readV2NoSchema(input: Input): CachedColumnarBatch = {
    val numRows = input.readInt()
    val sizeInBytes = input.readLong()
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    val bytes = new Array[Byte](length - 1)
    input.readBytes(bytes)
    val hasStats = input.readBoolean()
    val stats: InternalRow = if (hasStats) {
      val statsLen = input.readInt()
      val statsBytes = new Array[Byte](statsLen)
      input.readBytes(statsBytes)
      CachedColumnarBatchKryoSerializer.deserializeStats(statsBytes, null)
    } else null
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats, schema = null)
  }

  // Legacy V1 layout: no magic, no stats. first4 is numRows in Kryo fixed 4-byte BE.
  private def readV1(input: Input, first4: Array[Byte]): CachedColumnarBatch = {
    val numRows =
      ((first4(0) & 0xff) << 24) |
        ((first4(1) & 0xff) << 16) |
        ((first4(2) & 0xff) << 8) |
        (first4(3) & 0xff)
    val sizeInBytes = input.readLong()
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    val bytes = new Array[Byte](length - 1)
    input.readBytes(bytes)
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats = null, schema = null)
  }
}

object CachedColumnarBatchKryoSerializer {
  // Magic bytes are chosen so the high byte (0xFE) cannot collide with V1's first byte:
  // V1 starts with numRows as Kryo fixed 4-byte BE, whose high byte is in [0x00, 0x7F] for any
  // non-negative Int < 2^31.
  val V2_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte)
  val V3_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x03.toByte)

  // Per-column statsBlob layout (LE throughout, matches the cpp emitter in
  // VeloxColumnarBatchSerializer.cc):
  //
  //   [ numCols: u32 ]
  //   per col:
  //     [ supported: u8 ]
  //     [ nullCount: u32 ]
  //     [ count: u32 ]
  //     [ sizeInBytes: u64 ]
  //     if supported:
  //       [ lowerBoundLen: u32 ] [ lowerBound bytes ]
  //       [ upperBoundLen: u32 ] [ upperBound bytes ]
  //
  // The vanilla SimpleMetricsCachedBatch.stats InternalRow has 5 slots per source column in
  // order (lowerBound, upperBound, nullCount, count, sizeInBytes).
  //
  // Source dataTypes outside this allowlist are demoted to supported=0 in serializeStats
  // (the cpp side may still emit supported=1 for short-Decimal as Velox BIGINT; the JVM
  // gate prevents UnsupportedOperationException in dispatch).
  private[execution] def isDispatchable(dt: DataType): Boolean =
    dt match {
      case IntegerType | DateType | _: YearMonthIntervalType => true
      case ShortType => true
      case ByteType => true
      case LongType | _: DayTimeIntervalType | TimestampType | TimestampNTZType => true
      case d: DecimalType if d.precision <= 18 => true // short-decimal: Long unscaled
      case d: DecimalType if d.precision <= 38 => true // long-decimal: 16B LE int128
      case FloatType => true // 4B IEEE 754; NaN guard in cpp scanMinMax
      case DoubleType => true // 8B IEEE 754; NaN guard in cpp scanMinMax
      case BooleanType => true
      case StringType => true // truncated to 256B; see encodeStringBounds
      case _ => false
    }

  // schema may be null for legacy callsites; in that case behave as BIGINT-only (V2 read path).
  private[execution] def serializeStats(
      stats: InternalRow,
      schema: StructType): Array[Byte] = {
    require(
      stats.numFields % 5 == 0,
      s"stats InternalRow numFields=${stats.numFields} must be a multiple of 5 " +
        s"(vanilla PartitionStatistics schema = 5 slots per column)"
    )
    val numCols = stats.numFields / 5
    val baos = new ByteArrayOutputStream()
    writeU32LE(baos, numCols)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val hasLower = !stats.isNullAt(base)
      val hasUpper = !stats.isNullAt(base + 1)
      val dispatchable = (schema == null) ||
        CachedColumnarBatchKryoSerializer.isDispatchable(schema(col).dataType)
      // For String, pre-compute the truncated payload so an all-0xFF carry overflow
      // can demote `supported` *before* the supported byte is written.
      val isStringCol = (schema != null) && hasLower && hasUpper &&
        (schema(col).dataType eq StringType)
      val stringPayload: Option[(Array[Byte], Array[Byte])] =
        if (isStringCol) {
          val loB = stats.getUTF8String(base).getBytes
          val hiB = stats.getUTF8String(base + 1).getBytes
          encodeStringBounds(loB, hiB)
        } else None
      val supported = hasLower && hasUpper && dispatchable &&
        (!isStringCol || stringPayload.isDefined)
      baos.write(if (supported) 1 else 0)
      writeU32LE(baos, if (stats.isNullAt(base + 2)) 0 else stats.getInt(base + 2))
      writeU32LE(baos, if (stats.isNullAt(base + 3)) 0 else stats.getInt(base + 3))
      writeU64LE(baos, if (stats.isNullAt(base + 4)) 0L else stats.getLong(base + 4))
      if (supported) {
        // schema==null => BIGINT-only legacy behavior. Otherwise dispatch by source dataType,
        // matching vanilla ColumnBuilder's union for the integer / long families.
        val dt: DataType =
          if (schema == null) LongType else schema(col).dataType
        dt match {
          case IntegerType | DateType | _: YearMonthIntervalType =>
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base))
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base + 1))
          case ShortType =>
            writeU32LE(baos, 2)
            writeU16LE(baos, stats.getShort(base) & 0xffff)
            writeU32LE(baos, 2)
            writeU16LE(baos, stats.getShort(base + 1) & 0xffff)
          case ByteType =>
            writeU32LE(baos, 1)
            baos.write(stats.getByte(base) & 0xff)
            writeU32LE(baos, 1)
            baos.write(stats.getByte(base + 1) & 0xff)
          case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base))
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base + 1))
          case d: DecimalType if d.precision <= 18 =>
            // short-Decimal: Long unscaled (matches Velox short-decimal physical = BIGINT).
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base, d.precision, d.scale).toUnscaledLong)
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base + 1, d.precision, d.scale).toUnscaledLong)
          case d: DecimalType if d.precision <= 38 =>
            // long-Decimal: 16B LE signed two's-complement (cpp HUGEINT int128 wire).
            val loDec = stats.getDecimal(base, d.precision, d.scale)
            val hiDec = stats.getDecimal(base + 1, d.precision, d.scale)
            writeU32LE(baos, 16)
            writeI128LE(baos, loDec.toJavaBigDecimal.unscaledValue)
            writeU32LE(baos, 16)
            writeI128LE(baos, hiDec.toJavaBigDecimal.unscaledValue)
          case FloatType =>
            writeU32LE(baos, 4)
            writeU32LE(baos, JFloat.floatToRawIntBits(stats.getFloat(base)))
            writeU32LE(baos, 4)
            writeU32LE(baos, JFloat.floatToRawIntBits(stats.getFloat(base + 1)))
          case DoubleType =>
            writeU32LE(baos, 8)
            writeU64LE(baos, JDouble.doubleToRawLongBits(stats.getDouble(base)))
            writeU32LE(baos, 8)
            writeU64LE(baos, JDouble.doubleToRawLongBits(stats.getDouble(base + 1)))
          case BooleanType =>
            writeU32LE(baos, 1)
            baos.write(if (stats.getBoolean(base)) 1 else 0)
            writeU32LE(baos, 1)
            baos.write(if (stats.getBoolean(base + 1)) 1 else 0)
          case StringType =>
            // Pre-validated: encodeStringBounds returned Some, otherwise we'd have demoted.
            val (lo, hi) = stringPayload.get
            writeU32LE(baos, lo.length)
            baos.write(lo)
            writeU32LE(baos, hi.length)
            baos.write(hi)
          case other =>
            throw new UnsupportedOperationException(
              s"serializeStats: dispatch for $other not implemented")
        }
      }
      col += 1
    }
    baos.toByteArray
  }

  // schema may be null for legacy callsites; in that case behave as BIGINT-only (V2 read path).
  private[execution] def deserializeStats(
      blob: Array[Byte],
      schema: StructType): InternalRow = {
    val buf = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN)
    val numCols = buf.getInt
    require(
      numCols >= 0 && numCols <= 4096,
      s"corrupt statsBlob: numCols=$numCols out of valid range [0, 4096]")
    val row = new GenericInternalRow(numCols * 5)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val supported = buf.get()
      val nullCount = buf.getInt
      val count = buf.getInt
      val sizeInBytes = buf.getLong
      if (supported == 1) {
        val dt: DataType =
          if (schema == null) LongType else schema(col).dataType
        val lowerLen = buf.getInt
        dt match {
          case IntegerType | DateType | _: YearMonthIntervalType =>
            require(lowerLen == 4, s"Integer-family expects 4-byte lowerBound, got $lowerLen")
            row.update(base, buf.getInt)
            val upperLen = buf.getInt
            require(upperLen == 4, s"Integer-family expects 4-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getInt)
          case ShortType =>
            require(lowerLen == 2, s"ShortType expects 2-byte lowerBound, got $lowerLen")
            row.update(base, buf.getShort)
            val upperLen = buf.getInt
            require(upperLen == 2, s"ShortType expects 2-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getShort)
          case ByteType =>
            require(lowerLen == 1, s"ByteType expects 1-byte lowerBound, got $lowerLen")
            row.update(base, buf.get)
            val upperLen = buf.getInt
            require(upperLen == 1, s"ByteType expects 1-byte upperBound, got $upperLen")
            row.update(base + 1, buf.get)
          case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
            require(lowerLen == 8, s"Long-family expects 8-byte lowerBound, got $lowerLen")
            row.update(base, buf.getLong)
            val upperLen = buf.getInt
            require(upperLen == 8, s"Long-family expects 8-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getLong)
          case d: DecimalType if d.precision <= 18 =>
            // Wrap as Decimal (NOT raw Long), else SpecificInternalRow.getDecimal CCEs at codegen.
            require(lowerLen == 8, s"short-Decimal expects 8-byte lowerBound, got $lowerLen")
            row.update(base, Decimal(buf.getLong, d.precision, d.scale))
            val upperLen = buf.getInt
            require(upperLen == 8, s"short-Decimal expects 8-byte upperBound, got $upperLen")
            row.update(base + 1, Decimal(buf.getLong, d.precision, d.scale))
          case d: DecimalType if d.precision <= 38 =>
            require(lowerLen == 16, s"long-Decimal expects 16-byte lowerBound, got $lowerLen")
            val loBytes = new Array[Byte](16)
            buf.get(loBytes)
            row.update(
              base,
              Decimal(new JBigDecimal(readI128LE(loBytes), d.scale), d.precision, d.scale))
            val upperLenL = buf.getInt
            require(upperLenL == 16, s"long-Decimal expects 16-byte upperBound, got $upperLenL")
            val hiBytes = new Array[Byte](16)
            buf.get(hiBytes)
            row.update(
              base + 1,
              Decimal(new JBigDecimal(readI128LE(hiBytes), d.scale), d.precision, d.scale))
          case FloatType =>
            require(lowerLen == 4, s"FloatType expects 4B lowerBound, got $lowerLen")
            row.update(base, JFloat.intBitsToFloat(buf.getInt))
            val upperLenF = buf.getInt
            require(upperLenF == 4, s"FloatType expects 4B upperBound, got $upperLenF")
            row.update(base + 1, JFloat.intBitsToFloat(buf.getInt))
          case DoubleType =>
            require(lowerLen == 8, s"DoubleType expects 8B lowerBound, got $lowerLen")
            row.update(base, JDouble.longBitsToDouble(buf.getLong))
            val upperLenD = buf.getInt
            require(upperLenD == 8, s"DoubleType expects 8B upperBound, got $upperLenD")
            row.update(base + 1, JDouble.longBitsToDouble(buf.getLong))
          case BooleanType =>
            require(lowerLen == 1, s"BooleanType expects 1B lowerBound, got $lowerLen")
            row.update(base, buf.get != 0)
            val upperLenB = buf.getInt
            require(upperLenB == 1, s"BooleanType expects 1B upperBound, got $upperLenB")
            row.update(base + 1, buf.get != 0)
          case StringType =>
            require(
              lowerLen >= 0 && lowerLen <= 256,
              s"StringType expects lowerBound in [0, 256], got $lowerLen")
            val loBytes = new Array[Byte](lowerLen)
            buf.get(loBytes)
            row.update(base, UTF8String.fromBytes(loBytes))
            val upperLenS = buf.getInt
            require(
              upperLenS >= 0 && upperLenS <= 256,
              s"StringType expects upperBound in [0, 256], got $upperLenS")
            val hiBytes = new Array[Byte](upperLenS)
            buf.get(hiBytes)
            row.update(base + 1, UTF8String.fromBytes(hiBytes))
          case _ =>
            // cpp may emit supported=1 for types not yet in JVM dispatch (e.g. short-Decimal as
            // Velox BIGINT). Skip both payloads using their wire-declared lengths instead of
            // crashing; the row keeps the slot null so the caller treats it as supported=false.
            buf.get(new Array[Byte](lowerLen))
            val upperSkipLen = buf.getInt
            buf.get(new Array[Byte](upperSkipLen))
        }
      }
      row.update(base + 2, nullCount)
      row.update(base + 3, count)
      row.update(base + 4, sizeInBytes)
      col += 1
    }
    row
  }

  private def writeU16LE(out: ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
  }

  private def writeU32LE(out: ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
    out.write((v >>> 16) & 0xff)
    out.write((v >>> 24) & 0xff)
  }

  private def writeU64LE(out: ByteArrayOutputStream, v: Long): Unit = {
    var i = 0
    while (i < 8) {
      out.write(((v >>> (8 * i)) & 0xffL).toInt)
      i += 1
    }
  }

  private def writeI64LE(out: ByteArrayOutputStream, v: Long): Unit =
    writeU64LE(out, v)

  // 16B LE signed two's-complement representation of a BigInteger. BigInteger.toByteArray()
  // returns big-endian signed minimal-width bytes; sign-extend to 16 then reverse to LE.
  private def writeI128LE(out: ByteArrayOutputStream, v: BigInteger): Unit = {
    val raw = v.toByteArray
    require(raw.length <= 16, s"BigInteger does not fit int128 (${raw.length} bytes)")
    val padded = new Array[Byte](16)
    val signByte: Byte = if (v.signum < 0) 0xff.toByte else 0x00.toByte
    var i = 0
    while (i < 16 - raw.length) {
      padded(i) = signByte
      i += 1
    }
    System.arraycopy(raw, 0, padded, 16 - raw.length, raw.length)
    var j = 0
    while (j < 8) {
      val t = padded(j)
      padded(j) = padded(15 - j)
      padded(15 - j) = t
      j += 1
    }
    out.write(padded)
  }

  private def readI128LE(le: Array[Byte]): BigInteger = {
    require(le.length == 16, s"readI128LE expects 16 bytes, got ${le.length}")
    val be = new Array[Byte](16)
    var i = 0
    while (i < 16) {
      be(i) = le(15 - i)
      i += 1
    }
    new BigInteger(be) // signed BE constructor
  }

  // Encode (lo, hi) string bounds for the wire by truncating each to 256 bytes.
  // - Lower: prefix is byte-wise lex <= original, monotonic.
  // - Upper: needs +1 carry on the truncated tail to ensure encoded >= original. If the carry
  //   propagates past byte 0 (all 256 prefix bytes were 0xFF), we cannot form a safe widening
  //   upper bound; return None so the caller demotes supported.
  private val PA9_STRING_TRUNCATE_LEN = 256
  private def encodeStringBounds(
      loBytes: Array[Byte],
      hiBytes: Array[Byte]): Option[(Array[Byte], Array[Byte])] = {
    val loLen = math.min(loBytes.length, PA9_STRING_TRUNCATE_LEN)
    val loEnc = Arrays.copyOf(loBytes, loLen)
    if (hiBytes.length <= PA9_STRING_TRUNCATE_LEN) {
      Some((loEnc, Arrays.copyOf(hiBytes, hiBytes.length)))
    } else {
      val hiEnc = Arrays.copyOf(hiBytes, PA9_STRING_TRUNCATE_LEN)
      var i = PA9_STRING_TRUNCATE_LEN - 1
      while (i >= 0) {
        val b = (hiEnc(i) & 0xff) + 1
        if (b <= 0xff) {
          hiEnc(i) = b.toByte
          return Some((loEnc, hiEnc))
        }
        hiEnc(i) = 0.toByte
        i -= 1
      }
      None // carry overflowed past byte 0
    }
  }

  /**
   * Parse the JNI `serializeWithStats` framed return into (stats InternalRow, bytesBlob).
   *
   * Framed layout (matches cpp VeloxColumnarBatchSerializer.cc):
   * `[ V2_MAGIC: 4B ] [ statsLen: u32 LE ] [ statsBlob ] [ bytesLen: u32 LE ] [ bytesBlob ]`.
   *
   * Eager guards catch corrupt magic / truncated framing before they propagate.
   */
  private[execution] def parseFramedBytes(
      framed: Array[Byte],
      schema: StructType): (InternalRow, Array[Byte]) = {
    require(
      framed != null && framed.length >= 4 + 4 + 4,
      s"framed bytes too short: len=${if (framed == null) -1 else framed.length}")
    require(
      framed(0) == V2_MAGIC(0) && framed(1) == V2_MAGIC(1) &&
        framed(2) == V2_MAGIC(2) && framed(3) == V2_MAGIC(3),
      f"framed bytes magic mismatch: expected " +
        f"0x${V2_MAGIC(0) & 0xff}%02X${V2_MAGIC(1) & 0xff}%02X" +
        f"${V2_MAGIC(2) & 0xff}%02X${V2_MAGIC(3) & 0xff}%02X, got " +
        f"0x${framed(0) & 0xff}%02X${framed(1) & 0xff}%02X" +
        f"${framed(2) & 0xff}%02X${framed(3) & 0xff}%02X"
    )
    val buf = ByteBuffer.wrap(framed).order(ByteOrder.LITTLE_ENDIAN)
    buf.position(4) // skip magic
    val statsLen = buf.getInt
    require(
      statsLen >= 0 && statsLen <= buf.remaining() - 4,
      s"framed bytes statsLen=$statsLen exceeds remaining buffer ${buf.remaining() - 4}")
    val statsBlob = new Array[Byte](statsLen)
    buf.get(statsBlob)
    val stats = deserializeStats(statsBlob, schema)
    val bytesLen = buf.getInt
    require(
      bytesLen >= 0 && bytesLen == buf.remaining(),
      s"framed bytes bytesLen=$bytesLen != remaining ${buf.remaining()} (truncated or trailing)")
    val bytesBlob = new Array[Byte](bytesLen)
    buf.get(bytesBlob)
    (stats, bytesBlob)
  }
}

/**
 * Velox columnar cache serializer. Supports column pruning; converts row-based input via
 * [[RowToVeloxColumnarExec]] and falls back to vanilla Spark serialization for unsupported schemas.
 */
class ColumnarCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  private lazy val rowBasedCachedBatchSerializer = new DefaultCachedBatchSerializer

  private def glutenConf: GlutenConfig = GlutenConfig.get

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  private def validateSchema(schema: Seq[Attribute]): Boolean = {
    val dt = toStructType(schema)
    validateSchema(dt)
  }

  private def validateSchema(schema: StructType): Boolean = {
    val reason = BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema)
    if (reason.isDefined) {
      logInfo(s"Columnar cache does not support schema $schema, due to ${reason.get}")
      false
    } else {
      true
    }
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val localSchema = toStructType(schema)
    if (!validateSchema(localSchema)) {
      // we cannot use columnar cache here, as the `RowToColumnar` does not support this schema
      rowBasedCachedBatchSerializer.convertInternalRowToCachedBatch(
        input,
        schema,
        storageLevel,
        conf)
    } else {
      val numRows = conf.columnBatchSize
      val rddColumnarBatch = input.mapPartitions {
        it =>
          RowToVeloxColumnarExec.toColumnarBatchIterator(
            it,
            localSchema,
            numRows,
            VeloxConfig.get.veloxPreferredBatchBytes)
      }
      convertColumnarBatchToCachedBatch(rddColumnarBatch, schema, storageLevel, conf)
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      val rddColumnarBatch =
        convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      rddColumnarBatch.mapPartitions(it => VeloxColumnarToRowExec.toRowIterator(it))
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    input.mapPartitions {
      it =>
        val veloxBatches = it.map {
          /* Native code needs a Velox offloaded batch, making sure to offload
             if heavy batch is encountered */
          batch => VeloxColumnarBatches.ensureVeloxBatch(batch)
        }
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = veloxBatches.hasNext

          override def next(): CachedBatch = {
            val batch = veloxBatches.next()
            val jni = ColumnarBatchSerializerJniWrapper.create(
              Runtimes.contextInstance(
                BackendsApiManager.getBackendName,
                "ColumnarCachedBatchSerializer#serialize"))
            val handle =
              ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
            // PA-3.3: route through serializeWithStats when the JNI extension is
            // available (cpp PA-2.5c) AND the partition-stats conf is enabled
            // (PA-4 default-off ship gate). Capability is cached after first probe
            // (gluten-arrow PA-2.6 helper). When unavailable -- e.g. running
            // against an older cpp libgluten.so, or when the conf is left off --
            // fall back to the original serialize() path and emit stats=null;
            // PA-3.1 lazy-split iterator wrapper will then direct such batches
            // through without pruning.
            val partitionStatsEnabled =
              GlutenConfig.get.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED)
            if (partitionStatsEnabled && ColumnarBatchSerializerJniWrapper.supportsStatsExt()) {
              val framed = jni.serializeWithStats(handle)
              // PA-6.0: convert Seq[Attribute] to StructType once and carry per-batch
              // so Kryo (spill / disk cache) read path can dispatch by dataType.
              val structSchema = StructType(
                schema.map(
                  a =>
                    StructField(a.name, a.dataType, a.nullable)))
              val (stats, bytesBlob) =
                CachedColumnarBatchKryoSerializer.parseFramedBytes(framed, structSchema)
              CachedColumnarBatch(batch.numRows(), bytesBlob.length, bytesBlob, stats, structSchema)
            } else {
              val unsafeBuffer = jni.serialize(handle)
              val bytes = unsafeBuffer.toByteArray
              CachedColumnarBatch(
                batch.numRows(),
                bytes.length,
                bytes,
                stats = null,
                schema = null)
            }
          }
        }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToColumnarBatch(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      // Find the ordinals and data types of the requested columns.
      val requestedColumnIndices = selectedAttributes.map {
        a => cacheAttributes.map(_.exprId).indexOf(a.exprId)
      }
      val shouldSelectAttributes = cacheAttributes != selectedAttributes
      val localSchema = toStructType(cacheAttributes)
      val timezoneId = SQLConf.get.sessionLocalTimeZone
      input.mapPartitions {
        it =>
          val runtime = Runtimes.contextInstance(
            BackendsApiManager.getBackendName,
            "ColumnarCachedBatchSerializer#read")
          val jniWrapper = ColumnarBatchSerializerJniWrapper
            .create(runtime)
          val schema = SparkArrowUtil.toArrowSchema(localSchema, timezoneId)
          val arrowAlloc = ArrowBufferAllocators.contextInstance()
          val cSchema = ArrowSchema.allocateNew(arrowAlloc)
          ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
          val deserializerHandle = jniWrapper
            .init(cSchema.memoryAddress())
          cSchema.close()

          Iterators
            .wrap(new Iterator[ColumnarBatch] {
              override def hasNext: Boolean = it.hasNext

              override def next(): ColumnarBatch = {
                val cachedBatch = it.next().asInstanceOf[CachedColumnarBatch]
                val batchHandle =
                  jniWrapper
                    .deserialize(deserializerHandle, cachedBatch.bytes)
                val batch = ColumnarBatches.create(batchHandle)
                if (shouldSelectAttributes) {
                  try {
                    ColumnarBatches.select(
                      BackendsApiManager.getBackendName,
                      batch,
                      requestedColumnIndices.toArray)
                  } finally {
                    batch.close()
                  }
                } else {
                  batch
                }
              }
            })
            .protectInvocationFlow()
            .recycleIterator {
              jniWrapper.close(deserializerHandle)
            }
            .recyclePayload(_.close())
            .create()
      }
    }
  }

  // PA-3.1 GREEN: lazy-split iterator wrapper. stats=null batches (v1 binary
  // read path, or PA-4 SQLConf-off write path) are directed through unchanged
  // (skipping vanilla partition pruning); stats!=null batches are fed to the
  // inherited parent buildFilter. Without this guard, parent's
  // partitionFilter.eval(stats=null) NPEs on non-trivial predicates -- see
  // todos/features/gluten-inmemory-cache-stats/investigations/0003-...md rev 2
  // evidence 3 (both codegen and interpreted paths NPE, no fallback). A naive
  // occupier-row placeholder fails differently (silent drop, evidence 3.A);
  // only this lazy-split wrapper preserves correctness.
  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute])
      : (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val parent = super.buildFilter(predicates, cachedAttributes)
    (index, cachedBatchIterator) =>
      new Iterator[CachedBatch] {
        // Buffered so we can peek stats without consuming.
        private val peekable = cachedBatchIterator.buffered
        // When the head is stats != null, drain a contiguous run through
        // parent and buffer it here so hasNext can answer accurately.
        private var staged: Iterator[CachedBatch] = Iterator.empty

        // Advance until either staged has an element ready, or peekable is
        // drained. Called from hasNext so it is idempotent and safe to call
        // even after the iterator is exhausted.
        private def advance(): Unit = {
          while (!staged.hasNext && peekable.hasNext) {
            val head = peekable.head
            val stats = head match {
              case ccb: CachedColumnarBatch => ccb.stats
              case smcb: SimpleMetricsCachedBatch => smcb.stats
              case _ => null
            }
            if (stats == null) {
              // Pass through: load the staged buffer with this single batch
              // (do NOT feed to parent, which would NPE on null stats).
              staged = Iterator.single(peekable.next())
            } else {
              // Drain contiguous run of stats!=null batches and route through
              // parent. Lazy: only takes batches as parent's returned iterator
              // pulls. We give parent a self-terminating sub-iterator.
              val runIt = new Iterator[CachedBatch] {
                override def hasNext: Boolean = peekable.hasNext && {
                  val s = peekable.head match {
                    case ccb: CachedColumnarBatch => ccb.stats
                    case smcb: SimpleMetricsCachedBatch => smcb.stats
                    case _ => null
                  }
                  s != null
                }
                override def next(): CachedBatch = peekable.next()
              }
              staged = parent(index, runIt)
              // If parent pruned every batch in the run, loop and check
              // peekable again -- next batch may be stats=null pass-through.
            }
          }
        }

        override def hasNext: Boolean = {
          advance()
          staged.hasNext
        }

        override def next(): CachedBatch = {
          advance()
          staged.next()
        }
      }
  }
}
