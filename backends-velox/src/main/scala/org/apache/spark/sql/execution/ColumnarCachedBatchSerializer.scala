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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

/**
 * TODO: fix on Spark-4.1 - Documentation
 *
 * If you encounter serialization issues, manually register this class:
 * {{{
 *   spark.kryo.classesToRegister=org.apache.spark.sql.execution.CachedColumnarBatch
 * }}}
 *
 * `stats` carries per-partition column statistics (min/max/nullCount/count/sizeInBytes per
 * supported column) per the SimpleMetricsCachedBatch contract (SPARK-32274). `null` indicates stats
 * unavailable (e.g. v1 binary read or stats compute disabled); the serializer's buildFilter
 * override uses a lazy-split iterator wrapper to direct stats=null batches through (skipping
 * vanilla partition pruning) -- vanilla `SimpleMetricsCachedBatchSerializer.buildFilter` would NPE
 * on `partitionFilter.eval(null)` for non-trivial predicates (both codegen and interpreted paths;
 * no fallback). See todos/features/gluten-inmemory-cache-stats/investigations/
 * 0003-simplemetrics-buildfilter-survey.md rev 2 for the spark-shell evidence.
 *
 * Use eager `val` (not `lazy val`) for stats to keep Kryo round-trip deterministic. Note: trait
 * SimpleMetricsCachedBatch's default `sizeInBytes` sums per-column stats slots; we override with
 * the serialized blob length here for cache eviction accounting (Velox JNI serialize byte[] length
 * is the real off-heap footprint), not vanilla per-column-sum semantics.
 */
@DefaultSerializer(classOf[CachedColumnarBatchKryoSerializer])
case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte],
    override val stats: InternalRow,
    // PA-6.0: schema carried per-batch so Kryo read path can dispatch
    // serializeStats / deserializeStats by source-column dataType (vanilla full-type
    // alignment per design 0008 D-A5). Nullable for v1 binary back-compat
    // (legacy stats=null cache batches have no schema either).
    schema: org.apache.spark.sql.types.StructType = null)
  extends SimpleMetricsCachedBatch

/**
 * Kryo serializer for [[CachedColumnarBatch]] with magic-prefix versioning.
 *
 * v2 binary layout (current):
 * {{{
 *   [V2_MAGIC: 4 bytes 0xFE 0xCA 0x53 0x02]
 *   [numRows: Int (Kryo fixed 4-byte BIG-ENDIAN, via Output.writeInt(int))]
 *   [sizeInBytes: Long]
 *   [bytes.length+1: Int (Kryo fixed 4-byte BE, +1 to distinguish Kryo.NULL)]
 *   [bytes: bytes.length bytes]
 *   [hasStats: Boolean]
 *   if hasStats:
 *     [statsLen: Int]
 *     [statsBytes: statsLen bytes -- Layer A PA-3 will define statsBlob layout
 *      (see contract 0002 sec 3 + reference 0003 sec 3-4); PA-1 always writes stats=null]
 * }}}
 *
 * v1 binary layout (legacy, no stats field, pre-PA-1):
 * {{{
 *   [numRows: Int (Kryo fixed 4-byte BE)]
 *   [sizeInBytes: Long]
 *   [bytes.length+1: Int]
 *   [bytes]
 * }}}
 *
 * Read path uses 4-byte magic-prefix sniff. Magic `0xFECA5302` cannot collide with any v1 binary:
 * v1's first 4 bytes are numRows (Kryo fixed 4-byte BE), and any non-negative Int < 2^31 has BE
 * high byte (= `(numRows >>> 24) & 0xff`) in [0x00, 0x7F]. The magic's first byte 0xFE > 0x7F is
 * unreachable, so any v1 binary's first byte is <= 0x7F < 0xFE -- disjoint from the magic.
 *
 * v1 binary read produces stats=null -> buildFilter override uses lazy-split iterator wrapper to
 * direct such batches through, skipping vanilla partition pruning (Spark Streaming checkpoint
 * cross-version safe). NOTE: the prior comment "falls back to pass-through (graceful degrade)" was
 * wrong -- vanilla `SimpleMetricsCachedBatchSerializer.buildFilter` NPEs on
 * `partitionFilter.eval(null)`. See investigations/0003 rev 2 for evidence.
 *
 * NOTE (PA-1 scope): PA-1 write path always emits stats=null (see L347-350). The hasStats=true
 * branch + serializeStats/deserializeStats placeholders are inert in PA-1; PA-3 will (a) populate
 * stats from cpp computeStats output, (b) replace serializeStats placeholder with statsBlob binary
 * framing.
 */
class CachedColumnarBatchKryoSerializer extends KryoSerializer[CachedColumnarBatch] {

  override def write(kryo: Kryo, output: Output, batch: CachedColumnarBatch): Unit = {
    output.writeBytes(CachedColumnarBatchKryoSerializer.V2_MAGIC)
    // Kryo fixed 4-byte BE (single-arg overload). Avoid (int, boolean) overload
    // which silently forwards to writeVarInt (1-5 bytes) and breaks the magic
    // sniff invariant.
    output.writeInt(batch.numRows)
    output.writeLong(batch.sizeInBytes)
    require(
      batch.bytes != null,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"serialize using ${this.getClass.getName}")
    output.writeInt(batch.bytes.length + 1) // +1 to distinguish Kryo.NULL
    output.writeBytes(batch.bytes)
    if (batch.stats == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val statsBytes = CachedColumnarBatchKryoSerializer.serializeStats(batch.stats, batch.schema)
      output.writeInt(statsBytes.length)
      output.writeBytes(statsBytes)
    }
    // PA-6.0: schema for full-type dispatch (D-A5). Nullable: hasStats=false
    // batches still write hasSchema=false. v3 layout uses an explicit boolean
    // tag so reader can skip the JSON segment when schema is absent.
    if (batch.schema == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val schemaJson = batch.schema.json
      val schemaBytes = schemaJson.getBytes(java.nio.charset.StandardCharsets.UTF_8)
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
    if (java.util.Arrays.equals(first4, CachedColumnarBatchKryoSerializer.V2_MAGIC)) {
      readV2(input)
    } else {
      readV1(input, first4)
    }
  }

  private def readV2(input: Input): CachedColumnarBatch = {
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
    // PA-6.0: read schema first when present so deserializeStats can dispatch
    // by dataType. v3 layout: hasStats then statsBytes (if any) then hasSchema
    // then schemaJsonBytes (if any). Order MUST match writer above.
    // NOTE: avoid `val (a: T, b: U) = ...` tuple destructuring with type
    // ascription -- Scala 2.13 erases the Tuple2 generics and a typed-pattern
    // match against `Tuple2` throws MatchError at runtime.
    val statsAndSchema: (InternalRow, org.apache.spark.sql.types.StructType) = if (hasStats) {
      val statsLen = input.readInt()
      val statsBytes = new Array[Byte](statsLen)
      input.readBytes(statsBytes)
      val hasSchema = input.readBoolean()
      val sch = if (hasSchema) {
        val schemaLen = input.readInt()
        val schemaBytes = new Array[Byte](schemaLen)
        input.readBytes(schemaBytes)
        org.apache.spark.sql.types.DataType
          .fromJson(new String(schemaBytes, java.nio.charset.StandardCharsets.UTF_8))
          .asInstanceOf[org.apache.spark.sql.types.StructType]
      } else {
        null
      }
      (CachedColumnarBatchKryoSerializer.deserializeStats(statsBytes, sch), sch)
    } else {
      // Even with stats=null we must consume the hasSchema tag to keep the
      // stream aligned for any subsequent batches in the same Kryo session.
      val hasSchema = input.readBoolean()
      val sch = if (hasSchema) {
        val schemaLen = input.readInt()
        val schemaBytes = new Array[Byte](schemaLen)
        input.readBytes(schemaBytes)
        org.apache.spark.sql.types.DataType
          .fromJson(new String(schemaBytes, java.nio.charset.StandardCharsets.UTF_8))
          .asInstanceOf[org.apache.spark.sql.types.StructType]
      } else {
        null
      }
      (null, sch)
    }
    CachedColumnarBatch(numRows, sizeInBytes, bytes, statsAndSchema._1, statsAndSchema._2)
  }

  private def readV1(input: Input, first4: Array[Byte]): CachedColumnarBatch = {
    // first4 contains numRows in Kryo fixed 4-byte BE (legacy layout pre-PA-1).
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
    // stats=null -> buildFilter override falls back to pass-through (graceful degrade).
    // PA-6.0: v1 has no schema either, pass null schema; CachedColumnarBatch.schema
    // defaults to null so this remains source-compatible.
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats = null, schema = null)
  }
}

object CachedColumnarBatchKryoSerializer {
  // V2_MAGIC = 0xFE 0xCA 0x53 0x02. Cannot collide with v1 binary because v1's
  // first 4 bytes are numRows (Kryo fixed 4-byte BE) and any non-negative Int
  // < 2^31 has BE high byte in [0x00, 0x7F]. Magic high byte 0xFE > 0x7F is
  // structurally unreachable, so any v1 first byte is <= 0x7F < 0xFE -- disjoint.
  val V2_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte)

  // PA-3.2: statsBlob binary framing, aligned with cpp end
  // (cpp/velox/operators/serializer/VeloxColumnarBatchSerializer.cc PA-2.5b
  // helper). Layout (LE throughout, no schemaVer prefix to match cpp):
  //
  //   [ numCols: uint32 LE ]
  //   per col:
  //     [ supported: uint8 ]            // 1 if hasLower && hasUpper
  //     [ nullCount: uint32 LE ]
  //     [ count: uint32 LE ]
  //     [ sizeInBytes: uint64 LE ]
  //     if supported:
  //       [ lowerBoundLen: uint32 LE = 8 ]   // PA-3.2 v1: BIGINT only
  //       [ lowerBound: int64 LE ]
  //       [ upperBoundLen: uint32 LE = 8 ]
  //       [ upperBound: int64 LE ]
  //
  // PA-3.2 scope: BIGINT 1-col only (matches cpp PA-2.5b). Multi-col + other
  // types land in PA-3.3 / PA-3.5 / follow-up. The vanilla
  // SimpleMetricsCachedBatch.stats InternalRow layout is per-col 5 slots in
  // order (lowerBound, upperBound, nullCount, count, sizeInBytes) per docs/0003
  // sec 3 + investigations/0003 rev 2 evidence 2. PA-1 placeholder used 2
  // slots; PA-3.2 + test update align with vanilla.
  //
  // PA-6.0: schema parameter added for full-type dispatch (D-A5). PA-6.0 itself
  // remains BIGINT-only behavior (schema is unused inside); PA-6.A onwards adds
  // per-column dataType dispatch reading the schema. schema may be null for
  // legacy callsites that have not been updated; in that case we keep the
  // PA-3.2 BIGINT-only behavior (backward compatible).
  private[execution] def serializeStats(
      stats: InternalRow,
      schema: org.apache.spark.sql.types.StructType): Array[Byte] = {
    require(
      stats.numFields % 5 == 0,
      s"stats InternalRow numFields=${stats.numFields} must be a multiple of 5 " +
        s"(vanilla PartitionStatistics schema = 5 slots per column)"
    )
    val numCols = stats.numFields / 5
    val baos = new java.io.ByteArrayOutputStream()
    writeU32LE(baos, numCols)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val hasLower = !stats.isNullAt(base)
      val hasUpper = !stats.isNullAt(base + 1)
      val supported = hasLower && hasUpper
      baos.write(if (supported) 1 else 0)
      writeU32LE(baos, if (stats.isNullAt(base + 2)) 0 else stats.getInt(base + 2))
      writeU32LE(baos, if (stats.isNullAt(base + 3)) 0 else stats.getInt(base + 3))
      writeU64LE(baos, if (stats.isNullAt(base + 4)) 0L else stats.getLong(base + 4))
      if (supported) {
        // PA-6.A: dispatch by source-column dataType. PartitionStatistics.schema
        // emits 5 fields per source column (lowerBound, upperBound, nullCount,
        // count, sizeInBytes) where lowerBound/upperBound carry the SOURCE
        // column dataType (see ~/spark/.../ColumnStats.scala line 25-32).
        // schema==null => legacy BIGINT-only behavior (PA-3.2).
        val dt: org.apache.spark.sql.types.DataType =
          if (schema == null) org.apache.spark.sql.types.LongType else schema(base).dataType
        dt match {
          case org.apache.spark.sql.types.IntegerType =>
            // writeU32LE writes 4 LE bytes; signed Int has identical bit pattern.
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base))
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base + 1))
          case org.apache.spark.sql.types.LongType =>
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base))
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base + 1))
          case other =>
            throw new UnsupportedOperationException(
              s"PA-6.A serializeStats: dispatch for $other not implemented yet " +
                "(landing in PA-6.B/C/F/G or PA-7..PA-10)")
        }
      }
      col += 1
    }
    baos.toByteArray
  }

  // PA-6.0: schema parameter added for full-type dispatch (D-A5). PA-6.0 itself
  // remains BIGINT-only behavior (schema unused); PA-6.A onwards reads it.
  private[execution] def deserializeStats(
      blob: Array[Byte],
      schema: org.apache.spark.sql.types.StructType): InternalRow = {
    val buf = java.nio.ByteBuffer.wrap(blob).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val numCols = buf.getInt
    require(
      numCols >= 0 && numCols <= 4096,
      s"corrupt statsBlob: numCols=$numCols out of valid range [0, 4096]")
    val row = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(numCols * 5)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val supported = buf.get()
      val nullCount = buf.getInt
      val count = buf.getInt
      val sizeInBytes = buf.getLong
      if (supported == 1) {
        // PA-6.A: dispatch by source-column dataType. schema==null => legacy
        // BIGINT-only (PA-3.2). lowerLen on the wire still authoritative for
        // payload width; we cross-check against schema dispatch.
        val dt: org.apache.spark.sql.types.DataType =
          if (schema == null) org.apache.spark.sql.types.LongType else schema(base).dataType
        val lowerLen = buf.getInt
        dt match {
          case org.apache.spark.sql.types.IntegerType =>
            require(
              lowerLen == 4,
              s"PA-6.A IntegerType expects 4-byte lowerBound, got $lowerLen")
            row.update(base, buf.getInt)
            val upperLen = buf.getInt
            require(
              upperLen == 4,
              s"PA-6.A IntegerType expects 4-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getInt)
          case org.apache.spark.sql.types.LongType =>
            require(
              lowerLen == 8,
              s"PA-3.2/LongType expects 8-byte lowerBound, got $lowerLen")
            row.update(base, buf.getLong)
            val upperLen = buf.getInt
            require(
              upperLen == 8,
              s"PA-3.2/LongType expects 8-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getLong)
          case other =>
            throw new UnsupportedOperationException(
              s"PA-6.A deserializeStats: dispatch for $other not implemented yet " +
                "(landing in PA-6.B/C/F/G or PA-7..PA-10)")
        }
      }
      row.update(base + 2, nullCount)
      row.update(base + 3, count)
      row.update(base + 4, sizeInBytes)
      col += 1
    }
    row
  }

  private def writeU32LE(out: java.io.ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
    out.write((v >>> 16) & 0xff)
    out.write((v >>> 24) & 0xff)
  }

  private def writeU64LE(out: java.io.ByteArrayOutputStream, v: Long): Unit = {
    var i = 0
    while (i < 8) {
      out.write(((v >>> (8 * i)) & 0xffL).toInt)
      i += 1
    }
  }

  private def writeI64LE(out: java.io.ByteArrayOutputStream, v: Long): Unit =
    writeU64LE(out, v)

  /**
   * PA-3.3: Parse the JNI `serializeWithStats` framed return into (stats InternalRow, bytesBlob
   * Array[Byte]).
   *
   * Framed layout (matches cpp/velox/operators/serializer/ VeloxColumnarBatchSerializer.cc PA-2.5c
   * assembled return):
   *
   * [ V2_MAGIC: 4 bytes 0xFE 0xCA 0x53 0x02 ] [ statsLen: uint32 LE ] [ statsBlob: statsLen bytes ]
   * [ bytesLen: uint32 LE ] [ bytesBlob: bytesLen bytes ]
   *
   * Eager guards catch corrupt magic / truncated framing before they propagate into a malformed
   * CachedColumnarBatch.
   */
  private[execution] def parseFramedBytes(
      framed: Array[Byte],
      schema: org.apache.spark.sql.types.StructType): (InternalRow, Array[Byte]) = {
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
    val buf = java.nio.ByteBuffer.wrap(framed).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.position(4) // skip magic
    val statsLen = buf.getInt
    require(
      statsLen >= 0 && statsLen <= buf.remaining() - 4,
      s"framed bytes statsLen=$statsLen exceeds remaining buffer " +
        s"${buf.remaining() - 4} (truncated?)")
    val statsBlob = new Array[Byte](statsLen)
    buf.get(statsBlob)
    val stats = deserializeStats(statsBlob, schema)
    val bytesLen = buf.getInt
    require(
      bytesLen >= 0 && bytesLen <= buf.remaining(),
      s"framed bytes bytesLen=$bytesLen exceeds remaining buffer " +
        s"${buf.remaining()} (truncated?)")
    val bytesBlob = new Array[Byte](bytesLen)
    buf.get(bytesBlob)
    (stats, bytesBlob)
  }
}

// format: off
/**
 * Feature:
 * 1. This serializer supports column pruning
 * 2. TODO: support push down filter
 * 3. Super TODO: support store offheap object directly
 *
 * The data transformation pipeline:
 *
 *   - Serializer ColumnarBatch -> CachedColumnarBatch
 *     -> serialize to byte[]
 *
 *   - Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> deserialize to byte[] to create Velox ColumnarBatch
 *
 *   - Serializer InternalRow -> CachedColumnarBatch (support RowToColumnar)
 *     -> Convert InternalRow to ColumnarBatch
 *     -> Serializer ColumnarBatch -> CachedColumnarBatch
 *
 *   - Serializer InternalRow -> DefaultCachedBatch (unsupport RowToColumnar)
 *     -> Convert InternalRow to DefaultCachedBatch using vanilla Spark serializer
 *
 *   - Deserializer CachedColumnarBatch -> InternalRow (support ColumnarToRow)
 *     -> Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> Convert ColumnarBatch to InternalRow
 *
 *   - Deserializer DefaultCachedBatch -> InternalRow (unsupport ColumnarToRow)
 *     -> Convert DefaultCachedBatch to InternalRow using vanilla Spark serializer
 */
// format: on
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
              val structSchema = org.apache.spark.sql.types.StructType(
                schema.map(
                  a =>
                    org.apache.spark.sql.types.StructField(a.name, a.dataType, a.nullable)))
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
