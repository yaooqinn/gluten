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
import org.apache.spark.sql.catalyst.expressions.Attribute
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
    override val stats: InternalRow)
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
      val statsBytes = CachedColumnarBatchKryoSerializer.serializeStats(batch.stats)
      output.writeInt(statsBytes.length)
      output.writeBytes(statsBytes)
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
    val stats: InternalRow = if (hasStats) {
      val statsLen = input.readInt()
      val statsBytes = new Array[Byte](statsLen)
      input.readBytes(statsBytes)
      CachedColumnarBatchKryoSerializer.deserializeStats(statsBytes)
    } else {
      null
    }
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats)
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
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats = null)
  }
}

object CachedColumnarBatchKryoSerializer {
  // V2_MAGIC = 0xFE 0xCA 0x53 0x02. Cannot collide with v1 binary because v1's
  // first 4 bytes are numRows (Kryo fixed 4-byte BE) and any non-negative Int
  // < 2^31 has BE high byte in [0x00, 0x7F]. Magic high byte 0xFE > 0x7F is
  // structurally unreachable, so any v1 first byte is <= 0x7F < 0xFE -- disjoint.
  val V2_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte)

  // PA-1 placeholder: PA-1 write path always emits stats=null, so these helpers
  // are inert in PA-1. PA-3 will replace with statsBlob binary framing (per
  // contract 0002 sec 3 + reference 0003 sec 3-4 per-type marshal). Java serialization
  // is NOT a stable cross-version Spark Streaming checkpoint format and MUST
  // be removed before any code path can produce stats != null.
  private[execution] def serializeStats(stats: InternalRow): Array[Byte] = {
    val baos = new java.io.ByteArrayOutputStream()
    try {
      val oos = new java.io.ObjectOutputStream(baos)
      try {
        oos.writeObject(stats)
      } finally {
        oos.close()
      }
      baos.toByteArray
    } finally {
      baos.close()
    }
  }

  private[execution] def deserializeStats(bytes: Array[Byte]): InternalRow = {
    val bais = new java.io.ByteArrayInputStream(bytes)
    try {
      val ois = new java.io.ObjectInputStream(bais)
      try {
        ois.readObject().asInstanceOf[InternalRow]
      } finally {
        ois.close()
      }
    } finally {
      bais.close()
    }
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
            val unsafeBuffer = ColumnarBatchSerializerJniWrapper
              .create(
                Runtimes.contextInstance(
                  BackendsApiManager.getBackendName,
                  "ColumnarCachedBatchSerializer#serialize"))
              .serialize(ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch))
            val bytes = unsafeBuffer.toByteArray
            // PA-1: stats=null placeholder. PA-3 will replace with stats InternalRow
            // built from cpp computeStats output (via JNI serializeWithStats).
            CachedColumnarBatch(batch.numRows(), bytes.length, bytes, stats = null)
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

  // PA-3.1 RED: removed the pre-PA-3 TODO override `(_, it) => it`.
  // Base class SimpleMetricsCachedBatchSerializer.buildFilter is inherited as-is,
  // which NPEs on stats=null (v1 binary read path). PA-3.1 GREEN will add the
  // lazy-split iterator wrapper to fix.
}
