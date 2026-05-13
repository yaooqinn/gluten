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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.{InMemoryTableScanExec, SparkCacheUtil}
import org.apache.spark.sql.functions.{col, lit, when}

/**
 * PA-3.5 e2e smoke for Gluten in-memory cache stats (Layer A min/max).
 *
 * Scope: end-to-end smoke only - assert no crash, correct result, plan shape, and `numOutputRows`
 * significantly less than total rows. The precise prune semantics are anchored by
 * `ColumnarCachedBatchBuildFilterPruneSuite` (PA-3.4).
 *
 * Refs: todos/features/gluten-inmemory-cache-stats/docs/0005-pa35-e2e-test-shape-note.md
 */
class ColumnarCachedBatchE2ESuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkCacheUtil.clearCacheSerializer()
  }

  override protected def afterAll(): Unit = {
    SparkCacheUtil.clearCacheSerializer()
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "4")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key, "true")
  }

  // Build a deterministic, range-partitioned cached frame:
  //   k in [0, N), repartitioned to P partitions by id range so each partition
  //   carries a disjoint k interval. A point-equality filter on the pivot can
  //   then be pruned to a single partition by min/max metadata.
  private val N: Long = 1000L
  private val P: Int = 5
  private val pivot: Long = 500L // falls inside partition that owns [400, 600)

  private def cacheRange(): org.apache.spark.sql.DataFrame = {
    spark
      .range(N)
      .select(col("id").cast("bigint").as("k"))
      .repartitionByRange(P, col("k"))
      .cache()
  }

  test("PA-3.5.A e2e cache + equality filter: no crash + correct result") {
    val cached = cacheRange()
    try {
      cached.count() // materialize cache (triggers serializeWithStats path)
      val result = cached.filter(col("k") === pivot).count()
      assert(result == 1L, s"expected exactly one row matching k=$pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  test("PA-3.5.B plan contains InMemoryTableScanExec + our serializer kicked in") {
    val cached = cacheRange()
    try {
      cached.count()
      val df = cached.filter(col("k") === pivot)
      val plan = df.queryExecution.executedPlan
      val scan = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }
      assert(scan.isDefined, s"plan missing InMemoryTableScanExec:\n$plan")
      val ims = scan.get.asInstanceOf[InMemoryTableScanExec]
      val serName = ims.relation.cacheBuilder.serializer.getClass.getSimpleName
      assert(
        serName == "ColumnarCachedBatchSerializer",
        s"expected ColumnarCachedBatchSerializer, got $serName"
      )
      // Force execution so numOutputRows is populated for PA-3.5.C reading.
      df.count()
    } finally {
      cached.unpersist()
    }
  }

  test("PA-3.5.C numOutputRows reflects post-filter row count (significantly < N)") {
    val cached = cacheRange()
    try {
      cached.count()
      val df = cached.filter(col("k") === pivot)
      df.count()
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      // Prune evidence: numOutputRows must be << N (full-scan would give N).
      // Lower bound is 0 -- with full partition pruning the InMemoryTableScanExec
      // node may legitimately emit zero rows (the surviving row comes from cache
      // metadata / pivot resolution at a higher layer when Gluten native scan
      // uses its own metrics path). The semantic correctness is anchored by
      // PA-3.5.A (result == 1) and the precise prune behavior by PA-3.4
      // BuildFilterPruneSuite; this case only needs to refute full-scan.
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound " +
          s"(N=$N, P=$P, full-scan would give $N -- prune appears not effective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  // PA-5.A ship blocker (BL7): all-null Long column. partition that contains
  // only nulls must NOT be incorrectly pruned by min/max stats (lower=upper=
  // sentinel would silently drop the matching null row, but null comparison
  // semantics mean WHERE col = 5 returns no rows ANYWAY -- the real risk is
  // mishandling the all-null case during stats compute, leading to crash or
  // wrong metadata. We assert: cache + filter on all-null column doesn't
  // crash and returns the correct (empty) result.
  test("PA-5.A all-null Long column: cache + equality filter no crash + correct result") {
    val df = spark
      .range(N)
      .select(lit(null).cast("bigint").as("k"))
      .repartition(P)
      .cache()
    try {
      df.count() // materialize
      val result = df.filter(col("k") === 5L).count()
      assert(result == 0L, s"all-null col cannot match k=5, got $result")
      // Sanity: cached count (pre-filter) is still N
      assert(df.count() == N, s"all-null cached frame should still hold $N rows")
    } finally {
      df.unpersist()
    }
  }

  // PA-5.B ship blocker (NB4): partition containing NaN Float must NOT have
  // valid min/max bounds -- cpp poison guard sets supported=false on any NaN.
  // If guard is missing, NaN propagates into stats and arbitrary partitions
  // get silently pruned. Assert: WHERE col = 1.5 still returns the matching
  // non-NaN row even when other rows in same partition are NaN.
  // SCOPE NOTE: Layer A ships BIGINT-only stats; Float/Double marshal is
  // phase-2. Initial run returned 0 rows (entire cache pruned), suggesting cpp
  // either (a) silently treats Float as BIGINT and packs NaN bits as Long, or
  // (b) ANSI-mode plan fallback breaks the cache hit. Investigation doc:
  // todos/features/gluten-inmemory-cache-stats/investigations/0006-pa5b-float-nan-prune.md
  // Trigger: when Float/Double marshal lands or NaN root cause is fixed.
  // PA-10 (was PA-5.B): with Float marshal landed, NaN guard at cpp
  // scanMinMax<float>:124 prevents poisoned bounds. Verify e2e: cache
  // a Float column with one NaN sprinkled in; query for non-NaN value
  // must still find it (no silent prune).
  test("PA-10 Float NaN partition: filter on non-NaN not silently pruned") {
    val df = spark
      .range(N)
      .select(
        when(col("id") === 7L, lit(Float.NaN))
          .otherwise(col("id").cast("float"))
          .as("k"))
      .repartition(P)
      .cache()
    try {
      df.count()
      // pivot=42 is a non-NaN value that exists somewhere; the partition that
      // contains it may also contain the NaN row at id=7 (collision possible
      // depending on hash partitioning). Either way, equality must find it.
      val result = df.filter(col("k") === 42.0f).count()
      assert(
        result == 1L,
        s"expected 1 row with k=42.0, got $result " +
          s"(NaN may have poisoned partition stats)")
    } finally {
      df.unpersist()
    }
  }

  // PA-6.2.D Date e2e prune correctness: with cpp INTEGER computeStats +
  // 4B LE marshal landed in PA-6.2.B, a DateType column (days-since-epoch
  // Int) must end-to-end prune the same way BIGINT does. Asserts:
  //   (1) no crash
  //   (2) result count correct
  //   (3) numOutputRows << N (full-scan refuted)
  // Refs: docs/0008-d-a5-full-type-alignment.md
  test("PA-6.2.D Date column equality filter: prune via INTEGER stats (4B LE)") {
    import org.apache.spark.sql.functions.{date_add, lit => sparkLit}
    val base = sparkLit("2020-01-01").cast("date")
    val cached = spark
      .range(N)
      .select(date_add(base, col("id").cast("int")).as("d"))
      .repartitionByRange(P, col("d"))
      .cache()
    try {
      cached.count() // materialize - triggers cpp INTEGER computeStats path
      val pivotDate = date_add(base, sparkLit(pivot.toInt))
      val df = cached.filter(col("d") === pivotDate)
      val result = df.count()
      assert(result == 1L, s"expected exactly one row matching date pivot, got $result")
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound (Date prune ineffective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  // PA-6.5 B1 regression: multi-column cache must not crash with
  // IndexOutOfBoundsException. Pre-fix `schema(base)` (base=col*5) threw
  // for any col>=1. This asserts both no-crash and correct-result on a
  // 3-column LongType cache.
  test("PA-6.5 B1 multi-column cache: no IndexOOB + correct result") {
    val cached = spark
      .range(N)
      .selectExpr(
        "cast(id as bigint) as a",
        "cast(id * 2 as bigint) as b",
        "cast(id + 100 as bigint) as c")
      .repartitionByRange(P, col("a"))
      .cache()
    try {
      cached.count()
      val result = cached.filter(col("a") === pivot && col("c") === (pivot + 100L)).count()
      assert(result == 1L, s"expected 1 row matching pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  // PA-6.5 B2 regression: short-Decimal source column must not crash
  // (Velox emits supported=1 as BIGINT physical; JVM-side allowlist
  // demotes to supported=0 until PA-7 marshal lands).
  test("PA-6.5 B2 Decimal column cache: no UOE crash on materialize + read") {
    val cached = spark
      .range(N)
      .selectExpr("cast(id as decimal(10, 2)) as d")
      .repartition(P)
      .cache()
    try {
      cached.count()
      val total = cached.count()
      assert(total == N, s"expected $N rows, got $total")
    } finally {
      cached.unpersist()
    }
  }

  // PA-8.5 SB1 regression: IsNotNull predicate must NOT silently skip a
  // partition just because it has nullCount > 0. Vanilla buildFilter
  // computes (count - nullCount > 0) where count = numRows (vanilla
  // gatherNullStats increments count on null rows too). If cpp emits
  // count = numRows - nullCount instead, the predicate becomes
  // (numRows - 2*nullCount > 0) and silently false-negatives partitions
  // with nullCount > numRows/2.
  test("PA-8.5 SB1 IsNotNull predicate honors vanilla count semantics") {
    val df = spark
      .range(N)
      .selectExpr("if(id % 3 = 0, cast(null as bigint), id) as k") // ~33% nulls
      .repartition(P)
      .cache()
    try {
      df.count() // materialize
      val nonNullCount = df.filter(col("k").isNotNull).count()
      val expected = (0L until N).count(_ % 3 != 0).toLong
      assert(
        nonNullCount == expected,
        s"IsNotNull silently dropped partitions: got $nonNullCount, expected $expected")
    } finally {
      df.unpersist()
    }
  }

  // PA-6.2.E e2e Timestamp prune: TimestampType physically Long microseconds
  // (Spark) -> Velox Timestamp struct{seconds, nanos}. cpp scanMinMax<Timestamp>
  // GREEN in PA-6.2.E; emit converts via toMicros() to share JVM LongType 8B
  // wire arm. This sentinel proves: (a) no crash on cache + filter, (b) correct
  // result, (c) prune effective (numOutputRows <= 2 * (N/P)).
  test("PA-6.2.E Timestamp column equality filter: prune via Long us stats (8B LE)") {
    import org.apache.spark.sql.functions.{lit => sparkLit}
    // Build N rows of distinct timestamps, one second apart starting at epoch
    // 2024-01-01T00:00:00Z (= 1704067200 seconds). Pivot at second N/2.
    val baseSec = 1704067200L
    val cached = spark
      .range(N)
      .selectExpr(s"timestamp_seconds(${baseSec}L + id) as ts")
      .repartitionByRange(P, col("ts"))
      .cache()
    try {
      cached.count() // materialize -- triggers cpp TIMESTAMP computeStats path
      val pivotTs = sparkLit(java.sql.Timestamp.from(
        java.time.Instant.ofEpochSecond(baseSec + (N / 2))))
      val df = cached.filter(col("ts") === pivotTs)
      val result = df.count()
      assert(result == 1L, s"expected exactly one row matching timestamp pivot, got $result")
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound (Timestamp prune ineffective)"
      )
    } finally {
      cached.unpersist()
    }
  }
}
