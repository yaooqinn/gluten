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
import org.apache.spark.sql.functions.col

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
      // Upper bound: at most ~2 partitions worth of rows survive prune.
      // Lower bound: at least 1 row (the pivot itself).
      val upperBound = (N / P) * 2
      assert(
        outRows >= 1 && outRows <= upperBound,
        s"numOutputRows=$outRows expected in [1, $upperBound] " +
          s"(N=$N, P=$P, full-scan would give $N)"
      )
    } finally {
      cached.unpersist()
    }
  }
}
