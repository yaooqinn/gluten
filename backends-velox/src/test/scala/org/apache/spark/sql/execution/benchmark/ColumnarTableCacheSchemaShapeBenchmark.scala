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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, repeat}
import org.apache.spark.storage.StorageLevel

/**
 * Benchmark to measure the schema-shape gate added in #12132 for the columnar table cache (root
 * cause: #3456). Two workloads are compared end-to-end:
 *   - W1 (numeric, 16x long) -- the schema-shape gate must pass; columnar cache is faster.
 *   - W2 (wide-string, 16x ~200-char) -- the schema-shape gate must reject; row-based cache must be
 *     picked, otherwise gluten-on is ~1.5x slower than gluten-off plus an O(GB) R2C warmup tax.
 *
 * Each workload runs three cases:
 *   - gluten-off : Spark's DefaultCachedBatchSerializer (baseline)
 *   - gluten-on, gate default : enabled; v1 heuristic picks the right serializer
 *   - gluten-on, gate disabled : enabled with maxStringFraction=1.0 (forces columnar) -- exercises
 *     the regression #12132 prevents.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 */
object ColumnarTableCacheSchemaShapeBenchmark extends SqlBasedBenchmark {
  private val numRows = 5L * 1000 * 1000
  private val numCols = 16
  private val stringLen = 200

  private val cacheEnabledKey = GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key
  private val maxStringFractionKey =
    GlutenConfig.COLUMNAR_TABLE_CACHE_MAX_STRING_FRACTION.key

  private def numericDf(): DataFrame = {
    val cols = (0 until numCols).map(i => col("id").cast("long").as(s"c$i"))
    spark.range(numRows).select(cols: _*)
  }

  private def wideStringDf(): DataFrame = {
    // 16 string columns of ~200 chars each => ~16 GB cache for 5M rows under columnar
    // serialization; smaller under DefaultCachedBatchSerializer's UnsafeRow compression.
    val cols = (0 until numCols).map {
      i => repeat(lit(("abcdefghij" * 20).substring(0, 10)), stringLen / 10).as(s"s$i")
    }
    spark.range(numRows).select(cols: _*)
  }

  private def withConfs(pairs: (String, String)*)(body: => Unit): Unit = {
    val saved = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try body
    finally saved.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private def runOne(df: => DataFrame, predicate: String): Unit = {
    spark.catalog.clearCache()
    val cached = df.persist(StorageLevel.MEMORY_ONLY)
    try {
      // First .count() materializes the cache -- write path cost is included in the
      // first measured iteration, steady-state reads dominate iterations 2..3.
      cached.count()
      cached.where(predicate).count()
    } finally {
      cached.unpersist()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val w1 = new Benchmark(
      s"W1 numeric ($numCols x long), $numRows rows",
      numRows,
      output = output)
    w1.addCase("gluten-off", 3) {
      _ =>
        withConfs(cacheEnabledKey -> "false")(
          runOne(numericDf(), "c0 < 1000"))
    }
    w1.addCase("gluten-on (gate default)", 3) {
      _ =>
        withConfs(cacheEnabledKey -> "true")(
          runOne(numericDf(), "c0 < 1000"))
    }
    w1.addCase("gluten-on (gate disabled)", 3) {
      _ =>
        withConfs(
          cacheEnabledKey -> "true",
          maxStringFractionKey -> "1.0"
        )(runOne(numericDf(), "c0 < 1000"))
    }
    w1.run()

    val w2 = new Benchmark(
      s"W2 wide-string ($numCols x ~$stringLen-char), $numRows rows",
      numRows,
      output = output)
    w2.addCase("gluten-off", 3) {
      _ =>
        withConfs(cacheEnabledKey -> "false")(
          runOne(wideStringDf(), "s0 like 'a%'"))
    }
    w2.addCase("gluten-on (gate default)", 3) {
      _ =>
        withConfs(cacheEnabledKey -> "true")(
          runOne(wideStringDf(), "s0 like 'a%'"))
    }
    w2.addCase("gluten-on (gate disabled, regression case)", 3) {
      _ =>
        withConfs(
          cacheEnabledKey -> "true",
          maxStringFractionKey -> "1.0"
        )(runOne(wideStringDf(), "s0 like 'a%'"))
    }
    w2.run()
  }
}
