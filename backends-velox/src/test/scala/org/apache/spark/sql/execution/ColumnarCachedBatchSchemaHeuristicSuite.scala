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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.columnar.SparkCacheUtil
import org.apache.spark.sql.functions.{col, lit, repeat}
import org.apache.spark.sql.types._

/**
 * Tests the schema-shape gate added on top of [[ColumnarCachedBatchSerializer]] (#3456 followup):
 * wide-string / wide-row schemas must fall back to Spark's `DefaultCachedBatchSerializer`,
 * numeric-friendly schemas must take the columnar path. Read and write sides must agree.
 */
class ColumnarCachedBatchSchemaHeuristicSuite
  extends VeloxWholeStageTransformerSuite {

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
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
      .set("spark.sql.shuffle.partitions", "4")
  }

  private def attrs(schema: StructType): Seq[AttributeReference] =
    schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

  private def withConf(pairs: (String, String)*)(body: => Unit): Unit = {
    val saved = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try body
    finally saved.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  test("numeric-only schema passes the schema-shape gate") {
    val ser = new ColumnarCachedBatchSerializer
    val schema = StructType(
      (0 until 16).map(i => StructField(s"c$i", LongType, nullable = false)))
    assert(
      ser.supportsColumnarInput(attrs(schema)),
      "numeric schema must take the columnar cache path")
    assert(
      ser.supportsColumnarOutput(schema),
      "numeric schema must take the columnar cache path (output)")
  }

  test("wide-string schema is rejected by the schema-shape gate (default settings)") {
    val ser = new ColumnarCachedBatchSerializer
    val schema = StructType(
      (0 until 16).map(i => StructField(s"s$i", StringType, nullable = false)))
    assert(
      !ser.supportsColumnarInput(attrs(schema)),
      "wide-string schema must fall back to row-based cache under default heuristics")
    assert(
      !ser.supportsColumnarOutput(schema),
      "wide-string schema must fall back to row-based cache under default heuristics (output)")
  }

  test("session override re-enables columnar cache for wide-string schema") {
    withConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_MAX_STRING_FRACTION.key -> "1.0",
      GlutenConfig.COLUMNAR_TABLE_CACHE_MAX_AVG_ROW_BYTES.key -> Long.MaxValue.toString
    ) {
      val ser = new ColumnarCachedBatchSerializer
      val schema = StructType(
        (0 until 16).map(i => StructField(s"s$i", StringType, nullable = false)))
      assert(
        ser.supportsColumnarInput(attrs(schema)),
        "wide-string schema must take columnar path when both gates are disabled")
    }
  }

  test("boundary: 50/50 string-vs-long schema passes the default 0.5 threshold") {
    val ser = new ColumnarCachedBatchSerializer
    val fields = (0 until 8).map(i => StructField(s"s$i", StringType)) ++
      (0 until 8).map(i => StructField(s"l$i", LongType))
    val schema = StructType(fields)
    assert(
      ser.supportsColumnarInput(attrs(schema)),
      "stringFraction exactly at 0.5 must pass (inclusive boundary)")
  }

  test("avg-row-bytes gate alone rejects a wide numeric schema") {
    withConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_MAX_AVG_ROW_BYTES.key -> "32"
    ) {
      val ser = new ColumnarCachedBatchSerializer
      // 16 longs * 8 bytes = 128 bytes > 32-byte threshold
      val schema = StructType(
        (0 until 16).map(i => StructField(s"c$i", LongType, nullable = false)))
      assert(
        !ser.supportsColumnarInput(attrs(schema)),
        "schema wider than maxAvgRowBytes must fall back")
    }
  }

  test("e2e: wide-string DataFrame caches + reads correctly under fallback") {
    val n = 1000L
    val cached = spark.range(n)
      .select(
        repeat(lit("xxxxxxxx"), 25).as("s0"),
        repeat(lit("yyyyyyyy"), 25).as("s1"),
        repeat(lit("zzzzzzzz"), 25).as("s2"),
        col("id").as("k"))
      .cache()
    try {
      assert(cached.count() == n)
      val matched = cached.filter(col("k") < lit(10)).count()
      assert(matched == 10L, s"expected 10, got $matched")
    } finally {
      cached.unpersist()
    }
  }
}
