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

import org.apache.spark.SparkFunSuite

/**
 * PA-4 default-off ship gate.
 *
 * Verifies the SQLConf exists, has the right key/default, and the doc text states the default-off
 * rationale. The write-path branch behavior (conf=true routes through serializeWithStats,
 * conf=false falls back to legacy serialize) is anchored end-to-end by ColumnarCachedBatchE2ESuite
 * under fork CI.
 *
 * Refs: todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-4
 */
class ColumnarCachePartitionStatsConfSuite extends SparkFunSuite {
  test("PA-4.A conf key and default-off") {
    val entry = GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED
    assert(entry.key == "spark.gluten.sql.columnar.tableCache.partitionStats.enabled")
    assert(entry.defaultValue.contains(false), "PA-4 ships default-off")
  }

  test("PA-4.B conf doc mentions default-off rationale") {
    val doc = GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.doc
    assert(
      doc.toLowerCase(java.util.Locale.ROOT).contains("default"),
      s"doc should mention default state: $doc")
    assert(
      doc.toLowerCase(java.util.Locale.ROOT).contains("benchmark") ||
        doc.toLowerCase(java.util.Locale.ROOT).contains("regression"),
      s"doc should mention why default-off (bench / regression): $doc"
    )
  }
}
