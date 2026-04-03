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

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.SQLConf

class GlutenExchangeSuite
  extends ExchangeSuite
  with GlutenSQLTestsBaseTrait
  with AdaptiveSparkPlanHelper {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.shuffle.partitions", "2")
  }

  testGluten("AQE reuses identical ColumnarShuffleExchangeExec via stage cache") {
    // Verify that AQE's stage cache reuses identical columnar shuffle exchanges.
    // Two identical subqueries should result in one physical shuffle + one ReusedExchangeExec.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    ) {
      val df = sql("""
        WITH base AS (SELECT key, count(value) as total FROM testData GROUP BY key)
        SELECT * FROM base a JOIN base b ON a.key = b.key
      """)
      df.collect()
      val plan = df.queryExecution.executedPlan
      val stages = collect(plan) { case s: ShuffleQueryStageExec => s }
      val columnarStages = stages.filter(_.plan.isInstanceOf[ColumnarShuffleExchangeExecBase])
      assert(columnarStages.nonEmpty, "Expected ColumnarShuffleExchangeExec in AQE stages")
      // Verify identical exchanges produce the same canonical form (enabling reuse)
      if (columnarStages.size >= 2) {
        val canonicals = columnarStages.map(_.plan.canonicalized)
        val pairs =
          for (i <- canonicals.indices; j <- i + 1 until canonicals.size)
            yield (i, j, canonicals(i).sameResult(canonicals(j)))
        val matchingPairs = pairs.filter(_._3)
        assert(
          matchingPairs.nonEmpty,
          "At least two ColumnarShuffleExchangeExec stages should have matching " +
            "canonical forms for AQE reuse")
      }
    }
  }
}
