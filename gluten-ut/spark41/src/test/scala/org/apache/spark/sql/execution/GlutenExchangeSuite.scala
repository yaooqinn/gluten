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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

class GlutenExchangeSuite extends ExchangeSuite with GlutenSQLTestsBaseTrait {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.shuffle.partitions", "2")
  }

  testGluten("ColumnarShuffleExchangeExec canonicalizes to ShuffleExchangeExec type") {
    // Verify that ColumnarShuffleExchangeExec.canonicalized produces a ShuffleExchangeExec,
    // not a ColumnarShuffleExchangeExec. This is critical for AQE stage cache reuse:
    // the cache first-check uses ShuffleExchangeExec.canonicalized as key, so the canonical
    // form must be type-compatible.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    ) {
      val df = sql("SELECT key, value FROM testData JOIN testData2 ON key = a")
      val plan = df.queryExecution.executedPlan
      val columnarExchanges = plan.collect { case e: ColumnarShuffleExchangeExecBase => e }
      assert(columnarExchanges.nonEmpty, "Expected ColumnarShuffleExchangeExec in plan")
      columnarExchanges.foreach {
        exchange =>
          val canonicalized = exchange.canonicalized
          assert(
            canonicalized.isInstanceOf[ShuffleExchangeExec],
            s"ColumnarShuffleExchangeExec.canonicalized should be ShuffleExchangeExec, " +
              s"but was ${canonicalized.getClass.getSimpleName}"
          )
      }
    }
  }
}
