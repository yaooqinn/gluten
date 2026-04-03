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

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  AdaptiveSparkPlanHelper,
  BroadcastQueryStageExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.exchange.{
  Exchange,
  ReusedExchangeExec,
  ShuffleExchangeExec
}
import org.apache.spark.sql.internal.SQLConf

class GlutenReuseExchangeAndSubquerySuite
  extends ReuseExchangeAndSubquerySuite
  with GlutenSQLTestsBaseTrait
  with AdaptiveSparkPlanHelper {

  // ---------------------------------------------------------------
  // Test 1: Basic shuffle exchange reuse with identical subplans
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: identical subplans should produce " +
      "ReusedExchangeExec") {
    // Two identical aggregations under UNION ALL should share
    // the same shuffle exchange via ReusedExchangeExec.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val df = sql("""
        |SELECT count(*) FROM (
        |  SELECT key FROM testData GROUP BY key
        |  UNION ALL
        |  SELECT key FROM testData GROUP BY key
        |)
      """.stripMargin)

      val plan = df.queryExecution.executedPlan
      val exchanges = plan.collectWithSubqueries {
        case e: Exchange => e
      }
      val reused = plan.collectWithSubqueries {
        case r: ReusedExchangeExec => r
      }

      // With reuse working, we should see at least one
      // ReusedExchangeExec wrapping a ColumnarShuffleExchangeExec
      assert(
        reused.nonEmpty,
        "Expected ReusedExchangeExec for identical " +
          "subplans under UNION ALL.\n" +
          s"Exchanges found: ${exchanges.size}\n" +
          s"Plan:\n${plan.treeString}")

      // Verify the reused exchange wraps a columnar exchange
      reused.foreach { r =>
        assert(
          r.child.isInstanceOf[ColumnarShuffleExchangeExec],
          "ReusedExchangeExec should wrap a " +
            "ColumnarShuffleExchangeExec, but wraps: " +
            s"${r.child.getClass.getSimpleName}")
      }
    }
  }

  // ---------------------------------------------------------------
  // Test 2: Shuffle exchange reuse count matches vanilla Spark
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: reuse count should match " +
      "vanilla Spark") {
    // A self-join of a CTE pipeline creates reuse opportunities.
    // Gluten should produce at least as many ReusedExchangeExec
    // nodes as vanilla Spark.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val query =
        """
          |WITH pipeline AS (
          |  SELECT key, count(value) as cnt
          |  FROM testData GROUP BY key
          |)
          |SELECT * FROM pipeline p1
          |JOIN pipeline p2 ON p1.key = p2.key
        """.stripMargin

      // Count reuse with Gluten (current session)
      val glutenPlan =
        sql(query).queryExecution.executedPlan
      val glutenReused =
        glutenPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }

      // Count reuse with vanilla Spark
      val vanillaReused = withGlutenDisabled {
        val vanillaPlan =
          sql(query).queryExecution.executedPlan
        vanillaPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
      }

      assert(
        glutenReused.size >= vanillaReused.size,
        s"Gluten ReusedExchange count " +
          s"(${glutenReused.size}) should be >= " +
          s"vanilla Spark (${vanillaReused.size}).\n" +
          s"Gluten plan:\n" +
          s"${glutenPlan.treeString}")
    }
  }

  // ---------------------------------------------------------------
  // Test 3: ColumnarShuffleExchangeExec canonicalization
  //         should produce vanilla-compatible type
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: ColumnarShuffleExchangeExec " +
      "canonicalized should equal " +
      "ShuffleExchangeExec canonicalized") {
    // The AQE stage cache first-check uses
    // ShuffleExchangeExec.canonicalized as lookup key, but
    // the cache is populated with
    // ColumnarShuffleExchangeExec.canonicalized. For the
    // first-check to succeed, canonicalization of a
    // ColumnarShuffleExchangeExec must produce a form that
    // equals the ShuffleExchangeExec canonical form.
    //
    // This test FAILS on current code (no doCanonicalize
    // override) and PASSES after the fix.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val df = sql(
        """
          |SELECT key, count(value)
          |FROM testData GROUP BY key
        """.stripMargin)

      val plan = df.queryExecution.executedPlan
      val columnarExchanges =
        plan.collectWithSubqueries {
          case e: ColumnarShuffleExchangeExec => e
        }

      assert(
        columnarExchanges.nonEmpty,
        "Expected ColumnarShuffleExchangeExec " +
          "in the plan")

      // Build the equivalent vanilla ShuffleExchangeExec
      // for the same query with Gluten disabled
      val vanillaExchanges = withGlutenDisabled {
        val vdf = sql(
          """
            |SELECT key, count(value)
            |FROM testData GROUP BY key
          """.stripMargin)
        vdf.queryExecution.executedPlan
          .collectWithSubqueries {
            case e: ShuffleExchangeExec => e
          }
      }

      assert(
        vanillaExchanges.nonEmpty,
        "Expected ShuffleExchangeExec in " +
          "vanilla plan")

      // The canonical form of a
      // ColumnarShuffleExchangeExec should be equal to
      // the canonical form of the corresponding
      // ShuffleExchangeExec. Without doCanonicalize
      // override, the canonical form retains the
      // ColumnarShuffleExchangeExec class type, which
      // never equals ShuffleExchangeExec.
      val columnarCanon =
        columnarExchanges.head.canonicalized
      val vanillaCanon =
        vanillaExchanges.head.canonicalized

      assert(
        columnarCanon == vanillaCanon,
        "ColumnarShuffleExchangeExec.canonicalized " +
          "should equal " +
          "ShuffleExchangeExec.canonicalized " +
          "for AQE stage cache compatibility.\n" +
          s"Columnar canonical type: " +
          s"${columnarCanon.getClass.getSimpleName}\n" +
          s"Vanilla canonical type: " +
          s"${vanillaCanon.getClass.getSimpleName}")
    }
  }

  // ---------------------------------------------------------------
  // Test 3b: Two ColumnarShuffleExchangeExec instances
  //          should canonicalize identically
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: equivalent " +
      "ColumnarShuffleExchangeExec should " +
      "canonicalize identically") {
    // Two structurally identical
    // ColumnarShuffleExchangeExec nodes should produce
    // the same canonicalized form (same-type reuse).
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val df = sql(
        """
          |SELECT
          |  (SELECT max(a.key)
          |   FROM testData AS a
          |   JOIN testData AS b
          |   ON b.key = a.key),
          |  a.key
          |FROM testData AS a
          |JOIN testData AS b ON b.key = a.key
        """.stripMargin)

      val plan = df.queryExecution.executedPlan
      val columnarExchanges =
        plan.collectWithSubqueries {
          case e: ColumnarShuffleExchangeExec => e
        }

      assert(
        columnarExchanges.nonEmpty,
        "Expected ColumnarShuffleExchangeExec " +
          "nodes in the plan")

      // Exchanges with equivalent children should
      // produce identical canonical forms
      val canonicals =
        columnarExchanges.map(_.canonicalized)
      val groups =
        canonicals.groupBy(identity).values.toSeq
      if (columnarExchanges.size > 1) {
        assert(
          groups.size < columnarExchanges.size,
          "Expected some " +
            "ColumnarShuffleExchangeExec to have " +
            "identical canonical forms.\n" +
            s"${columnarExchanges.size} exchanges, " +
            s"${groups.size} distinct forms.\n" +
            s"Plan:\n${plan.treeString}")
      }
    }
  }

  // ---------------------------------------------------------------
  // Test 4: AQE stage cache hit with ColumnarShuffleExchangeExec
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: AQE stage cache should " +
      "reuse equivalent columnar shuffle exchanges") {
    // With AQE enabled, the stage cache should find reuse
    // opportunities for equivalent columnar exchanges.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val query =
        """
          |SELECT count(*) FROM (
          |  SELECT key FROM testData GROUP BY key
          |  UNION ALL
          |  SELECT key FROM testData GROUP BY key
          |)
        """.stripMargin
      val df = sql(query)
      df.collect() // Force AQE execution

      val finalPlan =
        df.queryExecution.executedPlan
      assert(
        finalPlan.toString.startsWith(
          "AdaptiveSparkPlan isFinalPlan=true"),
        "Expected AQE final plan")

      val adaptivePlan = finalPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
        .executedPlan

      // In the AQE final plan, reused exchanges appear
      // inside ShuffleQueryStageExec wrappers
      val reusedInStages = collectWithSubqueries(
        adaptivePlan) {
        case ShuffleQueryStageExec(
              _,
              r: ReusedExchangeExec,
              _) =>
          r
      }

      assert(
        reusedInStages.nonEmpty,
        "Expected AQE stage cache to reuse " +
          "equivalent ColumnarShuffleExchangeExec " +
          "nodes.\nFinal plan:\n" +
          s"${adaptivePlan.treeString}")
    }
  }

  // ---------------------------------------------------------------
  // Test 5: AQE reuse count should match vanilla Spark
  // ---------------------------------------------------------------
  testGluten(
    "GLUTEN-REUSE: AQE reuse count should " +
      "match vanilla Spark") {
    // Compare the number of ReusedExchangeExec nodes in
    // AQE plans between Gluten-enabled and vanilla Spark.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val query =
        """
          |SELECT
          |  (SELECT max(a.key)
          |   FROM testData AS a
          |   JOIN testData AS b ON b.key = a.key),
          |  a.key
          |FROM testData AS a
          |JOIN testData AS b ON b.key = a.key
        """.stripMargin

      // Count reuse with Gluten + AQE
      val glutenDf = sql(query)
      glutenDf.collect()
      val glutenFinalPlan = glutenDf.queryExecution
        .executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
        .executedPlan
      val glutenReused =
        collectWithSubqueries(glutenFinalPlan) {
          case ShuffleQueryStageExec(
                _,
                r: ReusedExchangeExec,
                _) =>
            r
          case BroadcastQueryStageExec(
                _,
                r: ReusedExchangeExec,
                _) =>
            r
        }

      // Count reuse with vanilla Spark + AQE
      val vanillaReused = withGlutenDisabled {
        val vanillaDf = sql(query)
        vanillaDf.collect()
        val vanillaFinalPlan = vanillaDf.queryExecution
          .executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]
          .executedPlan
        collectWithSubqueries(vanillaFinalPlan) {
          case ShuffleQueryStageExec(
                _,
                r: ReusedExchangeExec,
                _) =>
            r
          case BroadcastQueryStageExec(
                _,
                r: ReusedExchangeExec,
                _) =>
            r
        }
      }

      assert(
        glutenReused.size >= vanillaReused.size,
        s"Gluten AQE ReusedExchange count " +
          s"(${glutenReused.size}) should be >= " +
          s"vanilla Spark " +
          s"(${vanillaReused.size}).\n" +
          s"Gluten plan:\n" +
          s"${glutenFinalPlan.treeString}")
    }
  }

  /**
   * Execute a block with Gluten disabled to get vanilla
   * Spark behavior for comparison.
   */
  private def withGlutenDisabled[T](f: => T): T = {
    var result: Option[T] = None
    withSQLConf(
      "spark.gluten.enabled" -> "false"
    ) {
      result = Some(f)
    }
    result.get
  }
}
