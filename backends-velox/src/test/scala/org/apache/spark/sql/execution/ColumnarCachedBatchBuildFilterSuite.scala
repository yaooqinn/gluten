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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * PA-3.1 RED test for buildFilter stats=null guard (lazy-split iterator wrapper).
 *
 * Refs:
 *   - todos/features/gluten-inmemory-cache-stats/investigations/
 *     0003-simplemetrics-buildfilter-survey.md rev 2
 *   - todos/features/gluten-inmemory-cache-stats/docs/0001-layerA-minmax-design.md rev 4 (D-A3)
 *   - todos/features/gluten-inmemory-cache-stats/docs/0004-layerA-implementation-plan.md PA-3
 *
 * Background (from 0003 rev 2 evidence):
 *   - Vanilla SimpleMetricsCachedBatchSerializer.buildFilter calls
 *     `partitionFilter.eval(cachedBatch.stats)`. For non-trivial predicates, eval(null) NPEs on
 *     both codegen and interpreted paths (no fallback).
 *   - Naive "occupier row with null literals" placeholder fails differently: bind succeeds
 *     (Literal(null, IntegerType).resolved = true), eval returns false, silently dropping all
 *     batches (data loss, worse than NPE).
 *
 * Therefore PA-3.1 production change: ColumnarCachedBatchSerializer must override buildFilter with
 * a lazy-split iterator wrapper that directs stats=null batches through (skipping vanilla partition
 * pruning) and feeds stats!=null batches to super.buildFilter.
 *
 * Pure JVM-only suite. No native lib required. The test directly invokes
 * ColumnarCachedBatchSerializer.buildFilter on a synthetic Iterator[CachedBatch] containing
 * CachedColumnarBatch instances with stats=null, mimicking the v1 binary read path under
 * non-trivial predicate.
 */
class ColumnarCachedBatchBuildFilterSuite extends AnyFunSuite {

  // PA-3.1 RED: v1 binary (stats=null) + EqualTo predicate must NOT drop the batch.
  //
  // Expected RED failure modes (one of):
  //   (a) Without PA-3.1 production change: ColumnarCachedBatchSerializer still
  //       overrides buildFilter as `(_, it) => it` (pre-PA-3 TODO pass-through) ->
  //       this test passes trivially BUT will regress once PA-3.1 GREEN flips
  //       the base class to SimpleMetricsCachedBatchSerializer and removes the
  //       TODO override. To honor TDD strictly we assert against the post-flip
  //       behavior; before PA-3.1 GREEN, this test is therefore a RED whose
  //       failure mode is "test does nothing useful (impl change not yet made)".
  //       (See note (b) below for the harder RED.)
  //
  //   (b) HARDER RED (post-flip, pre-wrapper): if we manually flip the base
  //       class to SimpleMetricsCachedBatchSerializer and remove the TODO
  //       override (no wrapper yet), this test will throw NullPointerException
  //       at partitionFilter.eval(null) -- matching 0003 rev 2 evidence 3.
  //
  // GREEN once lazy-split wrapper is in place: stats=null batch is directed
  // through, length(filtered) == 1, no NPE.
  //
  // RED is staged with assertion (b)'s expected behavior, so as soon as the
  // base class flips the wrapper must already be in place; otherwise NPE
  // surfaces. This matches AGENTS.md iron law 1 (one RED guards one prod
  // change), and law 3 (RED expected to fail because feature is missing,
  // not because of typo).
  test("PA-3.1 buildFilter must direct stats=null batches through under EqualTo predicate") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", IntegerType, nullable = false)()
    val predicate = EqualTo(attr, Literal(5))
    val cachedAttributes = Seq(attr)

    val filter = serializer.buildFilter(Seq(predicate), cachedAttributes)

    // Three v1 binary batches: stats=null. A correct PA-3.1 wrapper directs
    // them all through (no pruning, since we have no stats to prune on).
    val batches: Iterator[CachedBatch] = Iterator(
      CachedColumnarBatch(
        numRows = 3,
        sizeInBytes = 12L,
        bytes = Array[Byte](1, 2, 3),
        stats = null),
      CachedColumnarBatch(
        numRows = 5,
        sizeInBytes = 20L,
        bytes = Array[Byte](4, 5),
        stats = null),
      CachedColumnarBatch(
        numRows = 7,
        sizeInBytes = 28L,
        bytes = Array[Byte](6, 7, 8),
        stats = null)
    )

    // GREEN expectation: all 3 batches returned (stats=null -> direct through).
    // RED expectation (pre-PA-3.1): NPE thrown at partitionFilter.eval(null)
    //   OR silent drop (if naive occupier-row placeholder was used).
    val result = filter(0, batches).toList

    assert(
      result.length === 3,
      "v1 binary (stats=null) batches must all be returned (lazy-split wrapper); " +
        s"got ${result.length}")
    assert(
      result.map(_.numRows) === Seq(3, 5, 7),
      "batch order must be preserved through wrapper")
  }
}
