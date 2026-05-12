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

#include <gtest/gtest.h>

#include "../utils/VeloxArrowUtils.h"
#include "compute/VeloxBackend.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"

#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <arrow/buffer.h>

#include <limits>

using namespace facebook::velox;

namespace gluten {

class VeloxColumnarBatchSerializerTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }
};

TEST_F(VeloxColumnarBatchSerializerTest, serialize) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({-0.1234567, std::nullopt, 0.1234567, std::nullopt, -0.142857}),
      makeNullableFlatVector<bool>({std::nullopt, true, false, std::nullopt, true}),
      makeFlatVector<StringView>({"alice0", "bob1", "alice2", "bob3", "Alice4uuudeuhdhfudhfudhfudhbvudubvudfvu"}),
      makeNullableFlatVector<StringView>({"alice", "bob", std::nullopt, std::nullopt, "Alice"}),
      makeNullableFlatVector<int64_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(12, 4)),
      makeNullableFlatVector<int128_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(20, 4)),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  serializer->append(batch);
  std::shared_ptr<arrow::Buffer> buffer;
  GLUTEN_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(serializer->maxSerializedSize(), arrowPool));
  serializer->serializeTo(reinterpret_cast<uint8_t*>(buffer->mutable_address()), buffer->size());

  ArrowSchema cSchema;
  exportToArrow(vector, cSchema, ArrowUtils::getBridgeOptions());
  auto deserializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, &cSchema);
  auto deserialized = deserializer->deserialize(const_cast<uint8_t*>(buffer->data()), buffer->size());
  auto deserializedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(deserialized)->getRowVector();
  test::assertEqualVectors(vector, deserializedVector);
}

// PA-2.1 RED: computeStats() Long FlatVector min/max scan.
//
// Scope: a single Long FlatVector column (no nulls), values [42, 7, 99, -3, 50].
// Expected stats[0]: hasLowerBound=true, hasUpperBound=true,
//                    lowerBound=-3, upperBound=99, nullCount=0.
//
// Expected RED failure: VeloxColumnarBatchSerializer::computeStats() does not
// exist yet -- compilation fails with:
//   error: 'class gluten::VeloxColumnarBatchSerializer' has no member named 'computeStats'
//
// PA-2 GREEN will add ColumnStats struct + computeStats(rowVector) member that
// runs DecodedVector scan and fills per-column stats. This RED locks the
// minimal contract before adding NaN / nulls-buffer-absent / Decimal cases.
TEST_F(VeloxColumnarBatchSerializerTest, PA_2_1_testComputeStatsLongFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({42, 7, 99, -3, 50}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  auto stats = serializer->computeStats(vector);

  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound);
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int64_t>(), -3);
  EXPECT_EQ(stats[0].upperBound.value<int64_t>(), 99);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// PA-2.2 RED: NaN Float partition must NOT participate in pruning.
//
// Scope: a single REAL (float) FlatVector with one NaN value. Per Spark
// equality semantics, NaN != NaN, so a partition containing any NaN must
// emit hasLowerBound=hasUpperBound=false (graceful unsupported -- buildFilter
// pass-through for that column). This is NB4 ship blocker invariant.
//
// Expected RED failure: PA-2.1 GREEN ONLY handles BIGINT; REAL falls through
// to the unsupported branch -- so hasLowerBound=false is already TRUE for
// any REAL column. We must DISTINGUISH the new NaN-poisoned semantics from
// the trivial "REAL not yet supported" pass-through. The RED locks REAL with
// NO NaN values returns hasLowerBound=true (i.e. REAL becomes supported),
// AND NaN poisoning re-disables it. PA-2.2 GREEN adds REAL scan + NaN guard.
//
// Expected RED compile failure: REAL scan path not implemented, so case (a)
// (REAL no NaN) fails the EXPECT_TRUE assertion (actual=false != expected=true).
TEST_F(VeloxColumnarBatchSerializerTest, PA_2_2_testComputeStatsNaNFloatPartition) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // (a) REAL FlatVector NO NaN -- min/max well-defined, should be supported.
  {
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({1.5f, 2.5f, 0.5f, 3.5f}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_TRUE(stats[0].hasLowerBound)
        << "REAL FlatVector w/o NaN must be supported after PA-2.2 GREEN";
    EXPECT_TRUE(stats[0].hasUpperBound);
  }

  // (b) REAL FlatVector WITH NaN -- must fall back to unsupported.
  {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({1.5f, nan, 3.5f}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_FALSE(stats[0].hasLowerBound)
        << "NaN-poisoned REAL column must emit hasLowerBound=false (NB4)";
    EXPECT_FALSE(stats[0].hasUpperBound);
  }
}

} // namespace gluten
