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

// PA-2.4 RED: Decimal(P>=19) -> HUGEINT (int128_t) FlatVector min/max.
//
// Scope: a single LongDecimal(20, 4) FlatVector, values [100, -50, 9999].
// Expected stats[0]: hasLowerBound=true, hasUpperBound=true,
//                    lowerBound = -50 (as int128_t), upperBound = 9999.
//
// PA-2.3 (nulls-buffer-absent) is folded into PA-5 ship blocker UT batch
// (existing GREEN code already has `if (nulls != nullptr)` defensive guard;
// no honest RED can be authored for an already-correct path -- per Plan
// sec 0 ironclad rule 2 we do not fake "already green" RED tests).
//
// Expected RED failure: PA-2.2 GREEN's switch only covers BIGINT + REAL;
// HUGEINT lands in the default (unsupported) branch -> hasLowerBound=false.
// Test asserts EXPECT_TRUE(stats[0].hasLowerBound) which fails.
// PA-6.5 B4: HUGEINT case removed from computeStats — emit_gate excluded
// it anyway, leaving dead code. Now expected unsupported until PA-8 lands
// proper long-Decimal marshal.
TEST_F(VeloxColumnarBatchSerializerTest, PA_2_4_testComputeStatsHugeintDecimalFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int128_t>(
          {static_cast<int128_t>(100),
           static_cast<int128_t>(-50),
           static_cast<int128_t>(9999)},
          DECIMAL(20, 4)),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_FALSE(stats[0].hasLowerBound)
      << "PA-6.5 B4: HUGEINT (long Decimal P>=19) intentionally unsupported until PA-8";
  EXPECT_FALSE(stats[0].hasUpperBound);
}

// PA-2.5a RED: framedSerializeWithStats top-level layout.
//
// Helper returns std::vector<uint8_t> framed as:
//   [ magic(4) | statsLen(4 LE) | statsBlob (statsLen bytes) | bytesLen(4 LE) | bytesBlob (bytesLen bytes) ]
//
// magic = 0xFE 0xCA 0x53 0x02 (matches V2_MAGIC of JVM Kryo serializer; downstream
// JVM parses framed bytes with magic-prefix sniff that already exists in PA-1).
//
// PA-2.5a scope: framing layout + bytesBlob (delegated to existing serializer)
// + EMPTY statsBlob (statsLen = 0). Per-column stats marshaling is PA-2.5b.
//
// Expected RED failure: framedSerializeWithStats() does not exist yet ->
// compile error 'no member named framedSerializeWithStats'.
TEST_F(VeloxColumnarBatchSerializerTest, PA_2_5a_testFramedSerializeWithStatsLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  std::vector<uint8_t> framed = serializer->framedSerializeWithStats(batch);

  // Top-level layout: at least magic(4) + statsLen(4) + bytesLen(4) = 12 bytes header.
  ASSERT_GE(framed.size(), 12u);

  // Magic bytes 0xFE 0xCA 0x53 0x02.
  EXPECT_EQ(framed[0], static_cast<uint8_t>(0xFE));
  EXPECT_EQ(framed[1], static_cast<uint8_t>(0xCA));
  EXPECT_EQ(framed[2], static_cast<uint8_t>(0x53));
  EXPECT_EQ(framed[3], static_cast<uint8_t>(0x02));

  // statsLen LE int32 -- post PA-2.5b this is non-zero. PA-2.5a only asserts
  // the layout shape (offsets / framing), not the numeric statsLen value.
  uint32_t statsLen = static_cast<uint32_t>(framed[4]) |
                      (static_cast<uint32_t>(framed[5]) << 8) |
                      (static_cast<uint32_t>(framed[6]) << 16) |
                      (static_cast<uint32_t>(framed[7]) << 24);

  // bytesLen LE int32 immediately after empty statsBlob.
  size_t bytesLenOffset = 8u + statsLen;
  ASSERT_GE(framed.size(), bytesLenOffset + 4u);
  uint32_t bytesLen = static_cast<uint32_t>(framed[bytesLenOffset]) |
                      (static_cast<uint32_t>(framed[bytesLenOffset + 1]) << 8) |
                      (static_cast<uint32_t>(framed[bytesLenOffset + 2]) << 16) |
                      (static_cast<uint32_t>(framed[bytesLenOffset + 3]) << 24);
  EXPECT_GT(bytesLen, 0u) << "bytesBlob must contain serialized batch payload";
  EXPECT_EQ(framed.size(), bytesLenOffset + 4u + bytesLen)
      << "framed total size must match magic + statsLen + statsBlob + bytesLen + bytesBlob";
}

// PA-2.5b RED: statsBlob layout per docs/0003-jni-binary-framing-reference.md sec 3.
//
//   [ numCols: uint32 LE ]
//   per col:
//     [ supported: uint8 ]
//     [ nullCount: uint32 LE ]
//     [ count: uint32 LE ]                  <- = rowVector->size()
//     [ sizeInBytes: uint64 LE ]            <- 0 placeholder in PA-2.5b
//     if supported:
//       [ lowerBoundLen: uint32 LE ][ lowerBound bytes ]
//       [ upperBoundLen: uint32 LE ][ upperBound bytes ]
//
// PA-2.5b scope: 1-col BIGINT path only. lowerBound/upperBound = int64 LE 8 bytes.
// REAL/HUGEINT marshaling deferred to follow-up micro-slices (PA-2.5b-real /
// PA-2.5b-hugeint) -- one type per RED stays honest under Plan sec 0 rule 1.
//
// Expected RED failure: PA-2.5a writes statsLen=0 (empty statsBlob); the
// PA-2.5b assertions read numCols=1 from the first 4 bytes of statsBlob and
// will fail (statsBlob is empty -> ASSERT_GE(statsLen, 4u) fires).
TEST_F(VeloxColumnarBatchSerializerTest, PA_2_5b_testStatsBlobBigintLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({42, 7, 99, -3, 50}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  std::vector<uint8_t> framed = serializer->framedSerializeWithStats(batch);
  ASSERT_GE(framed.size(), 12u);

  // Skip magic(4) header; read statsLen LE.
  uint32_t statsLen = static_cast<uint32_t>(framed[4]) |
                      (static_cast<uint32_t>(framed[5]) << 8) |
                      (static_cast<uint32_t>(framed[6]) << 16) |
                      (static_cast<uint32_t>(framed[7]) << 24);
  ASSERT_GE(statsLen, 4u) << "PA-2.5b: statsBlob must contain at least numCols(uint32)";

  // statsBlob starts at offset 8.
  auto readU32LE = [&](size_t off) {
    return static_cast<uint32_t>(framed[off]) |
        (static_cast<uint32_t>(framed[off + 1]) << 8) |
        (static_cast<uint32_t>(framed[off + 2]) << 16) |
        (static_cast<uint32_t>(framed[off + 3]) << 24);
  };
  auto readU64LE = [&](size_t off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v |= static_cast<uint64_t>(framed[off + i]) << (8 * i);
    }
    return v;
  };
  auto readI64LE = [&](size_t off) { return static_cast<int64_t>(readU64LE(off)); };

  size_t off = 8;
  uint32_t numCols = readU32LE(off);
  off += 4;
  EXPECT_EQ(numCols, 1u);

  // Per-col header.
  uint8_t supported = framed[off];
  off += 1;
  EXPECT_EQ(supported, 1u) << "BIGINT no-null col must be supported";

  uint32_t nullCount = readU32LE(off);
  off += 4;
  EXPECT_EQ(nullCount, 0u);

  uint32_t count = readU32LE(off);
  off += 4;
  EXPECT_EQ(count, 5u) << "count = rowVector->size()";

  uint64_t sizeInBytes = readU64LE(off);
  off += 8;
  EXPECT_EQ(sizeInBytes, 0u) << "PA-2.5b uses 0 placeholder for sizeInBytes";

  // lowerBound: BIGINT -> 8 bytes int64 LE = -3.
  uint32_t lowerLen = readU32LE(off);
  off += 4;
  EXPECT_EQ(lowerLen, 8u);
  EXPECT_EQ(readI64LE(off), -3);
  off += lowerLen;

  // upperBound: 8 bytes int64 LE = 99.
  uint32_t upperLen = readU32LE(off);
  off += 4;
  EXPECT_EQ(upperLen, 8u);
  EXPECT_EQ(readI64LE(off), 99);
  off += upperLen;

  // statsBlob ends exactly at off (no trailing bytes within statsBlob).
  EXPECT_EQ(off, 8u + statsLen) << "statsBlob content must exactly match statsLen";
}

// PA-6.2.A.1 RED: INTEGER (int32) FlatVector min/max scan.
//
// Scope: Spark IntegerType / DateType / YearMonthIntervalType all map to Velox
// TypeKind::INTEGER (4-byte signed Int) per ~/repos/velox/velox/type/Type.h
// line 78. Required for D-A5 Layer A full-type alignment with vanilla
// SimpleMetricsCachedBatch (see ../../investigations/0007 §G1 integer family).
//
// Expected RED failure: PA-2.* GREEN switch covers BIGINT / REAL / HUGEINT
// only; INTEGER falls into the default branch -> hasLowerBound=false. Test
// asserts EXPECT_TRUE(stats[0].hasLowerBound) which fails.
TEST_F(VeloxColumnarBatchSerializerTest, PA_6_2_A_1_testComputeStatsIntegerFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int32_t>({100, -2147483, 2147483, 0, 42}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "INTEGER FlatVector must be supported after PA-6.2 GREEN";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int32_t>(), -2147483);
  EXPECT_EQ(stats[0].upperBound.value<int32_t>(), 2147483);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// PA-6.2.A.2 RED: SMALLINT (int16) FlatVector min/max scan.
// Spark ShortType -> Velox TypeKind::SMALLINT.
TEST_F(VeloxColumnarBatchSerializerTest, PA_6_2_A_2_testComputeStatsSmallintFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int16_t>({static_cast<int16_t>(-12345),
                               static_cast<int16_t>(0),
                               static_cast<int16_t>(12345)}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "SMALLINT FlatVector must be supported after PA-6.2 GREEN";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int16_t>(), static_cast<int16_t>(-12345));
  EXPECT_EQ(stats[0].upperBound.value<int16_t>(), static_cast<int16_t>(12345));
}

// PA-6.2.A.3 RED: TINYINT (int8) FlatVector min/max scan.
// Spark ByteType -> Velox TypeKind::TINYINT.
TEST_F(VeloxColumnarBatchSerializerTest, PA_6_2_A_3_testComputeStatsTinyintFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int8_t>({static_cast<int8_t>(-128),
                              static_cast<int8_t>(0),
                              static_cast<int8_t>(127)}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "TINYINT FlatVector must be supported after PA-6.2 GREEN";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int8_t>(), static_cast<int8_t>(-128));
  EXPECT_EQ(stats[0].upperBound.value<int8_t>(), static_cast<int8_t>(127));
}

} // namespace gluten
