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
// PA-8: HUGEINT (long-Decimal precision > 18) restored — 16B LE marshal.
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
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "PA-8: HUGEINT (long Decimal P>=19) supported via 16B LE marshal";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int128_t>(), static_cast<int128_t>(-50));
  EXPECT_EQ(stats[0].upperBound.value<int128_t>(), static_cast<int128_t>(9999));
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

// PA-6.2.E RED: TIMESTAMP FlatVector min/max scan.
// Spark TimestampType / TimestampNTZType physically = Long microseconds since epoch.
// Velox Timestamp is a struct{int64_t seconds, uint64_t nanos} with defaulted
// operator<=> (Timestamp.h:377-378), so scanMinMax<Timestamp> compiles via
// existing template. Variant<Timestamp> is first-class
// (Variant.h:259, VELOX_VARIANT_SCALAR_CONSTRUCTOR(TIMESTAMP)).
// JVM marshal already lands in PA-6 (LongType path); cpp emit converts via
// Timestamp::toMicros() -> int64_t for wire compatibility.
TEST_F(VeloxColumnarBatchSerializerTest, PA_6_2_E_testComputeStatsTimestampFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Three timestamps chosen so min and max are distinct and order-clear:
  //   t1 = 2000-01-01T00:00:00 (sec= 946684800, nanos=0)
  //   t2 = 2024-01-01T00:00:00 (sec=1704067200, nanos=0)
  //   t3 = 2026-05-13T00:00:00 (sec=1778198400, nanos=0)
  Timestamp t1(946684800, 0);
  Timestamp t2(1704067200, 0);
  Timestamp t3(1778198400, 0);
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({t2, t1, t3})};
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "TIMESTAMP FlatVector must be supported after PA-6.2.E GREEN";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<Timestamp>(), t1);
  EXPECT_EQ(stats[0].upperBound.value<Timestamp>(), t3);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// PA-6.2.E.2 RED: TIMESTAMP wire marshaling via framedSerializeWithStats.
// Wire shape: lowerLen=8, payload = LE int64 microseconds (matches JVM
// LongType path), aligning Spark TimestampType physical = Long us.
TEST_F(VeloxColumnarBatchSerializerTest, PA_6_2_E_2_testFramedSerializeWithStatsTimestamp) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  Timestamp t1(946684800, 0);   // 2000-01-01
  Timestamp t3(1778198400, 0);  // 2026-05-13
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({t1, t3})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  // Skip MAGIC (4) + statsLen (4); statsBlob starts at offset 8.
  // statsBlob = [numCols u32 LE | supported u8 | nullCount u32 | count u32 |
  //              sizeInBytes u64 | lowerLen u32 LE | lower bytes |
  //              upperLen u32 LE | upper bytes]
  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  // numCols
  EXPECT_EQ(p[0], 1u);
  EXPECT_EQ(p[1], 0u);
  EXPECT_EQ(p[2], 0u);
  EXPECT_EQ(p[3], 0u);
  p += 4;
  // supported
  EXPECT_EQ(p[0], 1u) << "TIMESTAMP column should emit supported=1";
  p += 1;
  // nullCount + count + sizeInBytes
  p += 4 + 4 + 8;
  // lowerLen must be 8 (microseconds Long), matching JVM LongType path.
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 8u) << "TIMESTAMP wire lowerLen must be 8B (microseconds)";
  p += 4;
  // Read int64 LE microseconds; expect t1.toMicros().
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i) {
    loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  EXPECT_EQ(loMicros, t1.toMicros())
      << "TIMESTAMP wire lowerBound microseconds mismatch";
}

// PA-9 RED: VARCHAR FlatVector min/max scan via StringView.
// StringView::compare() uses memcmp -> unsigned byte ordering, matching
// Spark ByteArray.compareBinary which uses Byte.toUnsignedInt + 0xFF
// (ByteArray.java:67-90). variant(std::string) takes ownership so the
// StringView's pointer-into-FlatVector-buffer lifetime is not a concern
// after computeStats returns (Variant.h:232 heap-allocates new std::string).
TEST_F(VeloxColumnarBatchSerializerTest, PA_9_testComputeStatsVarcharFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Test inputs include high-bit byte (0xC2) to confirm unsigned comparison
  // (signed cmp would put 0xC2 < 'a' = 0x61, wrong).
  std::vector<VectorPtr> children = {
      makeFlatVector<StringView>({StringView("apple"),
                                  StringView("banana"),
                                  StringView("\xc2\xa9" "copy")})};
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound)
      << "VARCHAR FlatVector must be supported after PA-9 GREEN";
  EXPECT_TRUE(stats[0].hasUpperBound);
  // Unsigned byte order: "apple"(0x61) < "banana"(0x62) < "\xc2\xa9copy"(0xc2).
  EXPECT_EQ(stats[0].lowerBound.value<TypeKind::VARCHAR>(), std::string("apple"));
  EXPECT_EQ(stats[0].upperBound.value<TypeKind::VARCHAR>(),
            std::string("\xc2\xa9" "copy"));
  EXPECT_EQ(stats[0].nullCount, 0);
}

// PA-9.2 RED: VARCHAR wire marshaling via framedSerializeWithStats.
// Wire shape per docs/0008 sec 3.1: lowerLen u32 LE + N bytes raw UTF-8.
// cpp does NOT truncate (JVM truncates to 256B on read), so cpp wire is
// the full byte sequence.
TEST_F(VeloxColumnarBatchSerializerTest, PA_9_2_testFramedSerializeWithStatsVarchar) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<StringView>({StringView("apple"), StringView("banana")})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  // Skip MAGIC (4) + statsLen (4); statsBlob starts at offset 8.
  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  // numCols = 1
  EXPECT_EQ(p[0], 1u);
  p += 4;
  // supported = 1
  EXPECT_EQ(p[0], 1u) << "VARCHAR column should emit supported=1";
  p += 1;
  // skip nullCount + count + sizeInBytes (4 + 4 + 8)
  p += 4 + 4 + 8;
  // lowerLen = 5 (apple)
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 5u) << "VARCHAR lowerLen should be 5 for 'apple'";
  p += 4;
  std::string lower(reinterpret_cast<const char*>(p), 5);
  EXPECT_EQ(lower, "apple") << "VARCHAR lowerBound bytes mismatch";
  p += 5;
  // upperLen = 6 (banana)
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 6u) << "VARCHAR upperLen should be 6 for 'banana'";
  p += 4;
  std::string upper(reinterpret_cast<const char*>(p), 6);
  EXPECT_EQ(upper, "banana") << "VARCHAR upperBound bytes mismatch";
}

// CB-1 RED: TIMESTAMP with sub-microsecond nanos must NOT silently floor
// the upper bound (which would shrink prune interval and false-negative
// drop ns!=0 rows). cpp emit must use floor(lo) and ceil(hi) so the
// statsBlob window is a CONSERVATIVE WIDENING of the true [lo, hi].
// Velox/Timestamp.h:183 toMicros() = seconds * 1e6 + nanos / 1000 (integer
// floor); we must compensate on hi.
TEST_F(VeloxColumnarBatchSerializerTest, CB_1_testTimestampNanosCeilUpperFloorLower) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Pick boundary nanos:
  //   lo = (s, 500)        -> raw us = s*1e6, nanos%1000=500 (truncated)
  //                           expected wire lower = floor = s*1e6
  //   hi = (s, 999'500)    -> raw us = s*1e6+999, nanos%1000=500 (truncated)
  //                           expected wire upper = ceil = s*1e6+999+1 = s*1e6+1000
  // If we naively call toMicros for hi, we get s*1e6+999 which is LESS than
  // the true value -> bug.
  const int64_t s = 1704067200;  // 2024-01-01T00:00:00Z
  Timestamp lo(s, 500);
  Timestamp hi(s, 999'500);
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({lo, hi})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;  // skip MAGIC + statsLen
  EXPECT_EQ(p[0], 1u);  // numCols=1
  p += 4;
  EXPECT_EQ(p[0], 1u) << "TIMESTAMP nanos column should still emit supported=1";
  p += 1 + 4 + 4 + 8;  // skip supported + nullCount + count + sizeInBytes

  // lower: lowerLen=8, payload = floor lo = s*1e6
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 8u);
  p += 4;
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i) {
    loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  const int64_t expectedLo = s * 1'000'000LL;
  EXPECT_EQ(loMicros, expectedLo)
      << "CB-1: lower must be floor(lo). Got " << loMicros << ", want " << expectedLo;
  p += 8;

  // upper: upperLen=8, payload = ceil hi = s*1e6 + 1000 (NOT s*1e6+999)
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 8u);
  p += 4;
  int64_t hiMicros = 0;
  for (int i = 0; i < 8; ++i) {
    hiMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  const int64_t expectedHi = s * 1'000'000LL + 1000;  // ceil to next us
  EXPECT_EQ(hiMicros, expectedHi)
      << "CB-1: upper must be ceil(hi) when nanos%1000 != 0. Got "
      << hiMicros << ", want " << expectedHi;
}

// CB-1.2 RED: when nanos % 1000 == 0 (exact us), no ceil adjustment needed.
TEST_F(VeloxColumnarBatchSerializerTest, CB_1_2_testTimestampExactMicrosNoCeil) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  const int64_t s = 1704067200;
  Timestamp lo(s, 0);
  Timestamp hi(s, 1'000);  // exactly 1 us
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({lo, hi})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  const uint8_t* p = framed.data() + 8;
  p += 4 + 1 + 4 + 4 + 8 + 4;  // numCols + supported + n/c/sz + lowerLen
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i) loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  EXPECT_EQ(loMicros, s * 1'000'000LL);
  p += 8 + 4;  // lower bytes + upperLen
  int64_t hiMicros = 0;
  for (int i = 0; i < 8; ++i) hiMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  EXPECT_EQ(hiMicros, s * 1'000'000LL + 1)
      << "CB-1.2: upper exact us should NOT ceil-adjust beyond toMicros";
}

} // namespace gluten
