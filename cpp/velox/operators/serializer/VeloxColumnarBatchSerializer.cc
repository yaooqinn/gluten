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

#include "VeloxColumnarBatchSerializer.h"

#include <arrow/buffer.h>

#include <cmath>
#include <cstring>

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <iostream>

using namespace facebook::velox;

namespace gluten {
namespace {

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

} // namespace

VeloxColumnarBatchSerializer::VeloxColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> veloxPool,
    struct ArrowSchema* cSchema)
    : ColumnarBatchSerializer(arrowPool), veloxPool_(std::move(veloxPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  arena_ = std::make_unique<StreamArena>(veloxPool_.get());
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
}

void VeloxColumnarBatchSerializer::append(const std::shared_ptr<ColumnarBatch>& batch) {
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  if (serializer_ == nullptr) {
    // Using first batch's schema to create the Velox serializer. This logic was introduced in
    // https://github.com/apache/gluten/pull/1568. It's a bit suboptimal because the schemas
    // across different batches may vary.
    auto numRows = rowVector->size();
    auto rowType = asRowType(rowVector->type());
    serializer_ = serde_->createIterativeSerializer(rowType, numRows, arena_.get(), &options_);
  }
  const IndexRange allRows{0, rowVector->size()};
  serializer_->append(rowVector, folly::Range(&allRows, 1));
}

int64_t VeloxColumnarBatchSerializer::maxSerializedSize() {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  return serializer_->maxSerializedSize();
}

void VeloxColumnarBatchSerializer::serializeTo(uint8_t* address, int64_t size) {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  auto sizeNeeded = serializer_->maxSerializedSize();
  GLUTEN_CHECK(
      size >= sizeNeeded,
      "The target buffer size is insufficient: " + std::to_string(size) + " vs." + std::to_string(sizeNeeded));
  std::shared_ptr<arrow::MutableBuffer> valueBuffer = std::make_shared<arrow::MutableBuffer>(address, size);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer_->flush(&out);
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), veloxPool_.get(), rowType_, &result, &options_);
  return std::make_shared<VeloxColumnarBatch>(result);
}

namespace {

// Per-type FlatVector min/max scan + NaN guard. Returns false if column
// must be marked unsupported (any NaN observed for floating-point types).
template <typename T>
bool scanMinMax(
    const facebook::velox::FlatVector<T>* flat,
    T& tLo,
    T& tHi,
    int64_t& nullCnt,
    bool& seen) {
  const auto size = flat->size();
  const uint64_t* nulls = flat->rawNulls();
  const T* values = flat->rawValues();
  for (vector_size_t i = 0; i < size; ++i) {
    if (nulls != nullptr && bits::isBitNull(nulls, i)) {
      ++nullCnt;
      continue;
    }
    T v = values[i];
    if constexpr (std::is_floating_point_v<T>) {
      // NaN poisons the whole column: Spark equality (NaN != NaN) means
      // min/max-based pruning would silently drop matching rows. NB4
      // ship blocker.
      if (std::isnan(v)) {
        return false;
      }
    }
    if (!seen) {
      tLo = v;
      tHi = v;
      seen = true;
    } else {
      if (v < tLo) tLo = v;
      if (v > tHi) tHi = v;
    }
  }
  return true;
}

} // namespace

std::vector<ColumnStats> VeloxColumnarBatchSerializer::computeStats(RowVectorPtr rowVector) {
  std::vector<ColumnStats> result;
  const auto numCols = rowVector->childrenSize();
  result.resize(numCols);
  for (column_index_t col = 0; col < numCols; ++col) {
    auto& stats = result[col];
    auto child = rowVector->childAt(col);
    if (child == nullptr || !child->isFlatEncoding()) {
      continue;
    }
    bool seen = false;
    int64_t nullCnt = 0;
    bool supported = false;
    switch (child->typeKind()) {
      case TypeKind::BIGINT: {
        auto* flat = child->asFlatVector<int64_t>();
        int64_t lo = 0, hi = 0;
        supported = scanMinMax<int64_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::INTEGER: {
        auto* flat = child->asFlatVector<int32_t>();
        int32_t lo = 0, hi = 0;
        supported = scanMinMax<int32_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::SMALLINT: {
        auto* flat = child->asFlatVector<int16_t>();
        int16_t lo = 0, hi = 0;
        supported = scanMinMax<int16_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TINYINT: {
        auto* flat = child->asFlatVector<int8_t>();
        int8_t lo = 0, hi = 0;
        supported = scanMinMax<int8_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::REAL: {
        auto* flat = child->asFlatVector<float>();
        float lo = 0.f, hi = 0.f;
        supported = scanMinMax<float>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::DOUBLE: {
        // PA-10: scanMinMax<double> NaN guard returns false on first NaN
        // (poisons the column → emit supported=0 to avoid silent wrong prune).
        auto* flat = child->asFlatVector<double>();
        double lo = 0.0, hi = 0.0;
        supported = scanMinMax<double>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::BOOLEAN: {
        // PA-10: bool min/max via scanMinMax<bool>. Velox stores bool as
        // packed bits but FlatVector<bool>::rawValues() exposes contiguous
        // unsigned char buffer; scanMinMax compares 0/1 ints.
        auto* flat = child->asFlatVector<bool>();
        bool lo = false, hi = false;
        supported = scanMinMax<bool>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::HUGEINT: {
        // PA-8: restore HUGEINT scan for long-Decimal (precision > 18) marshal.
        // (B4 in PA-6.5 dropped this when emit_gate excluded it; PA-8 wires it
        // through with 16B LE.)
        auto* flat = child->asFlatVector<int128_t>();
        int128_t lo = 0, hi = 0;
        supported = scanMinMax<int128_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TIMESTAMP: {
        // PA-6.2.E: Velox Timestamp struct {seconds, nanos} has defaulted
        // operator<=> (Timestamp.h:377-378), so scanMinMax<Timestamp>
        // compiles via the existing template. Variant<Timestamp> is a
        // first-class scalar (Variant.h:259). Wire emit converts via
        // Timestamp::toMicros() to int64 microseconds, sharing the JVM
        // LongType 8B path (Spark TimestampType / TimestampNTZType
        // physical = Long microseconds).
        auto* flat = child->asFlatVector<Timestamp>();
        Timestamp lo, hi;
        supported = scanMinMax<Timestamp>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::VARCHAR: {
        // PA-9: VARCHAR scan via StringView. StringView::operator<=>
        // (StringView.h:219) uses memcmp -> unsigned byte ordering, matching
        // Spark ByteArray.compareBinary which uses Byte.toUnsignedInt + 0xFF
        // (ByteArray.java). variant(std::string{sv}) heap-allocates and owns
        // the bytes (Variant.h:232), so the StringView's pointer-into-
        // FlatVector-buffer lifetime is not a concern after computeStats
        // returns. cpp does NOT truncate -- JVM PA-9 truncates to 256B on
        // read with carry-overflow demote (design 0008 sec 3.1).
        auto* flat = child->asFlatVector<StringView>();
        StringView lo, hi;
        supported = scanMinMax<StringView>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          // Force std::string copy to own the bytes; std::string ctor
          // takes (const char*, size_t).
          stats.lowerBound = variant(std::string(lo.data(), lo.size()));
          stats.upperBound = variant(std::string(hi.data(), hi.size()));
        }
        break;
      }
      default:
        // Other types deferred to later micro-slices (Integer / Double / String /
        // Decimal). hasLowerBound=hasUpperBound=false => buildFilter pass-through.
        break;
    }
    stats.nullCount = nullCnt;
  }
  return result;
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeWithStats(
    const std::shared_ptr<ColumnarBatch>& batch) {
  // Step 1: compute stats over the inbound rowVector BEFORE delegating to the
  // append path (which may consume / mutate iterator state on subsequent calls).
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  const uint32_t numRows = static_cast<uint32_t>(rowVector->size());
  std::vector<ColumnStats> perCol = computeStats(rowVector);
  const uint32_t numCols = static_cast<uint32_t>(perCol.size());

  // Step 2: marshal statsBlob per docs/0003 sec 3. Helpers append LE primitives.
  std::vector<uint8_t> statsBlob;
  auto pushU8 = [&](uint8_t v) { statsBlob.push_back(v); };
  auto pushU32 = [&](uint32_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };
  auto pushU64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      statsBlob.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }
  };
  auto pushI64LE = [&](int64_t v) { pushU64(static_cast<uint64_t>(v)); };
  auto pushU16LE = [&](uint16_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  };

  pushU32(numCols);
  for (const auto& s : perCol) {
    // PA-6.2.B: dispatch by Variant typeKind for BIGINT/INTEGER/SMALLINT/TINYINT.
    // Width matches JVM wire format (writeU64/U32/U16LE / 1B). REAL / HUGEINT
    // marshaling still lands in follow-up slices.
    auto kind = s.lowerBound.kind();
    bool emitSupported = s.hasLowerBound && s.hasUpperBound &&
        s.lowerBound.kind() == s.upperBound.kind() &&
        (kind == facebook::velox::TypeKind::BIGINT ||
         kind == facebook::velox::TypeKind::INTEGER ||
         kind == facebook::velox::TypeKind::SMALLINT ||
         kind == facebook::velox::TypeKind::TINYINT ||
         kind == facebook::velox::TypeKind::HUGEINT ||
         kind == facebook::velox::TypeKind::REAL ||
         kind == facebook::velox::TypeKind::DOUBLE ||
         kind == facebook::velox::TypeKind::BOOLEAN ||
         kind == facebook::velox::TypeKind::TIMESTAMP ||
         kind == facebook::velox::TypeKind::VARCHAR);
    pushU8(emitSupported ? 1 : 0);
    pushU32(static_cast<uint32_t>(s.nullCount));
    pushU32(numRows);  // PA-6.5 B3 (corrected post-Layer-2 #2): vanilla
                       // PartitionStatistics.count = numRows (gatherNullStats
                       // increments count too per ColumnStats.scala:65). Earlier
                       // attempt subtracted nullCount, which inverted IsNotNull
                       // predicate (count - nullCount > 0) into double-subtract.
    pushU64(0);        // sizeInBytes placeholder (PA-2.5b)
    if (emitSupported) {
      switch (kind) {
        case facebook::velox::TypeKind::BIGINT:
          pushU32(8);
          pushI64LE(s.lowerBound.value<int64_t>());
          pushU32(8);
          pushI64LE(s.upperBound.value<int64_t>());
          break;
        case facebook::velox::TypeKind::INTEGER:
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.lowerBound.value<int32_t>()));
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.upperBound.value<int32_t>()));
          break;
        case facebook::velox::TypeKind::SMALLINT:
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.lowerBound.value<int16_t>()));
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.upperBound.value<int16_t>()));
          break;
        case facebook::velox::TypeKind::TINYINT:
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.lowerBound.value<int8_t>()));
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.upperBound.value<int8_t>()));
          break;
        case facebook::velox::TypeKind::HUGEINT: {
          // PA-8: 16 LE bytes. int128 split into low/high uint64 halves,
          // little-endian wire order (low first). JVM reconstructs via
          // BigInteger from signed two's-complement big-endian byte array
          // (reverse on read).
          auto pushI128LE = [&](int128_t v) {
            uint64_t lo = static_cast<uint64_t>(v);
            uint64_t hi = static_cast<uint64_t>(v >> 64);
            pushU64(lo);
            pushU64(hi);
          };
          pushU32(16);
          pushI128LE(s.lowerBound.value<int128_t>());
          pushU32(16);
          pushI128LE(s.upperBound.value<int128_t>());
          break;
        }
        case facebook::velox::TypeKind::REAL: {
          // PA-10: 4B IEEE 754 float, little-endian via bit reinterpret.
          // JVM reconstructs via Float.intBitsToFloat.
          uint32_t loBits, hiBits;
          float lo = s.lowerBound.value<float>();
          float hi = s.upperBound.value<float>();
          std::memcpy(&loBits, &lo, sizeof(uint32_t));
          std::memcpy(&hiBits, &hi, sizeof(uint32_t));
          pushU32(4);
          pushU32(loBits);
          pushU32(4);
          pushU32(hiBits);
          break;
        }
        case facebook::velox::TypeKind::DOUBLE: {
          // PA-10: 8B IEEE 754 double, little-endian via bit reinterpret.
          // JVM reconstructs via Double.longBitsToDouble.
          uint64_t loBits, hiBits;
          double lo = s.lowerBound.value<double>();
          double hi = s.upperBound.value<double>();
          std::memcpy(&loBits, &lo, sizeof(uint64_t));
          std::memcpy(&hiBits, &hi, sizeof(uint64_t));
          pushU32(8);
          pushU64(loBits);
          pushU32(8);
          pushU64(hiBits);
          break;
        }
        case facebook::velox::TypeKind::BOOLEAN: {
          // PA-10: 1B (0 or 1).
          pushU32(1);
          pushU8(s.lowerBound.value<bool>() ? 1 : 0);
          pushU32(1);
          pushU8(s.upperBound.value<bool>() ? 1 : 0);
          break;
        }
        case facebook::velox::TypeKind::TIMESTAMP: {
          // PA-6.2.E: emit as 8B LE int64 microseconds. Spark TimestampType /
          // TimestampNTZType physical = Long us; this shares the JVM LongType
          // 8B wire arm with no JVM-side changes (LongType isDispatchable +
          // serialize/deserialize already accept Timestamp/TimestampNTZ).
          pushU32(8);
          pushI64LE(s.lowerBound.value<facebook::velox::Timestamp>().toMicros());
          pushU32(8);
          pushI64LE(s.upperBound.value<facebook::velox::Timestamp>().toMicros());
          break;
        }
        case facebook::velox::TypeKind::VARCHAR: {
          // PA-9: emit as len u32 LE + raw UTF-8 bytes. cpp does NOT
          // truncate; JVM PA-9 applies 256B truncate + carry-overflow
          // demote on read (design 0008 sec 3.1). variant.value<VARCHAR>()
          // returns a std::string& (owned).
          const auto& loStr = s.lowerBound.value<facebook::velox::TypeKind::VARCHAR>();
          const auto& hiStr = s.upperBound.value<facebook::velox::TypeKind::VARCHAR>();
          pushU32(static_cast<uint32_t>(loStr.size()));
          for (auto c : loStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          pushU32(static_cast<uint32_t>(hiStr.size()));
          for (auto c : hiStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          break;
        }
        default:
          break;
      }
    }
  }
  const uint32_t statsLen = static_cast<uint32_t>(statsBlob.size());

  // Step 3: produce bytesBlob via existing serializer path.
  append(batch);
  const int64_t bytesLen = maxSerializedSize();
  std::vector<uint8_t> bytesBlob(bytesLen);
  serializeTo(bytesBlob.data(), bytesLen);

  // Step 4: assemble [magic(4) | statsLen(4 LE) | statsBlob | bytesLen(4 LE) | bytesBlob].
  std::vector<uint8_t> framed;
  framed.reserve(4 + 4 + statsLen + 4 + bytesLen);

  framed.push_back(0xFE);
  framed.push_back(0xCA);
  framed.push_back(0x53);
  framed.push_back(0x02);

  framed.push_back(static_cast<uint8_t>(statsLen & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 24) & 0xFF));

  framed.insert(framed.end(), statsBlob.begin(), statsBlob.end());

  const uint32_t bytesLen32 = static_cast<uint32_t>(bytesLen);
  framed.push_back(static_cast<uint8_t>(bytesLen32 & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 24) & 0xFF));

  framed.insert(framed.end(), bytesBlob.begin(), bytesBlob.end());
  return framed;
}

} // namespace gluten
