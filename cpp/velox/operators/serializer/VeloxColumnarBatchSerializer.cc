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
      case TypeKind::HUGEINT: {
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
  // Step 1: produce bytesBlob via existing serializer path (append + serializeTo).
  append(batch);
  const int64_t bytesLen = maxSerializedSize();
  std::vector<uint8_t> bytesBlob(bytesLen);
  serializeTo(bytesBlob.data(), bytesLen);

  // Step 2: PA-2.5a uses an EMPTY statsBlob; PA-2.5b will populate from computeStats.
  const uint32_t statsLen = 0;

  // Step 3: assemble [magic(4) | statsLen(4 LE) | statsBlob | bytesLen(4 LE) | bytesBlob].
  std::vector<uint8_t> framed;
  framed.reserve(4 + 4 + statsLen + 4 + bytesLen);

  // magic: 0xFE 0xCA 0x53 0x02 (matches JVM Kryo V2_MAGIC).
  framed.push_back(0xFE);
  framed.push_back(0xCA);
  framed.push_back(0x53);
  framed.push_back(0x02);

  // statsLen LE.
  framed.push_back(static_cast<uint8_t>(statsLen & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 24) & 0xFF));

  // statsBlob: empty in PA-2.5a.

  // bytesLen LE.
  const uint32_t bytesLen32 = static_cast<uint32_t>(bytesLen);
  framed.push_back(static_cast<uint8_t>(bytesLen32 & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 24) & 0xFF));

  // bytesBlob.
  framed.insert(framed.end(), bytesBlob.begin(), bytesBlob.end());
  return framed;
}

} // namespace gluten
