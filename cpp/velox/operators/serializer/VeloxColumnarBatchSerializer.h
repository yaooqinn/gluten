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

#pragma once

#include <arrow/c/abi.h>

#include "memory/ColumnarBatch.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Variant.h"

namespace gluten {

// PA-2 ColumnStats: minimal per-column min/max + nullCount carrier consumed
// by the JVM cache stats marshaling (Layer A). PA-2.1 fills BIGINT; later
// micro-slices add other scalar types. Unsupported / NaN-poisoned columns
// emit hasLowerBound=hasUpperBound=false (graceful degrade for buildFilter).
struct ColumnStats {
  bool hasLowerBound{false};
  bool hasUpperBound{false};
  facebook::velox::variant lowerBound;
  facebook::velox::variant upperBound;
  int64_t nullCount{0};
};

class VeloxColumnarBatchSerializer : public ColumnarBatchSerializer {
 public:
  VeloxColumnarBatchSerializer(
      arrow::MemoryPool* arrowPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      struct ArrowSchema* cSchema);

  void append(const std::shared_ptr<ColumnarBatch>& batch) override;

  int64_t maxSerializedSize() override;

  void serializeTo(uint8_t* address, int64_t size) override;

  std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) override;

  // PA-2.1 GREEN: per-column min/max scan. Scalar v1 -- only BIGINT FlatVector
  // returns hasLowerBound/UpperBound=true. All other types fall back to
  // hasLowerBound=hasUpperBound=false (buildFilter pass-through).
  std::vector<ColumnStats> computeStats(facebook::velox::RowVectorPtr rowVector);

  // PA-2.5a/b GREEN: framed bytes carrying [magic | statsLen | statsBlob | bytesLen | bytesBlob].
  // statsBlob marshaled per docs/0003 sec 3 (numCols + per-col supported/nullCount/count/
  // sizeInBytes + lower/upperBound bytes). PA-2.5b currently emits BIGINT lower/upperBound;
  // REAL/HUGEINT marshaling lands in follow-up slices. magic = 0xFE 0xCA 0x53 0x02 matches
  // the JVM Kryo V2_MAGIC for PA-1 round-trip.
  std::vector<uint8_t> framedSerializeWithStats(const std::shared_ptr<ColumnarBatch>& batch) override;

 protected:
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::unique_ptr<facebook::velox::StreamArena> arena_;
  std::unique_ptr<facebook::velox::IterativeVectorSerializer> serializer_;
  facebook::velox::RowTypePtr rowType_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions options_;
};

} // namespace gluten
