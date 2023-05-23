/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::window::prestosql {

namespace {

class LeadFunction : public exec::WindowFunction {
 public:
  explicit LeadFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr) {
    //    VELOX_CHECK_EQ(
    //        args.size(), 3, "Lead window function only accept 3 input
    //        arguments.");
    VELOX_CHECK_NULL(args[0].constantValue);
    VELOX_CHECK(
        args[1].type->isInteger(),
        "Second argument of Lead window function must be integer.");

    valueIndex_ = args[0].index.value();
    //    defaultValueOffset_ = args[2].index.value();

    if (args[1].constantValue) {
      VELOX_CHECK(!args[1].constantValue->isNullAt(0));
      constantOffset_ =
          args[1]
              .constantValue->template as<ConstantVector<int32_t>>()
              ->valueAt(0);
    } else {
      offsetIndex_ = args[1].index.value();
    }
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& peerGroupEnds,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    auto numRows = frameStarts->size() / sizeof(vector_size_t);
    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();

    rowNumbers_.resize(numRows);
    if (constantOffset_.has_value()) {
      setRowNumbersForConstantOffset(
          numRows, validRows, rawFrameStarts, rawFrameEnds);
    }

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);

    partitionOffset_ += numRows;

    LOG(INFO) << "rowNumbers";
    std::stringstream ss;
    std::for_each(rowNumbers_.begin(), rowNumbers_.end(), [&](vector_size_t i) {
      ss << i << " ";
    });
    LOG(INFO) << ss.str();
  }

 private:
  void setRowNumbersForConstantOffset(
      vector_size_t numRows,
      const SelectivityVector& validRows,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds) {
    auto constantOffsetValue = constantOffset_.value();
    std::vector<vector_size_t> validIdxs;
    validIdxs.resize(validRows.countSelected());
    validRows.applyToSelected([&](vector_size_t i) { validIdxs.push_back(i); });

    for (auto i = 0; i < validIdxs.size(); i++) {
      auto validIdx = validIdxs[i];
      auto frameStart = frameStarts[validIdx];
      auto frameEnd = frameEnds[validIdx];
      rowNumbers_[validIdx] = ((i + constantOffsetValue) >= validIdxs.size()) &&
              (frameStarts + constantOffsetValue) <= frameEnds
          ? -1
          : frameStart + constantOffsetValue;
    }

    setRowNumbersForEmptyFrames(validRows);
  }

  void setRowNumbersForEmptyFrames(const SelectivityVector& validRows) {
    if (validRows.isAllSelected()) {
      return;
    }
    // Rows with empty (not-valid) frames have nullptr in the result.
    // So mark rowNumber to copy as -1 for it.
    invalidRows_.resizeFill(validRows.size(), true);
    invalidRows_.deselect(validRows);
    invalidRows_.applyToSelected([&](auto i) { rowNumbers_[i] = -1; });
  }

  column_index_t valueIndex_;
  column_index_t offsetIndex_;
  column_index_t defaultValueOffset_;

  std::optional<int32_t> constantOffset_;

  vector_size_t partitionOffset_;

  std::vector<vector_size_t> rowNumbers_;

  const exec::WindowPartition* partition_;
  SelectivityVector invalidRows_;
};

} // namespace

void registerLead(const std::string& name) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      // (T, offset[int], T[default]) -> T
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("integer")
          .argumentType("T")
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          velox::memory::MemoryPool* pool,
          HashStringAllocator*
          /*stringAllocator*/) -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadFunction>(args, resultType, pool);
      });
}
} // namespace facebook::velox::window::prestosql
