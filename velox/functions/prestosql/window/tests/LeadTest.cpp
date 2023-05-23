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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class LeadTest : public WindowTestBase {
 protected:
  LeadTest() : overClause_("") {}

  explicit LeadTest(const std::string& overClause) : overClause_(overClause) {}

  void testPrimitiveType(const TypePtr& type) {
    vector_size_t size = 25;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        // Note : The Fuzz vector used in nth_value can have null values.
        makeRandomInputVector(type, size, 0.3),
    });


    LOG(INFO) << "Input rows";
    LOG(INFO) << vectors->toString();
    LOG(INFO) << vectors->toString(0, vectors->size());

    // Add c4 column in sort order in overClauses to impose a deterministic
    // output row order in the tests.
    auto newOverClause = overClause_ + ", c4";

    // The below tests cover nth_value invocations with constant and column
    // arguments. The offsets could also give rows beyond the partition
    // returning null in those cases.
    WindowTestBase::testWindowFunction(
        {vectors}, "lead(c4, c1)", {newOverClause}, kFrameClauses);
  }

 private:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function) {
    WindowTestBase::testWindowFunction(
        input, function, {overClause_}, kFrameClauses);
  }

  const std::string overClause_;
};

class LeadValueTest : public LeadTest,
                      public testing::WithParamInterface<std::string> {
 public:
  LeadValueTest() : LeadTest(GetParam()) {}
};

TEST_P(LeadValueTest, integerValues) {
  testPrimitiveType(INTEGER());
//  duckDbQueryRunner_.execute("select lead(c4, 1) over (partition by c0 order by c1, c2, c3, c4 range between unbounded preceding and current row) from tmp");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    LeadTest,
    LeadValueTest,
    testing::ValuesIn(std::vector<std::string>(kOverClauses)));

} // namespace
} // namespace facebook::velox::window::test
