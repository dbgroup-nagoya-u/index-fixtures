/*
 * Copyright 2021 Database Group, Nagoya University
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

#ifndef DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_HPP
#define DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_HPP

// C++ standard libraries
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

// external libraries
#include <gtest/gtest.h>

// external C++ libraries
#include <dbgroup/index/concepts.hpp>
#include <dbgroup/index/utility.hpp>

// local sources
#include "common.hpp"
#include "index_wrapper.hpp"

namespace dbgroup::index::test
{
/*############################################################################*
 * Fixture class definition
 *############################################################################*/

template <class IndexInfo>
class IndexFixture : public ::testing::Test
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using Comp = typename IndexInfo::Key::Comp;
  using Index = typename IndexInfo::Index;
  using IndexWrapper_t = IndexWrapper<IndexInfo>;

 protected:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kRecNumWithoutSMOs = 10;
  static constexpr size_t kRecNumWithLeafSMOs = 1000;
  static constexpr size_t kRecNumWithInternalSMOs = 30000;
  static constexpr uint32_t kUpdDelta = kDisableRecordMerging ? 0 : 1;

  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  static void
  SetUpTestSuite()
  {
    keys = PrepareTestData<Key>(kExecNum);

    forward.reserve(kExecNum);
    for (size_t i = 0; i < kExecNum; ++i) {
      forward.emplace_back(i);
    }

    backward = forward;
    std::reverse(backward.begin(), backward.end());

    random = forward;
    std::mt19937_64 rand_engine{kRandomSeed};
    std::shuffle(random.begin(), random.end(), rand_engine);
  }

  static void
  TearDownTestSuite()
  {
    forward = {};
    backward = {};
    random = {};
    ReleaseTestData(keys);
  }

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
    if (index_) {
      index_->TearDown();
      index_ = nullptr;
    }
  }

  /*##########################################################################*
   * Utility functions
   *##########################################################################*/

  void
  Preprocess(  //
      const AccessPattern pattern = kSequential,
      const size_t rec_num = kExecNum)
  {
    index_ = std::make_unique<IndexWrapper_t>(keys);
    index_->SetUp();
    exec_num_ = rec_num;
    if (pattern == kSequential) {
      target_ids_ = &forward;
    } else if (pattern == kReverse) {
      target_ids_ = &backward;
    } else {  // pattern == kRandom
      target_ids_ = &random;
    }
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyRead(  //
      const bool expect_success,
      const uint32_t expected_val)
  {
    if (!HasRead<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] read...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      const auto& ret = index_->Read(id);
      if (HasFailure()) return;

      if (expect_success) {
        ASSERT_TRUE(ret) << "[Read: RC]";
        ASSERT_EQ(ret.value(), expected_val) << "[Read: returned value]";
      } else {
        ASSERT_FALSE(ret) << "[Read: RC]";
      }
    }
  }

  void
  VerifyScanForward(  //
      [[maybe_unused]] const size_t rec_num,
      [[maybe_unused]] const bool expect_success,
      [[maybe_unused]] const uint32_t expected_val)
  {
    if (!HasScan<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] scan forward...\n";
    auto&& iter = index_->Scan();
    if (expect_success) {
      if constexpr (!kDisableScanVerifyTest) {
        iter.PrepareVerifier();
      }

      size_t i = 0;
      for (; !HasFailure() && iter && i < rec_num; ++iter, ++i) {
        const auto& [key, payload] = *iter;
        ASSERT_TRUE(Equal<Comp>(key, keys[i])) << "[Scan: key]";
        ASSERT_EQ(payload, expected_val) << "[Scan: payload]";
      }
      ASSERT_EQ(i, rec_num) << "[Scan: # of records]";

      if constexpr (!kDisableScanVerifyTest) {
        ASSERT_TRUE(iter.VerifySnapshot()) << "[Scan: snapshot read]";
        ASSERT_TRUE(iter.VerifyNoPhantom()) << "[Scan: phantom avoidance]";
      }
    }
    ASSERT_FALSE(iter) << "[Scan: iterator]";
  }

  void
  VerifyScanBackward(  //
      [[maybe_unused]] const size_t rec_num,
      [[maybe_unused]] const bool expect_success,
      [[maybe_unused]] const uint32_t expected_val)
  {
    if (!HasScanBackward<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] scan backward...\n";
    auto&& iter = index_->ScanBackward();
    if (expect_success) {
      if constexpr (!kDisableScanVerifyTest) {
        iter.PrepareVerifier();
      }

      auto i = static_cast<int32_t>(rec_num - 1);
      for (; !HasFailure() && iter && i >= 0; ++iter, --i) {
        const auto& [key, payload] = *iter;
        ASSERT_TRUE(Equal<Comp>(key, keys[i])) << "[ScanBackward: key]";
        ASSERT_EQ(payload, expected_val) << "[ScanBackward: payload]";
      }
      ASSERT_EQ(i, -1) << "[ScanBackward: # of records]";

      if constexpr (!kDisableScanVerifyTest) {
        ASSERT_TRUE(iter.VerifySnapshot()) << "[ScanBackward: snapshot read]";
        ASSERT_TRUE(iter.VerifyNoPhantom()) << "[ScanBackward: phantom avoidance]";
      }
    }
    ASSERT_FALSE(iter) << "[ScanBackward: iterator]";
  }

  void
  VerifyWrite()
  {
    if (!HasWrite<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] write...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      index_->Write(id);
      if (HasFailure()) return;
    }
  }

  void
  VerifyUpsert(  //
      const bool expect_insert = true,
      const uint32_t expected_val = 0)
  {
    if (!HasUpsert<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] upsert...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      const auto& ret = index_->Upsert(id);
      if (HasFailure()) return;

      if (expect_insert) {
        ASSERT_FALSE(ret) << "[Upsert: RC]";
      } else {
        ASSERT_TRUE(ret) << "[Upsert: RC]";
        ASSERT_EQ(ret.value(), expected_val) << "[Upsert: returned value]";
      }
    }
  }

  void
  VerifyInsert(  //
      const bool expect_success,
      const uint32_t expected_val = 0)
  {
    if (!HasInsert<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] insert...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      const auto& ret = index_->Insert(id);
      if (HasFailure()) return;

      if (expect_success) {
        ASSERT_FALSE(ret) << "[Insert: RC]";
      } else {
        ASSERT_TRUE(ret) << "[Insert: RC]";
        ASSERT_EQ(ret.value(), expected_val) << "[Insert: returned value]";
      }
    }
  }

  void
  VerifyUpdate(  //
      const bool expect_success,
      const uint32_t expected_val)
  {
    if (!HasUpdate<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] update...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      const auto& ret = index_->Update(id);
      if (HasFailure()) return;

      if (expect_success) {
        ASSERT_TRUE(ret) << "[Update: RC]";
        ASSERT_EQ(ret.value(), expected_val) << "[Update: returned value]";
      } else {
        ASSERT_FALSE(ret) << "[Update: RC]";
      }
    }
  }

  void
  VerifyDelete(  //
      const bool expect_success,
      const uint32_t expected_val)
  {
    if (!HasDelete<Index, Key, Payload>() || HasFailure()) return;

    std::cout << "  [dbgroup] delete...\n";
    for (size_t i = 0; i < exec_num_; ++i) {
      const auto id = target_ids_->at(i);
      const auto& ret = index_->Delete(id);
      if (HasFailure()) return;

      if (expect_success) {
        ASSERT_TRUE(ret) << "[Delete: RC]";
        ASSERT_EQ(ret.value(), expected_val) << "[Delete: returned value]";
      } else {
        ASSERT_FALSE(ret) << "[Delete: RC]";
      }
    }
  }

  /*##########################################################################*
   * Functions for test definitions
   *##########################################################################*/

  void
  VerifyReadEmptyIndex()
  {
    Preprocess();
    VerifyRead(kExpectFailed, 0);
  }

  void
  VerifyScanForwardWith(  //
      const bool closed)
  {
    if (!HasScan<Index, Key, Payload>()                                              //
        || (!HasWrite<Index, Key, Payload>() && !HasInsert<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    Preprocess();

    std::cout << "  [dbgroup] initialization...\n";
    for (size_t i = 0; !HasFailure() && i < kExecNum; ++i) {
      if constexpr (!HasWrite<Index, Key, Payload>()) {
        index_->Write(i);
      } else {
        index_->Insert(i);
      }
    }

    std::cout << "  [dbgroup] scan forward...\n";
    const size_t rec_num = kExecNum - (closed ? 0 : 1);
    auto&& iter = index_->Scan(0, closed, kExecNum - 1, closed);
    size_t i = closed ? 0 : 1;
    for (; !HasFailure() && iter && i < rec_num; ++iter, ++i) {
      const auto& [key, payload] = *iter;
      ASSERT_TRUE(Equal<Comp>(key, keys[i])) << "[Scan: key]";
      ASSERT_EQ(payload, 1) << "[Scan: payload]";
    }
    ASSERT_EQ(i, rec_num) << "[Scan: # of records]";
    ASSERT_FALSE(iter) << "[Scan: iterator]";
  }

  void
  VerifyScanBackwardWith(  //
      const bool closed)
  {
    if (!HasScanBackward<Index, Key, Payload>()                                      //
        || (!HasWrite<Index, Key, Payload>() && !HasInsert<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    Preprocess();

    std::cout << "  [dbgroup] initialization...\n";
    for (size_t i = 0; !HasFailure() && i < kExecNum; ++i) {
      if constexpr (!HasWrite<Index, Key, Payload>()) {
        index_->Write(i);
      } else {
        index_->Insert(i);
      }
    }

    std::cout << "  [dbgroup] scan backward...\n";
    const int32_t end_pos = closed ? -1 : 0;
    auto&& iter = index_->ScanBackward(0, closed, kExecNum - 1, closed);
    auto i = static_cast<int32_t>(kExecNum - (closed ? 1 : 2));
    for (; !HasFailure() && iter && i > end_pos; ++iter, --i) {
      const auto& [key, payload] = *iter;
      ASSERT_TRUE(Equal<Comp>(key, keys[i])) << "[ScanBackward: key]";
      ASSERT_EQ(payload, 1) << "[ScanBackward: payload]";
    }
    ASSERT_EQ(i, end_pos) << "[ScanBackward: # of records]";
    ASSERT_FALSE(iter) << "[ScanBackward: iterator]";
  }

  void
  VerifyWriteWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern,
      const size_t ops_num = kExecNum)
  {
    if (!HasWrite<Index, Key, Payload>()                        //
        || (with_delete && !HasDelete<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    Preprocess(pattern, ops_num);

    const auto upd_delta = with_delete ? 1 : kUpdDelta;
    uint32_t expected_val = 1;
    VerifyWrite();

    if (with_delete) {
      VerifyDelete(kExpectSuccess, expected_val);
      expected_val = 0;
    }

    if (write_twice) {
      VerifyWrite();
      expected_val += upd_delta;
    }

    VerifyRead(expect_success, expected_val);
    VerifyScanForward(ops_num, expect_success, expected_val);
    VerifyScanBackward(ops_num, expect_success, expected_val);
  }

  void
  VerifyUpsertWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern,
      const size_t ops_num = kExecNum)
  {
    if (!HasUpsert<Index, Key, Payload>()                       //
        || (with_delete && !HasDelete<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    Preprocess(pattern, ops_num);

    const auto upd_delta = with_delete ? 1 : kUpdDelta;
    uint32_t expected_val = 1;
    VerifyUpsert();

    if (with_delete) {
      VerifyDelete(kExpectSuccess, expected_val);
      expected_val = 0;
    }

    if (write_twice) {
      VerifyUpsert(with_delete, expected_val);
      expected_val += upd_delta;
    }

    VerifyRead(expect_success, expected_val);
    VerifyScanForward(ops_num, expect_success, expected_val);
    VerifyScanBackward(ops_num, expect_success, expected_val);
  }

  void
  VerifyInsertWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasInsert<Index, Key, Payload>()                       //
        || (with_delete && !HasDelete<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    Preprocess(pattern);

    uint32_t expected_val = 1;
    VerifyInsert(kExpectSuccess, expected_val);

    if (with_delete) {
      VerifyDelete(kExpectSuccess, expected_val);
    }

    if (write_twice) {
      VerifyInsert(with_delete, expected_val);
    }

    VerifyRead(expect_success, expected_val);
    VerifyScanForward(kExecNum, expect_success, expected_val);
    VerifyScanBackward(kExecNum, expect_success, expected_val);
  }

  void
  VerifyUpdateWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasUpdate<Index, Key, Payload>()                       //
        || (with_write && !HasWrite<Index, Key, Payload>())     //
        || (with_delete && !HasDelete<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;
    Preprocess(pattern);

    uint32_t expected_val = 0;
    if (with_write) {
      VerifyWrite();
      expected_val = 1;
    }

    if (with_delete) {
      VerifyDelete(with_write, expected_val);
    }

    VerifyUpdate(expect_success, expected_val);
    if (expect_success) {
      expected_val += kUpdDelta;
    }

    VerifyRead(expect_success, expected_val);
    VerifyScanForward(kExecNum, expect_success, expected_val);
    VerifyScanBackward(kExecNum, expect_success, expected_val);
  }

  void
  VerifyDeleteWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasDelete<Index, Key, Payload>()                     //
        || (with_write && !HasWrite<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;
    Preprocess(pattern);

    uint32_t expected_val = 0;
    if (with_write) {
      VerifyWrite();
      expected_val = 1;
    }

    if (with_delete) {
      VerifyDelete(with_write, expected_val);
    }

    VerifyDelete(expect_success, expected_val);

    VerifyRead(kExpectFailed, expected_val);
    VerifyScanForward(kExecNum, kExpectFailed, expected_val);
    VerifyScanBackward(kExecNum, kExpectFailed, expected_val);
  }

  void
  VerifyBulkloadWith(  //
      const WriteOperation write_ops,
      const AccessPattern pattern)
  {
    if (!HasBulkload<Index, Key, Payload>()                              //
        || (write_ops == kWrite && !HasWrite<Index, Key, Payload>())     //
        || (write_ops == kUpsert && !HasUpsert<Index, Key, Payload>())   //
        || (write_ops == kInsert && !HasInsert<Index, Key, Payload>())   //
        || (write_ops == kUpdate && !HasUpdate<Index, Key, Payload>())   //
        || (write_ops == kDelete && !HasDelete<Index, Key, Payload>()))  //
    {
      GTEST_SKIP();
    }

    Preprocess(pattern);
    auto expect_success = true;
    uint32_t expected_val = 1;

    std::cout << "  [dbgroup] bulkload...\n";
    index_->Bulkload();
    switch (write_ops) {
      case kWrite:
        VerifyWrite();
        expected_val += kUpdDelta;
        break;
      case kUpsert:
        expected_val += kUpdDelta;
        VerifyUpsert(expected_val);
        break;
      case kInsert:
        VerifyInsert(kExpectFailed, expected_val);
        break;
      case kUpdate:
        VerifyUpdate(kExpectSuccess, expected_val);
        expected_val += kUpdDelta;
        break;
      case kDelete:
        VerifyDelete(kExpectSuccess, expected_val);
        expect_success = false;
        break;
      case kWithoutWrite:
      default:
        break;
    }

    VerifyRead(expect_success, expected_val);
    VerifyScanForward(kExecNum, expect_success, expected_val);
    VerifyScanBackward(kExecNum, expect_success, expected_val);
  }

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      kExecNum >= kRecNumWithInternalSMOs,
      "DBGROUP_TEST_EXEC_NUM >= 30,000.");

  /*##########################################################################*
   * Static member variables
   *##########################################################################*/

  /// @brief Actual keys.
  static inline std::vector<Key> keys;

  /// @brief Target IDs for sequential accesses in forward order.
  static inline std::vector<size_t> forward;

  /// @brief Target IDs for sequential accesses in backward order.
  static inline std::vector<size_t> backward;

  /// @brief Target IDs for random accesses.
  static inline std::vector<size_t> random;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief An index for testing.
  std::unique_ptr<IndexWrapper_t> index_{};

  /// @brief The number of executions.
  size_t exec_num_{};

  /// @brief Record IDs for testing.
  const std::vector<size_t>* target_ids_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_HPP
