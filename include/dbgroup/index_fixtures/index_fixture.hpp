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
#include <optional>
#include <random>
#include <utility>
#include <vector>

// external libraries
#include "dbgroup/index/utility.hpp"
#include "gtest/gtest.h"

// local sources
#include "common.hpp"
#include "index_wrapper.hpp"

namespace dbgroup::index::test
{
/*############################################################################*
 * Fixture class definition
 *############################################################################*/

template <class IndexInfo>
class IndexFixture : public testing::Test
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Index = IndexWrapper<IndexInfo>;

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

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
    index_ = nullptr;
  }

  /*##########################################################################*
   * Utility functions
   *##########################################################################*/

  void
  PrepareData(  //
      const AccessPattern pattern = kSequential,
      const size_t rec_num = kExecNum)
  {
    index_ = std::make_unique<Index>(kExecNum);

    target_ids_.reserve(rec_num);
    for (size_t i = 0; i < rec_num; ++i) {
      target_ids_.emplace_back(i);
    }

    if (pattern == kReverse) {
      std::reverse(target_ids_.begin(), target_ids_.end());
    } else if (pattern == kRandom) {
      std::mt19937_64 rand_engine{kRandomSeed};
      std::shuffle(target_ids_.begin(), target_ids_.end(), rand_engine);
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
    if (kDisableReadTest || HasFailure()) return;

    std::cout << "  [dbgroup] read...\n";
    for (const auto &id : target_ids_) {
      const auto &ret = index_->Read(id);
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
  VerifyScan(  //
      [[maybe_unused]] const size_t rec_num,
      [[maybe_unused]] const bool expect_success,
      [[maybe_unused]] const uint32_t expected_val)
  {
    if (kDisableScanTest || HasFailure()) return;

    std::cout << "  [dbgroup] scan...\n";
    auto &&iter = index_->Scan();
    if (expect_success) {
      if constexpr (!kDisableScanVerifyTest) {
        iter.PrepareVerifier();
      }

      size_t i = 0;
      for (; !HasFailure() && iter && i < rec_num; ++iter, ++i) {
        const auto &[_, payload] = *iter;
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
  VerifyWrite()
  {
    if (kDisableWriteTest || HasFailure()) return;

    std::cout << "  [dbgroup] write...\n";
    for (const auto &id : target_ids_) {
      index_->Write(id);
      if (HasFailure()) return;
    }
  }

  void
  VerifyUpsert(  //
      const bool expect_insert = true,
      const uint32_t expected_val = 0)
  {
    if (kDisableUpsertTest || HasFailure()) return;

    std::cout << "  [dbgroup] upsert...\n";
    for (const auto &id : target_ids_) {
      const auto &ret = index_->Upsert(id);
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
    if (kDisableInsertTest || HasFailure()) return;

    std::cout << "  [dbgroup] insert...\n";
    for (const auto &id : target_ids_) {
      const auto &ret = index_->Insert(id);
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
    if (kDisableUpdateTest || HasFailure()) return;

    std::cout << "  [dbgroup] update...\n";
    for (const auto &id : target_ids_) {
      const auto &ret = index_->Update(id);
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
    if (kDisableDeleteTest || HasFailure()) return;

    std::cout << "  [dbgroup] delete...\n";
    for (const auto &id : target_ids_) {
      const auto &ret = index_->Delete(id);
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
    PrepareData();
    VerifyRead(kExpectFailed, 0);
  }

  void
  VerifyScanWith(  //
      const bool closed)
  {
    if (kDisableScanTest                               //
        || (kDisableWriteTest && kDisableInsertTest))  //
    {
      GTEST_SKIP();
    }

    const size_t rec_num = kExecNum - (closed ? 0 : 2);
    PrepareData();

    std::cout << "  [dbgroup] initialization...\n";
    for (size_t i = 0; !HasFailure() && i < kExecNum; ++i) {
      if constexpr (kDisableWriteTest) {
        index_->Write(i);
      } else {
        index_->Insert(i);
      }
    }

    std::cout << "  [dbgroup] scan...\n";
    auto &&iter = index_->Scan(0, closed, kExecNum - 1, closed);
    size_t i = closed ? 0 : 1;
    for (; !HasFailure() && iter && i < rec_num; ++iter, ++i) {
      const auto &[key, payload] = *iter;
      ASSERT_EQ(payload, 1) << "[Scan: payload]";
    }
    ASSERT_EQ(i, rec_num) << "[Scan: # of records]";
    if (closed) {
      ASSERT_FALSE(iter) << "[Scan: iterator]";
    } else {
      ASSERT_TRUE(iter) << "[Scan: iterator]";
    }
  }

  void
  VerifyWriteWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern,
      const size_t ops_num = kExecNum)
  {
    if (kDisableWriteTest                        //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    PrepareData(pattern, ops_num);

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
    VerifyScan(ops_num, expect_success, expected_val);
  }

  void
  VerifyUpsertWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern,
      const size_t ops_num = kExecNum)
  {
    if (kDisableWriteTest                        //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    PrepareData(pattern, ops_num);

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
    VerifyScan(ops_num, expect_success, expected_val);
  }

  void
  VerifyInsertWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableInsertTest                       //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    PrepareData(pattern);

    uint32_t expected_val = 1;
    VerifyInsert(kExpectSuccess, expected_val);

    if (with_delete) {
      VerifyDelete(kExpectSuccess, expected_val);
    }

    if (write_twice) {
      VerifyInsert(with_delete, expected_val);
    }

    VerifyRead(expect_success, expected_val);
    VerifyScan(kExecNum, expect_success, expected_val);
  }

  void
  VerifyUpdateWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableUpdateTest                       //
        || (with_write && kDisableWriteTest)     //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;
    PrepareData(pattern);

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
    VerifyScan(kExecNum, expect_success, expected_val);
  }

  void
  VerifyDeleteWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableDeleteTest                     //
        || (with_write && kDisableWriteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;
    PrepareData(pattern);

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
    VerifyScan(kExecNum, kExpectFailed, expected_val);
  }

  void
  VerifyBulkloadWith(  //
      const WriteOperation write_ops,
      const AccessPattern pattern)
  {
    if (kDisableBulkloadTest                              //
        || (write_ops == kWrite && kDisableWriteTest)     //
        || (write_ops == kUpsert && kDisableUpsertTest)   //
        || (write_ops == kInsert && kDisableInsertTest)   //
        || (write_ops == kUpdate && kDisableUpdateTest)   //
        || (write_ops == kDelete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData(pattern);
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
    VerifyScan(kExecNum, expect_success, expected_val);
  }

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      kExecNum >= kRecNumWithInternalSMOs,
      "DBGROUP_TEST_EXEC_NUM >= 30,000.");

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief An index for testing.
  std::unique_ptr<Index> index_{};

  /// @brief Record IDs for testing.
  std::vector<size_t> target_ids_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_HPP
