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
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include "dbgroup/index/utility.hpp"
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::index::test
{
/*##############################################################################
 * Fixture class definition
 *############################################################################*/

template <class IndexInfo>
class IndexFixture : public testing::Test
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  // extract key-payload types
  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using KeyComp = typename IndexInfo::Key::Comp;
  using PayComp = typename IndexInfo::Payload::Comp;
  using Index = typename IndexInfo::Index;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using ScanKeyRef = std::optional<std::pair<size_t, bool>>;

 protected:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kRecNumWithoutSMOs = 30;
  static constexpr size_t kRecNumWithLeafSMOs = 1000;
  static constexpr size_t kRecNumWithInternalSMOs = 30000;
  static constexpr size_t kKeyNum =
      (kExecNum < kRecNumWithInternalSMOs ? kRecNumWithInternalSMOs : kExecNum) + 2;

  /*############################################################################
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

  /*############################################################################
   * Utility functions
   *##########################################################################*/

  void
  PrepareData()
  {
    index_ = std::make_unique<Index>();
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kKeyNum);
  }

  void
  DestroyData()
  {
    ReleaseTestData(keys_);
    ReleaseTestData(payloads_);
  }

  [[nodiscard]] auto
  CreateTargetIDs(  //
      const size_t rec_num,
      const AccessPattern pattern) const  //
      -> std::vector<size_t>
  {
    std::mt19937_64 rand_engine{kRandomSeed};
    std::vector<size_t> target_ids{};
    target_ids.reserve(rec_num);
    if (pattern == kReverse) {
      for (int64_t i = static_cast<int64_t>(rec_num) - 1; i >= 0; --i) {
        target_ids.emplace_back(i);
      }
    } else {
      for (size_t i = 0; i < rec_num; ++i) {
        target_ids.emplace_back(i);
      }
    }
    if (pattern == kRandom) {
      std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
    }
    return target_ids;
  }

  /*############################################################################
   * Wrapper functions
   *##########################################################################*/

  auto
  Read(                                      //
      [[maybe_unused]] const size_t key_id)  //
      -> std::optional<Payload>
  {
    if constexpr (kDisableReadTest) {
      return std::nullopt;
    } else {
      const auto &key = keys_.at(key_id);
      return index_->Read(key, GetLength(key));
    }
  }

  auto
  Scan(  //
      [[maybe_unused]] const ScanKey &begin_key = std::nullopt,
      [[maybe_unused]] const ScanKey &end_key = std::nullopt)
  // -> IteratorType
  {
    if constexpr (kDisableScanTest) {
      return DummyIter<Key, Payload>{};
    } else {
      return index_->Scan(begin_key, end_key);
    }
  }

  auto
  Write(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)  //
      -> ReturnCode
  {
    if constexpr (kDisableWriteTest) {
      return kKeyNotExist;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Write(key, payload, GetLength(key), GetLength(payload));
    }
  }

  auto
  Insert(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)  //
      -> ReturnCode
  {
    if constexpr (kDisableInsertTest) {
      return kKeyNotExist;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Insert(key, payload, GetLength(key), GetLength(payload));
    }
  }

  auto
  Update(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)  //
      -> ReturnCode
  {
    if constexpr (kDisableUpdateTest) {
      return kKeyExist;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Update(key, payload, GetLength(key), GetLength(payload));
    }
  }

  auto
  Delete(                                    //
      [[maybe_unused]] const size_t key_id)  //
      -> ReturnCode
  {
    if constexpr (kDisableDeleteTest) {
      return kKeyExist;
    } else {
      const auto &key = keys_.at(key_id);
      return index_->Delete(key, GetLength(key));
    }
  }

  auto
  Bulkload()  //
      -> ReturnCode
  {
    if constexpr (kDisableBulkloadTest) {
      return kKeyNotExist;
    } else {
      std::vector<std::tuple<Key, Payload, size_t, size_t>> entries{};
      entries.reserve(kExecNum);
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto &key = keys_.at(i);
        const auto &payload = payloads_.at(i);
        entries.emplace_back(key, payload, GetLength(key), GetLength(payload));
      }
      return index_->Bulkload(entries, 1);
    }
  }

  void
  FillIndex()
  {
    for (size_t i = 0; i < kExecNum; ++i) {
      if constexpr (kDisableWriteTest) {
        Write(i, i);
      } else {
        Insert(i, i);
      }
    }
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  void
  VerifyRead(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success,
      const bool write_twice = false)
  {
    if constexpr (kDisableReadTest) {
      return;
    }

    std::cout << "  [dbgroup] read...\n";
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;
      const auto &read_val = Read(key_id);
      if (expect_success) {
        ASSERT_TRUE(read_val) << "[Read: payload]";
        if (read_val) {
          AssertEQ<PayComp>(payloads_.at(pay_id), read_val.value(), "[Read: payload]");
        }
      } else {
        ASSERT_FALSE(read_val) << "[Read: payload]";
      }
      if (HasFatalFailure()) return;
    }
  }

  void
  VerifyScan(  //
      [[maybe_unused]] const ScanKeyRef &begin_ref,
      [[maybe_unused]] const ScanKeyRef &end_ref,
      [[maybe_unused]] const bool expect_success = true,
      [[maybe_unused]] const bool write_twice = false)
  {
    if constexpr (kDisableScanTest) {
      return;
    }

    std::cout << "  [dbgroup] scan...\n";
    ScanKey begin_key{std::nullopt};
    size_t begin_pos = 0;
    if (begin_ref) {
      auto &&[begin_id, begin_closed] = *begin_ref;
      const auto &key = keys_.at(begin_id);
      begin_key.emplace(key, GetLength(key), begin_closed);
      begin_pos = (begin_closed) ? begin_id : begin_id + 1;
    }

    ScanKey end_key{std::nullopt};
    size_t end_pos = 0;
    if (end_ref) {
      auto &&[end_id, end_closed] = *end_ref;
      const auto &key = keys_.at(end_id);
      end_key.emplace(key, GetLength(key), end_closed);
      end_pos = (end_closed) ? end_id + 1 : end_id;
    }

    auto &&iter = Scan(begin_key, end_key);
    if (expect_success) {
      if constexpr (!kDisableScanVerifyTest) {
        iter.PrepareVerifier();
      }
      for (; iter; ++iter, ++begin_pos) {
        const auto &[key, payload] = *iter;
        const auto val_id = (write_twice) ? begin_pos + 1 : begin_pos;
        AssertEQ<KeyComp>(keys_.at(begin_pos), key, "[Scan: key]");
        AssertEQ<PayComp>(payloads_.at(val_id), payload, "[Scan: payload]");
        if (HasFatalFailure()) return;
      }
      if constexpr (!kDisableScanVerifyTest) {
        ASSERT_TRUE(iter.VerifySnapshot()) << "[Scan: snapshot read]";
        ASSERT_TRUE(iter.VerifyNoPhantom()) << "[Scan: phantom avoidance]";
      }
      if (end_ref) {
        ASSERT_EQ(begin_pos, end_pos) << "[Scan: iterator]";
      }
    }
    ASSERT_FALSE(iter) << "[Scan: iterator]";
  }

  void
  VerifyWrite(  //
      const std::vector<size_t> &target_ids,
      const bool write_twice = false)
  {
    std::cout << "  [dbgroup] write...\n";
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;
      ASSERT_EQ(Write(key_id, pay_id), kSuccess) << "[Write: RC]";
    }
  }

  void
  VerifyInsert(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success,
      const bool write_twice = false)
  {
    std::cout << "  [dbgroup] insert...\n";
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;
      if (expect_success) {
        ASSERT_EQ(Insert(key_id, pay_id), kSuccess) << "[Insert: RC]";
      } else {
        ASSERT_EQ(Insert(key_id, pay_id), kKeyExist) << "[Insert: RC]";
      }
    }
  }

  void
  VerifyUpdate(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success)
  {
    std::cout << "  [dbgroup] update...\n";
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = key_id + 1;
      if (expect_success) {
        ASSERT_EQ(Update(key_id, pay_id), kSuccess) << "[Update: RC]";
      } else {
        ASSERT_EQ(Update(key_id, pay_id), kKeyNotExist) << "[Update: RC]";
      }
    }
  }

  void
  VerifyDelete(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success)
  {
    std::cout << "  [dbgroup] delete...\n";
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      if (expect_success) {
        ASSERT_EQ(Delete(key_id), kSuccess) << "[Delete: RC]";
      } else {
        ASSERT_EQ(Delete(key_id), kKeyNotExist) << "[Delete: RC]";
      }
    }
  }

  void
  VerifyBulkload()
  {
    ASSERT_EQ(Bulkload(), kSuccess) << "[Bulkload: RC]";
  }

  /*############################################################################
   * Functions for test definitions
   *##########################################################################*/

  void
  VerifyReadEmptyIndex()
  {
    PrepareData();
    VerifyRead({0}, kExpectFailed);
    DestroyData();
  }

  void
  VerifyScanWith(  //
      const bool has_range,
      const bool closed = true)
  {
    constexpr auto kRecNum = kExecNum;

    if (kDisableScanTest                               //
        || (kDisableWriteTest && kDisableInsertTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData();

    ScanKeyRef begin_key = std::nullopt;
    ScanKeyRef end_key = std::nullopt;
    if (has_range) {
      begin_key = std::make_pair(0, closed);
      end_key = std::make_pair(kRecNum - 1, closed);
    }

    std::cout << "  [dbgroup] initialization...\n";
    FillIndex();
    VerifyScan(begin_key, end_key);

    DestroyData();
  }

  void
  VerifyWritesWith(  //
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

    PrepareData();

    const auto &target_ids = CreateTargetIDs(ops_num, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(ops_num, kRangeOpened);

    VerifyWrite(target_ids);
    if (HasFatalFailure()) goto END_TEST;

    if (with_delete) VerifyDelete(target_ids, kExpectSuccess);
    if (HasFatalFailure()) goto END_TEST;

    if (write_twice) VerifyWrite(target_ids, kWriteTwice);
    if (HasFatalFailure()) goto END_TEST;

    VerifyRead(target_ids, !with_delete || write_twice, write_twice);
    if (HasFatalFailure()) goto END_TEST;

    VerifyScan(begin_ref, end_ref, kExpectSuccess, write_twice);
  END_TEST:
    DestroyData();
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableInsertTest                       //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData();

    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_success = !with_delete || write_twice;
    const bool is_updated = write_twice && with_delete;

    VerifyInsert(target_ids, kExpectSuccess);
    if (HasFatalFailure()) goto END_TEST;

    if (with_delete) VerifyDelete(target_ids, kExpectSuccess);
    if (HasFatalFailure()) goto END_TEST;

    if (write_twice) VerifyInsert(target_ids, with_delete, kWriteTwice);
    if (HasFatalFailure()) goto END_TEST;

    VerifyRead(target_ids, expect_success, is_updated);
    if (HasFatalFailure()) goto END_TEST;

    VerifyScan(begin_ref, end_ref, expect_success, is_updated);
  END_TEST:
    DestroyData();
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableUpdateTest                                                            //
        || (with_write && kDisableWriteTest) || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData();

    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_update = with_write && !with_delete;

    if (with_write) VerifyWrite(target_ids);
    if (HasFatalFailure()) goto END_TEST;

    if (with_delete) VerifyDelete(target_ids, with_write);
    if (HasFatalFailure()) goto END_TEST;

    VerifyUpdate(target_ids, expect_update);
    if (HasFatalFailure()) goto END_TEST;

    VerifyRead(target_ids, expect_update, kWriteTwice);
    if (HasFatalFailure()) goto END_TEST;

    VerifyScan(begin_ref, end_ref, expect_update, kWriteTwice);
  END_TEST:
    DestroyData();
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableDeleteTest                     //
        || (with_write && kDisableWriteTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData();

    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_delete = with_write && !with_delete;

    if (with_write) VerifyWrite(target_ids);
    if (HasFatalFailure()) goto END_TEST;

    if (with_delete) VerifyDelete(target_ids, with_write);
    if (HasFatalFailure()) goto END_TEST;

    VerifyDelete(target_ids, expect_delete);
    if (HasFatalFailure()) goto END_TEST;

    VerifyRead(target_ids, kExpectFailed);
    if (HasFatalFailure()) goto END_TEST;

    VerifyScan(begin_ref, end_ref, kExpectFailed);
  END_TEST:
    DestroyData();
  }

  void
  VerifyBulkloadWith(  //
      const WriteOperation write_ops,
      const AccessPattern pattern)
  {
    if (kDisableBulkloadTest                              //
        || (write_ops == kWrite && kDisableWriteTest)     //
        || (write_ops == kInsert && kDisableInsertTest)   //
        || (write_ops == kUpdate && kDisableUpdateTest)   //
        || (write_ops == kDelete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    PrepareData();

    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    auto expect_success = true;
    auto is_updated = false;

    std::cout << "  [dbgroup] bulkload...\n";
    VerifyBulkload();
    if (HasFatalFailure()) goto END_TEST;

    switch (write_ops) {
      case kWrite:
        VerifyWrite(target_ids, kWriteTwice);
        is_updated = true;
        break;
      case kInsert:
        VerifyInsert(target_ids, kExpectFailed);
        break;
      case kUpdate:
        VerifyUpdate(target_ids, kExpectSuccess);
        is_updated = true;
        break;
      case kDelete:
        VerifyDelete(target_ids, kExpectSuccess);
        expect_success = false;
        break;
      case kWithoutWrite:
      default:
        break;
    }
    if (HasFatalFailure()) goto END_TEST;

    VerifyRead(target_ids, expect_success, is_updated);
    if (HasFatalFailure()) goto END_TEST;

    VerifyScan(begin_ref, end_ref, expect_success, is_updated);
  END_TEST:
    DestroyData();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// actual keys
  std::vector<Key> keys_{};

  /// actual payloads
  std::vector<Payload> payloads_{};

  /// an index for testing
  std::unique_ptr<Index> index_{nullptr};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_HPP
