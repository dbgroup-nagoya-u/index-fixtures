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

#ifndef DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
#define DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP

// C++ standard libraries
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <tuple>
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
/*##############################################################################
 * Fixture class definition
 *############################################################################*/

template <class IndexInfo>
class IndexMultiThreadFixture : public testing::Test
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Index = IndexWrapper<IndexInfo>;

 protected:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kScanSize = 1000;
  static constexpr uint32_t kInitVal = kDisableRecordMerging ? 1 : kWorkerNum;
  static constexpr uint32_t kUpdDelta = kDisableRecordMerging ? 0 : kWorkerNum;

  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    ready_num_ = 0;
    is_ready_ = false;
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
  PrepareData(  //
      const AccessPattern pattern)
  {
    index_ = std::make_unique<Index>(kExecNum);
    pattern_ = pattern;
  }

  [[nodiscard]] auto
  CreateIDs(                            //
      const size_t rec_num = kExecNum)  //
      -> std::vector<size_t>
  {
    std::vector<size_t> target_ids{};
    target_ids.reserve(rec_num);
    for (size_t i = 0; i < rec_num; ++i) {
      target_ids.emplace_back(i);
    }

    if (pattern_ == kReverse) {
      std::reverse(target_ids.begin(), target_ids.end());
    } else if (pattern_ == kRandom) {
      std::mt19937_64 rand_engine{std::random_device{}()};
      std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
    }

    ++ready_num_;
    while (!is_ready_) {
      std::this_thread::yield();
    }
    return target_ids;
  }

  void
  RunMT(  //
      const std::function<void(size_t)> &func)
  {
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(func, i);
    }
    while (ready_num_ < kThreadNum) {
      std::this_thread::yield();
    }
    is_ready_ = true;
    for (auto &&t : threads) {
      t.join();
    }

    is_ready_ = false;
    ready_num_ = 0;
    if constexpr (kNodeNum > 1) {
      index_->Barrier();
    }
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  void
  VerifyRead(  //
      const bool expect_success,
      const uint32_t expected_val)
  {
    if (kDisableReadTest || HasFailure()) return;

    auto mt_worker = [&]([[maybe_unused]] const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        const auto &ret = index_->Read(id);
        if (HasFailure()) return;

        if (expect_success) {
          ASSERT_TRUE(ret) << "[Read: RC]";
          ASSERT_EQ(ret.value(), expected_val) << "[Read: returned value]";
        } else {
          ASSERT_FALSE(ret) << "[Read: RC]";
        }
      }
    };

    std::cout << "  [dbgroup] read...\n";
    RunMT(mt_worker);
  }

  void
  VerifyScan(  //
      [[maybe_unused]] const bool expect_success,
      [[maybe_unused]] const uint32_t expected_val)
  {
    if (kDisableScanTest || HasFailure()) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (auto id : CreateIDs(kExecNum - (kThreadNum + 1))) {
        if (id % kThreadNum != w_id) continue;
        const auto end_id = id + kThreadNum;
        auto &&iter = index_->Scan(id, kClosed, end_id, kOpen);
        if (expect_success) {
          if constexpr (!kDisableScanVerifyTest) {
            iter.PrepareVerifier();
          }
          for (; iter; ++iter, ++id) {
            if (HasFailure()) return;
            const auto &[key, payload] = *iter;
            ASSERT_EQ(payload, expected_val) << "[Scan: payload]";
          }
          if constexpr (!kDisableScanVerifyTest) {
            ASSERT_TRUE(iter.VerifySnapshot()) << "[Scan: snapshot read]";
            ASSERT_TRUE(iter.VerifyNoPhantom()) << "[Scan: phantom avoidance]";
          }
          ASSERT_EQ(id, end_id) << "[Scan: # of scanned records]";
        }
        ASSERT_FALSE(iter) << "[Scan: iterator reach end]";
      }
    };

    std::cout << "  [dbgroup] scan...\n";
    RunMT(mt_worker);
  }

  void
  VerifyWrite()
  {
    if (kDisableWriteTest || HasFailure()) return;

    auto mt_worker = [&]([[maybe_unused]] const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        index_->Write(id);
        if (HasFailure()) return;
      }
    };

    std::cout << "  [dbgroup] write...\n";
    RunMT(mt_worker);
  }

  void
  VerifyInsert(  //
      const uint32_t expected_val)
  {
    if (kDisableInsertTest || HasFailure()) return;

    auto mt_worker = [&]([[maybe_unused]] const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        const auto &ret = index_->Insert(id);
        if (HasFailure()) return;

        if (ret) {
          ASSERT_EQ(ret.value(), expected_val) << "[Insert: returned value]";
        }
      }
    };

    std::cout << "  [dbgroup] insert...\n";
    RunMT(mt_worker);
  }

  void
  VerifyUpdate(  //
      const bool expect_success,
      const uint32_t max_expected_val)
  {
    if (kDisableUpdateTest || HasFailure()) return;

    auto mt_worker = [&]([[maybe_unused]] const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        const auto &ret = index_->Update(id);
        if (HasFailure()) return;

        if (expect_success) {
          ASSERT_TRUE(ret) << "[Update: RC]";
          ASSERT_LE(ret.value(), max_expected_val) << "[Update: returned value]";
        } else {
          ASSERT_FALSE(ret) << "[Update: RC]";
        }
      }
    };

    std::cout << "  [dbgroup] update...\n";
    RunMT(mt_worker);
  }

  void
  VerifyDelete(  //
      const uint32_t expected_val)
  {
    if (kDisableDeleteTest || HasFailure()) return;

    auto mt_worker = [&]([[maybe_unused]] const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        const auto &ret = index_->Delete(id);
        if (HasFailure()) return;

        if (ret) {
          ASSERT_EQ(ret.value(), expected_val) << "[Delete: returned value]";
        }
      }
    };

    std::cout << "  [dbgroup] delete...\n";
    RunMT(mt_worker);
  }

  /*############################################################################
   * Functions for test definitions
   *##########################################################################*/

  void
  VerifyWritesWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (kDisableWriteTest                        //
        || (with_delete && kDisableDeleteTest))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    PrepareData(pattern);

    const auto upd_delta = with_delete ? kInitVal : kUpdDelta;
    uint32_t expected_val = kInitVal;
    VerifyWrite();

    if (with_delete) {
      VerifyDelete(expected_val);
      expected_val = 0;
    }

    if (write_twice) {
      expected_val += upd_delta;
      VerifyWrite();
    }

    VerifyRead(expect_success, expected_val);
    VerifyScan(expect_success, expected_val);
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

    const auto expect_success = !with_delete || write_twice;
    PrepareData(pattern);

    uint32_t expected_val = 1;
    VerifyInsert(expected_val);

    if (with_delete) {
      VerifyDelete(expected_val);
    }

    if (write_twice) {
      VerifyInsert(expected_val);
    }

    VerifyRead(expect_success, expected_val);
    VerifyScan(expect_success, expected_val);
  }

  void
  VerifyUpdatesWith(  //
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
      expected_val = kInitVal;
      VerifyWrite();
    }

    if (with_delete) {
      VerifyDelete(expected_val);
    }

    expected_val += kUpdDelta;
    VerifyUpdate(expect_success, expected_val);

    VerifyRead(expect_success, expected_val);
    VerifyScan(expect_success, expected_val);
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

    PrepareData(pattern);

    uint32_t expected_val = 0;
    if (with_write) {
      VerifyWrite();
      expected_val = kInitVal;
    }

    if (with_delete) {
      VerifyDelete(expected_val);
    }

    VerifyDelete(expected_val);

    VerifyRead(kExpectFailed, expected_val);
    VerifyScan(kExpectFailed, expected_val);
  }

  void
  VerifyConcurrentSMOs()
  {
    constexpr size_t kRepeatNum = 5;
    constexpr size_t kDeleteThread = kThreadNum * 1 / 4;
    constexpr size_t kReadThread = kThreadNum * 2 / 4;
    constexpr size_t kScanThread = kThreadNum * 3 / 4;
    constexpr size_t kMaxVal = kRepeatNum * kWorkerNum;
    std::atomic_size_t counter{};

    if (kDisableWriteTest      //
        || kDisableDeleteTest  //
        || kDisableScanTest)   //
    {
      GTEST_SKIP();
    }

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto &id : CreateIDs()) {
        if (w_id >= kScanThread) {
          auto &&iter = index_->Scan(id);
          for (size_t i = 0; iter && i < kScanSize; ++iter, ++i) {
            const auto &[_, val] = *iter;
            ASSERT_GE(val, 0) << "[Scan: read value]";
            ASSERT_LE(val, kMaxVal) << "[Scan: read value]";
            if (HasFailure() || counter >= kScanThread) return;
          }
        } else if (w_id >= kReadThread) {
          const auto &ret = index_->Read(id);
          if (ret) {
            const auto &val = *ret;
            ASSERT_GE(val, 0) << "[Read: read value]";
            ASSERT_LE(val, kMaxVal) << "[Read: read value]";
          }
        } else if (w_id >= kDeleteThread) {
          index_->Delete(id);
        } else {
          index_->Write(id);
        }
        if (HasFailure()) break;
      }
      counter += 1;
    };

    PrepareData(kRandom);
    std::cout << "  [dbgroup] initialization...\n";
    for (size_t i = 0; i < kRepeatNum && !HasFailure(); ++i) {
      std::cout << "  [dbgroup] repeat #" << i << "...\n";
      counter = 0;
      RunMT(mt_worker);
    }
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

    PrepareData(pattern);
    auto expect_success = true;
    uint32_t expected_val = 1;

    std::cout << "  [dbgroup] bulkload...\n";
    index_->Bulkload();
    switch (write_ops) {
      case kWrite:
        expected_val += kUpdDelta;
        VerifyWrite();
        break;
      case kInsert:
        VerifyInsert(expected_val);
        break;
      case kUpdate:
        expected_val += kUpdDelta;
        VerifyUpdate(kExpectSuccess, expected_val);
        break;
      case kDelete:
        VerifyDelete(expected_val);
        expect_success = false;
        break;
      case kWithoutWrite:
      default:
        break;
    }

    VerifyRead(expect_success, expected_val);
    VerifyScan(expect_success, expected_val);
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief An index for testing
  std::unique_ptr<Index> index_{};

  /// @brief The number of threads that are ready for testing.
  std::atomic_size_t ready_num_{};

  /// @brief A flag for indicating ready.
  std::atomic_bool is_ready_{};

  /// @brief An access pattern for testing.
  AccessPattern pattern_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
