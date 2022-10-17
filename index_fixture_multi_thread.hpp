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

#ifndef INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
#define INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// project libraries
#include "common.hpp"

namespace dbgroup::index::test
{
/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class IndexInfo>
class IndexMultiThreadFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // extract key-payload types
  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using KeyComp = typename IndexInfo::Key::Comp;
  using PayComp = typename IndexInfo::Payload::Comp;
  using Index_t = typename IndexInfo::Index_t;
  using ImplStat = typename IndexInfo::ImplStatus;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

#ifdef INDEX_FIXTURE_THREAD_NUM
  static constexpr size_t kThreadNum = INDEX_FIXTURE_THREAD_NUM;
#else
  static constexpr size_t kThreadNum = 8;
#endif

  static constexpr size_t kKeyNum = kExecNum * kThreadNum + 1;
  static constexpr size_t kWaitForThreadCreation = 100;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kThreadNum * 2);

    index_ = std::make_unique<Index_t>();

    is_ready_ = false;
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_);
    ReleaseTestData(payloads_);

    index_ = nullptr;
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  auto
  Write(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (HasWriteOperation<ImplStat>()) {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Write(key, payload, GetLength(key));
    } else {
      return 0;
    }
  }

  auto
  Insert(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (HasInsertOperation<ImplStat>()) {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Insert(key, payload, GetLength(key));
    } else {
      return 0;
    }
  }

  auto
  Update(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (HasUpdateOperation<ImplStat>()) {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return index_->Update(key, payload, GetLength(key));
    } else {
      return 0;
    }
  }

  auto
  Delete([[maybe_unused]] const size_t key_id)
  {
    if constexpr (HasDeleteOperation<ImplStat>()) {
      const auto &key = keys_.at(key_id);
      return index_->Delete(key, GetLength(key));
    } else {
      return 0;
    }
  }

  [[nodiscard]] auto
  CreateTargetIDs(  //
      const size_t w_id,
      const AccessPattern pattern)  //
      -> std::vector<size_t>
  {
    std::vector<size_t> target_ids{};
    {
      std::shared_lock guard{s_mtx_};

      target_ids.reserve(kExecNum);
      if (pattern == kReverse) {
        for (int64_t i = kExecNum - 1; i >= 0; --i) {
          target_ids.emplace_back(kThreadNum * i + w_id);
        }
      } else {
        for (size_t i = 0; i < kExecNum; ++i) {
          target_ids.emplace_back(kThreadNum * i + w_id);
        }
      }

      if (pattern == kRandom) {
        std::mt19937_64 rand_engine{kRandomSeed};
        std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
      }
    }

    std::unique_lock lock{x_mtx_};
    cond_.wait(lock, [this] { return is_ready_; });

    return target_ids;
  }

  void
  RunMT(const std::function<void(size_t)> &func)
  {
    std::vector<std::thread> threads{};
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(func, i);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{kWaitForThreadCreation});
    std::lock_guard guard{s_mtx_};

    is_ready_ = true;
    cond_.notify_all();

    for (auto &&t : threads) {
      t.join();
    }
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const bool expect_success,
      const bool is_update,
      const AccessPattern pattern)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        const auto &key = keys_.at(id);
        const auto read_val = index_->Read(key, GetLength(key));
        if (expect_success) {
          ASSERT_TRUE(read_val);
          const auto expected_val = payloads_.at((is_update) ? w_id + kThreadNum : w_id);
          const auto actual_val = read_val.value();
          EXPECT_TRUE(IsEqual<PayComp>(expected_val, actual_val));
        } else {
          EXPECT_FALSE(read_val);
        }
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyScan(  //
      const bool expect_success,
      const bool is_update)
  {
    if constexpr (HasScanOperation<ImplStat>()) {
      auto mt_worker = [&](const size_t w_id) -> void {
        size_t begin_id = kExecNum * w_id;
        auto &&key = keys_.at(begin_id);
        const auto &begin_key = std::make_tuple(key, GetLength(key), kRangeClosed);

        size_t end_id = kExecNum * (w_id + 1);
        key = keys_.at(end_id);
        const auto &end_key = std::make_tuple(key, GetLength(key), kRangeOpened);

        auto &&iter = index_->Scan(begin_key, end_key);
        if (expect_success) {
          for (; iter.HasNext(); ++iter, ++begin_id) {
            const auto key_id = begin_id;
            const auto val_id =
                (is_update) ? key_id % kThreadNum + kThreadNum : key_id % kThreadNum;

            const auto &[key, payload] = *iter;
            EXPECT_TRUE(IsEqual<KeyComp>(keys_.at(key_id), key));
            EXPECT_TRUE(IsEqual<PayComp>(payloads_.at(val_id), payload));
          }
          EXPECT_EQ(begin_id, end_id);
        }
        EXPECT_FALSE(iter.HasNext());
      };

      RunMT(mt_worker);
    }
  }

  void
  VerifyWrite(  //
      const bool is_update,
      const AccessPattern pattern)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        const auto rc = Write(id, (is_update) ? w_id + kThreadNum : w_id);
        EXPECT_EQ(rc, 0);
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyInsert(  //
      const bool expect_success,
      const bool is_update,
      const AccessPattern pattern)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        const auto rc = Insert(id, (is_update) ? w_id + kThreadNum : w_id);
        if (expect_success) {
          EXPECT_EQ(rc, 0);
        } else {
          EXPECT_NE(rc, 0);
        }
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyUpdate(  //
      const bool expect_success,
      const AccessPattern pattern)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        const auto rc = Update(id, w_id + kThreadNum);
        if (expect_success) {
          EXPECT_EQ(rc, 0);
        } else {
          EXPECT_NE(rc, 0);
        }
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyDelete(  //
      const bool expect_success,
      const AccessPattern pattern)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        const auto rc = Delete(id);
        if (expect_success) {
          EXPECT_EQ(rc, 0);
        } else {
          EXPECT_NE(rc, 0);
        }
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyBulkload()
  {
    if constexpr (HasBulkloadOperation<ImplStat>()) {
      constexpr size_t kOpsNum = kExecNum * kThreadNum;
      if constexpr (IsVarLen<Key>()) {
        std::vector<std::tuple<Key, Payload, size_t>> entries{};
        entries.reserve(kOpsNum);
        for (size_t i = 0; i < kOpsNum; ++i) {
          const auto &key = keys_.at(i);
          const auto &payload = payloads_.at(i % kThreadNum);
          entries.emplace_back(key, payload, GetLength(key));
        }

        const auto rc = index_->Bulkload(entries, kThreadNum);
        EXPECT_EQ(rc, 0);
      } else {
        std::vector<std::pair<Key, Payload>> entries{};
        entries.reserve(kOpsNum);
        for (size_t i = 0; i < kOpsNum; ++i) {
          entries.emplace_back(keys_.at(i), payloads_.at(i % kThreadNum));
        }

        const auto rc = index_->Bulkload(entries, kThreadNum);
        EXPECT_EQ(rc, 0);
      }
    }
  }

  void
  VerifyWritesWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasWriteOperation<ImplStat>()                        //
        || (with_delete && !HasDeleteOperation<ImplStat>()))  //
    {
      GTEST_SKIP();
    }

    VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(kExpectSuccess, pattern);
    if (write_twice) VerifyWrite(kWriteTwice, pattern);
    VerifyRead(kExpectSuccess, write_twice, pattern);
    VerifyScan(kExpectSuccess, write_twice);
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasInsertOperation<ImplStat>()                       //
        || (with_delete && !HasDeleteOperation<ImplStat>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = !with_delete || write_twice;
    const auto is_updated = with_delete && write_twice;

    VerifyInsert(kExpectSuccess, !kWriteTwice, pattern);
    if (with_delete) VerifyDelete(kExpectSuccess, pattern);
    if (write_twice) VerifyInsert(with_delete, write_twice, pattern);
    VerifyRead(expect_success, is_updated, pattern);
    VerifyScan(expect_success, is_updated);
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasUpdateOperation<ImplStat>()  //
        || (with_write && !HasWriteOperation<ImplStat>())
        || (with_delete && !HasDeleteOperation<ImplStat>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(with_write, pattern);
    VerifyUpdate(expect_success, pattern);
    VerifyRead(expect_success, kWriteTwice, pattern);
    VerifyScan(expect_success, kWriteTwice);
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    if (!HasDeleteOperation<ImplStat>()                     //
        || (with_write && !HasWriteOperation<ImplStat>()))  //
    {
      GTEST_SKIP();
    }

    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(with_write, pattern);
    VerifyDelete(expect_success, pattern);
    VerifyRead(kExpectFailed, !kWriteTwice, pattern);
    VerifyScan(kExpectFailed, !kWriteTwice);
  }

  void
  VerifyConcurrentSMOs()
  {
    if (!HasWriteOperation<ImplStat>()       //
        || !HasDeleteOperation<ImplStat>())  //
    {
      GTEST_SKIP();
    }

    constexpr size_t kRepeatNum = 5;

    auto init_worker = [&](const size_t w_id) -> void {
      if (w_id % 2 == 0) {
        for (const auto id : CreateTargetIDs(w_id, kRandom)) {
          EXPECT_EQ(Write(id, w_id), 0);
        }
      }
    };

    auto even_delete_worker = [&](const size_t w_id) -> void {
      if (w_id % 2 == 0) {
        for (const auto id : CreateTargetIDs(w_id, kRandom)) {
          EXPECT_EQ(Delete(id), 0);
        }
      } else {
        for (const auto id : CreateTargetIDs(w_id, kRandom)) {
          EXPECT_EQ(Write(id, w_id), 0);
        }
      }
    };

    auto odd_delete_worker = [&](const size_t w_id) -> void {
      if (w_id % 2 == 0) {
        for (const auto id : CreateTargetIDs(w_id, kRandom)) {
          EXPECT_EQ(Write(id, w_id), 0);
        }
      } else {
        for (const auto id : CreateTargetIDs(w_id, kRandom)) {
          EXPECT_EQ(Delete(id), 0);
        }
      }
    };

    RunMT(init_worker);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      RunMT(even_delete_worker);
      RunMT(odd_delete_worker);
    }
  }

  void
  VerifyBulkloadWith(  //
      const WriteOperation write_ops,
      const AccessPattern pattern)
  {
    if (!HasBulkloadOperation<ImplStat>()                              //
        || (write_ops == kWrite && !HasWriteOperation<ImplStat>())     //
        || (write_ops == kInsert && !HasInsertOperation<ImplStat>())   //
        || (write_ops == kUpdate && !HasUpdateOperation<ImplStat>())   //
        || (write_ops == kDelete && !HasDeleteOperation<ImplStat>()))  //
    {
      GTEST_SKIP();
    }

    auto expect_success = true;
    auto is_updated = false;

    VerifyBulkload();
    switch (write_ops) {
      case kWrite:
        VerifyWrite(kWriteTwice, pattern);
        is_updated = true;
        break;
      case kInsert:
        VerifyInsert(kExpectFailed, kWriteTwice, pattern);
        break;
      case kUpdate:
        VerifyUpdate(kExpectSuccess, pattern);
        is_updated = true;
        break;
      case kDelete:
        VerifyDelete(kExpectSuccess, pattern);
        expect_success = false;
        break;
      case kWithoutWrite:
      default:
        break;
    }
    VerifyRead(expect_success, is_updated, pattern);
    VerifyScan(expect_success, is_updated);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// actual keys
  std::vector<Key> keys_{};

  /// actual payloads
  std::vector<Payload> payloads_{};

  /// an index for testing
  std::unique_ptr<Index_t> index_{nullptr};

  /// a mutex for notifying worker threads.
  std::mutex x_mtx_{};

  /// a shared mutex for blocking main process.
  std::shared_mutex s_mtx_{};

  /// a flag for indicating ready.
  bool is_ready_{false};

  /// a condition variable for notifying worker threads.
  std::condition_variable cond_{};
};

}  // namespace dbgroup::index::test

#endif  // INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
