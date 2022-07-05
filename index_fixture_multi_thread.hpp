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
#include <vector>

// external libraries
#include "gtest/gtest.h"

// project libraries
#include "common.hpp"

namespace dbgroup::index::test
{
/*######################################################################################
 * Constants for multi threading
 *####################################################################################*/

#ifdef INDEX_FIXTURE_THREAD_NUM
static constexpr size_t kThreadNum = INDEX_FIXTURE_THREAD_NUM;
#else
static constexpr size_t kThreadNum = 8;
#endif

/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <template <class K, class V, class C> class IndexType, class KeyType, class PayloadType>
struct IndexInfo {
  using Key = KeyType;
  using Payload = PayloadType;
  using Index = IndexType<typename Key::Data, typename Payload::Data, typename Key::Comp>;
};

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
  using Index = typename IndexInfo::Index;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kKeyNum = kExecNum * kThreadNum;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kThreadNum * 2);

    index_ = std::make_unique<Index>();

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
      const size_t key_id,
      const size_t pay_id)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      return index_->Write(keys_.at(key_id), payloads_.at(pay_id), kKeyLen);
    } else {
      return index_->Write(keys_.at(key_id), payloads_.at(pay_id));
    }
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t pay_id)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      return index_->Insert(keys_.at(key_id), payloads_.at(pay_id), kKeyLen);
    } else {
      return index_->Insert(keys_.at(key_id), payloads_.at(pay_id));
    }
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t pay_id)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      return index_->Update(keys_.at(key_id), payloads_.at(pay_id), kKeyLen);
    } else {
      return index_->Update(keys_.at(key_id), payloads_.at(pay_id));
    }
  }

  auto
  Delete(const size_t key_id)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      return index_->Delete(keys_.at(key_id), kKeyLen);
    } else {
      return index_->Delete(keys_.at(key_id));
    }
  }

  [[nodiscard]] auto
  CreateTargetIDs(  //
      const size_t w_id,
      const bool is_shuffled)  //
      -> std::vector<size_t>
  {
    std::vector<size_t> target_ids{};
    {
      std::shared_lock guard{s_mtx_};

      target_ids.reserve(kExecNum);
      for (size_t i = 0; i < kExecNum; ++i) {
        target_ids.emplace_back(kThreadNum * i + w_id);
      }

      if (is_shuffled) {
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

    std::this_thread::sleep_for(std::chrono::milliseconds{1});
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
      const bool is_shuffled)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, is_shuffled)) {
        const auto &read_val = index_->Read(keys_.at(id));
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
  VerifyWrite(  //
      const bool is_update,
      const bool is_shuffled)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, is_shuffled)) {
        const auto rc = Write(id, (is_update) ? w_id + kThreadNum : w_id);
        EXPECT_EQ(rc, 0);
      }
    };

    RunMT(mt_worker);
  }

  void
  VerifyInsert(  //
      const bool expect_success,
      const bool is_shuffled)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, is_shuffled)) {
        const auto rc = Insert(id, w_id);
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
      const bool is_shuffled)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, is_shuffled)) {
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
      const bool is_shuffled)
  {
    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, is_shuffled)) {
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
  VerifyWritesWith(  //
      const bool write_twice,
      const bool with_delete,
      const bool is_shuffled)
  {
    VerifyWrite(!kWriteTwice, is_shuffled);
    if (with_delete) VerifyDelete(kExpectSuccess, is_shuffled);
    if (write_twice) VerifyWrite(kWriteTwice, is_shuffled);
    VerifyRead(kExpectSuccess, write_twice, is_shuffled);
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const bool is_shuffled)
  {
    VerifyInsert(kExpectSuccess, is_shuffled);
    if (with_delete) VerifyDelete(kExpectSuccess, is_shuffled);
    if (write_twice) VerifyInsert(with_delete, is_shuffled);
    VerifyRead(kExpectSuccess, !kWriteTwice, is_shuffled);
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, is_shuffled);
    if (with_delete) VerifyDelete(with_write, is_shuffled);
    VerifyUpdate(expect_success, is_shuffled);
    VerifyRead(expect_success, kWriteTwice, is_shuffled);
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, is_shuffled);
    if (with_delete) VerifyDelete(with_write, is_shuffled);
    VerifyDelete(expect_success, is_shuffled);
    VerifyRead(kExpectFailed, !kWriteTwice, is_shuffled);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// actual keys
  std::vector<Key> keys_{};

  /// actual payloads
  std::vector<Payload> payloads_{};

  /// an index for testing
  std::unique_ptr<Index> index_{nullptr};

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
