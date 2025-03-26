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
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

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

  // extract key-payload types
  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using KeyComp = typename IndexInfo::Key::Comp;
  using PayComp = typename IndexInfo::Payload::Comp;
  using Index = typename IndexInfo::Index;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

 protected:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kThreadNum = DBGROUP_TEST_THREAD_NUM;
  static constexpr size_t kKeyNum = (kExecNum + 2) * kThreadNum;
  static constexpr size_t kWaitForThreadCreation = 100;

  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    is_ready_ = false;
    no_failure_ = true;
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
  AssertTrue(  //
      const bool expect_true,
      const std::string_view &tag)
  {
    if (!expect_true) {
      const std::lock_guard lock{io_mtx_};
      std::cout << "  [" << tag << "] The actual value was not true.\n";
#ifdef NDEBUG
      throw std::runtime_error{""};
#else
      FAIL();
#endif
    }
  }

  void
  AssertFalse(  //
      const bool expect_false,
      const std::string_view &tag)
  {
    if (expect_false) {
      const std::lock_guard lock{io_mtx_};
      std::cout << "  [" << tag << "] The actual value was not false.\n";
#ifdef NDEBUG
      throw std::runtime_error{""};
#else
      FAIL();
#endif
    }
  }

  template <class T>
  void
  AssertEQ(  //
      const T &actual,
      const T &expected,
      const std::string_view &tag)
  {
    bool is_equal;
    if constexpr (std::is_same_v<T, char *>) {
      is_equal = std::strcmp(actual, expected) == 0;
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      is_equal = *actual == *expected;
    } else {
      is_equal = actual == expected;
    }

    if (!is_equal) {
      const std::lock_guard lock{io_mtx_};
      std::cout << "  [" << tag << "] The actual value was different from the expected one.\n"
                << "    actual:   " << actual << "\n"
                << "    expected: " << expected << "\n";
#ifdef NDEBUG
      throw std::runtime_error{""};
#else
      FAIL();
#endif
    }
  }

  template <class T>
  void
  AssertNE(  //
      const T &actual,
      const T &expected,
      const std::string_view &tag)
  {
    bool is_not_equal;
    if constexpr (std::is_same_v<T, char *>) {
      is_not_equal = std::strcmp(actual, expected) != 0;
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      is_not_equal = *actual != *expected;
    } else {
      is_not_equal = actual != expected;
    }

    if (!is_not_equal) {
      const std::lock_guard lock{io_mtx_};
      std::cout << "  [" << tag << "] The actual value was equal to the expected one.\n"
                << "    actual:   " << actual << "\n"
                << "    expected: " << expected << "\n";
#ifdef NDEBUG
      throw std::runtime_error{""};
#else
      FAIL();
#endif
    }
  }

  template <class T>
  void
  AssertLT(  //
      const T &lhs,
      const T &rhs,
      const std::string_view &tag)
  {
    bool is_less;
    if constexpr (std::is_same_v<T, char *>) {
      is_less = std::strcmp(lhs, rhs) < 0;
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      is_less = *lhs < *rhs;
    } else {
      is_less = lhs < rhs;
    }

    if (!is_less) {
      const std::lock_guard lock{io_mtx_};
      std::cout << "  [" << tag
                << "] The left-hand side value was larger the right-hand side one.\n"
                << "    lhs: " << lhs << "\n"
                << "    rhs: " << rhs << "\n";
#ifdef NDEBUG
      throw std::runtime_error{""};
#else
      FAIL();
#endif
    }
  }

  void
  PrepareData()
  {
    index_ = std::make_unique<Index>();
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kThreadNum * 2);
  }

  void
  DestroyData()
  {
    ReleaseTestData(keys_);
    ReleaseTestData(payloads_);
  }

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
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (kDisableWriteTest) {
      return 0;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return static_cast<int>(index_->Write(key, payload, GetLength(key), GetLength(payload)));
    }
  }

  auto
  Insert(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (kDisableInsertTest) {
      return 0;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return static_cast<int>(index_->Insert(key, payload, GetLength(key), GetLength(payload)));
    }
  }

  auto
  Update(  //
      [[maybe_unused]] const size_t key_id,
      [[maybe_unused]] const size_t pay_id)
  {
    if constexpr (kDisableUpdateTest) {
      return 0;
    } else {
      const auto &key = keys_.at(key_id);
      const auto &payload = payloads_.at(pay_id);
      return static_cast<int>(index_->Update(key, payload, GetLength(key), GetLength(payload)));
    }
  }

  auto
  Delete(  //
      [[maybe_unused]] const size_t key_id)
  {
    if constexpr (kDisableDeleteTest) {
      return 0;
    } else {
      const auto &key = keys_.at(key_id);
      return static_cast<int>(index_->Delete(key, GetLength(key)));
    }
  }

  auto
  Bulkload()
  {
    if constexpr (kDisableBulkloadTest) {
      return 0;
    } else {
      constexpr size_t kOpsNum = (kExecNum + 1) * kThreadNum;
      std::vector<std::tuple<Key, Payload, size_t, size_t>> entries{};
      entries.reserve(kOpsNum);
      for (size_t i = kThreadNum; i < kOpsNum; ++i) {
        const auto &key = keys_.at(i);
        const auto &payload = payloads_.at(i % kThreadNum);
        entries.emplace_back(key, payload, GetLength(key), GetLength(payload));
      }
      return index_->Bulkload(entries, kThreadNum);
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
        for (size_t i = kExecNum; i > 0; --i) {
          target_ids.emplace_back(kThreadNum * i + w_id);
        }
      } else {
        for (size_t i = 1; i <= kExecNum; ++i) {
          target_ids.emplace_back(kThreadNum * i + w_id);
        }
      }

      if (pattern == kRandom) {
        std::mt19937_64 rand_engine{kRandomSeed};
        std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
      }
    }
    {
      std::unique_lock lock{x_mtx_};
      cond_.wait(lock, [this] { return is_ready_; });
    }
    return target_ids;
  }

  [[nodiscard]] auto
  CreateTargetIDsForConcurrentSMOs()  //
      -> std::vector<size_t>
  {
    std::mt19937_64 rng{kRandomSeed};
    std::uniform_int_distribution<size_t> exec_dist{1, kExecNum};
    std::uniform_int_distribution<size_t> thread_dist{0, kThreadNum / 2 - 1};
    std::vector<size_t> target_ids{};
    {
      std::shared_lock guard{s_mtx_};

      target_ids.reserve(kExecNum);
      for (size_t i = 0; i < kExecNum; ++i) {
        target_ids.emplace_back(kThreadNum * exec_dist(rng) + thread_dist(rng));
      }
    }

    std::unique_lock lock{x_mtx_};
    cond_.wait(lock, [this] { return is_ready_; });

    return target_ids;
  }

  void
  RunMT(  //
      const std::function<void(size_t)> &func)
  {
    std::vector<std::thread> threads{};
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(
          [&](const size_t id) {
            try {
              func(id);
            } catch ([[maybe_unused]] const std::exception &e) {
              no_failure_.store(false, std::memory_order_relaxed);
              ADD_FAILURE();
            }
          },
          i);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{kWaitForThreadCreation});
    {
      std::lock_guard guard{s_mtx_};
      is_ready_ = true;
      cond_.notify_all();
    }
    for (auto &&t : threads) {
      t.join();
    }
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  void
  VerifyRead(  //
      const bool expect_success,
      const bool is_update,
      const AccessPattern pattern)
  {
    if constexpr (kDisableReadTest) {
      return;
    }

    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        const auto &read_val = Read(id);
        if (expect_success) {
          AssertTrue(static_cast<bool>(read_val), "Read: RC");
          if (read_val) {
            const auto expected_val = payloads_.at((is_update) ? w_id + kThreadNum : w_id);
            AssertEQ(expected_val, read_val.value(), "Read: payload");
          }
        } else {
          AssertFalse(static_cast<bool>(read_val), "Read: RC");
        }
      }
    };

    std::cout << "  [dbgroup] read...\n";
    RunMT(mt_worker);
  }

  void
  VerifyScan(  //
      [[maybe_unused]] const bool expect_success,
      [[maybe_unused]] const bool is_update)
  {
    if constexpr (kDisableScanTest) {
      return;
    }

    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      size_t begin_id = kThreadNum + kExecNum * w_id;
      const auto &begin_k = keys_.at(begin_id);
      const auto &begin_key = std::make_tuple(begin_k, GetLength(begin_k), kRangeClosed);

      size_t end_id = kExecNum * (w_id + 1);
      const auto &end_k = keys_.at(end_id);
      const auto &end_key = std::make_tuple(end_k, GetLength(end_k), kRangeOpened);

      auto &&iter = Scan(begin_key, end_key);
      if (expect_success) {
        for (; iter; ++iter, ++begin_id) {
          if (!no_failure_.load(std::memory_order_relaxed)) break;
          const auto key_id = begin_id;
          const auto val_id = (is_update) ? key_id % kThreadNum + kThreadNum : key_id % kThreadNum;
          const auto &[key, payload] = *iter;
          AssertEQ(keys_.at(key_id), key, "Scan: key");
          AssertEQ(payloads_.at(val_id), payload, "Scan: payload");
        }
        AssertEQ(begin_id, end_id, "Scan: # of records");
      }
      AssertFalse(static_cast<bool>(iter), "Scan: iterator reach end");
    };

    std::cout << "  [dbgroup] scan...\n";
    RunMT(mt_worker);
  }

  void
  VerifyWrite(  //
      const bool is_update,
      const AccessPattern pattern)
  {
    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        const auto pay_id = (is_update) ? w_id + kThreadNum : w_id;
        AssertEQ(Write(id, pay_id), 0, "Write: RC");
      }
    };

    std::cout << "  [dbgroup] write...\n";
    RunMT(mt_worker);
  }

  void
  VerifyInsert(  //
      const bool expect_success,
      const bool is_update,
      const AccessPattern pattern)
  {
    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        const auto pay_id = (is_update) ? w_id + kThreadNum : w_id;
        if (expect_success) {
          AssertEQ(Insert(id, pay_id), 0, "Insert: RC");
        } else {
          AssertNE(Insert(id, pay_id), 0, "Insert: RC");
        }
      }
    };

    std::cout << "  [dbgroup] insert...\n";
    RunMT(mt_worker);
  }

  void
  VerifyUpdate(  //
      const bool expect_success,
      const AccessPattern pattern)
  {
    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        const auto pay_id = w_id + kThreadNum;
        if (expect_success) {
          AssertEQ(Update(id, pay_id), 0, "Update: RC");
        } else {
          AssertNE(Update(id, pay_id), 0, "Update: RC");
        }
      }
    };

    std::cout << "  [dbgroup] update...\n";
    RunMT(mt_worker);
  }

  void
  VerifyDelete(  //
      const bool expect_success,
      const AccessPattern pattern)
  {
    if (!no_failure_.load(std::memory_order_relaxed)) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, pattern)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        if (expect_success) {
          AssertEQ(Delete(id), 0, "Delete: RC");
        } else {
          AssertNE(Delete(id), 0, "Delete: RC");
        }
      }
    };

    std::cout << "  [dbgroup] delete...\n";
    RunMT(mt_worker);
  }

  void
  VerifyBulkload()
  {
    if (!no_failure_.load(std::memory_order_relaxed)) return;

    AssertEQ(static_cast<int>(Bulkload()), 0, "Bulkload: RC");
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

    PrepareData();

    VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(kExpectSuccess, pattern);
    if (write_twice) VerifyWrite(kWriteTwice, pattern);
    VerifyRead(kExpectSuccess, write_twice, pattern);
    VerifyScan(kExpectSuccess, write_twice);

    ReleaseTestData(keys_);
    ReleaseTestData(payloads_);
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

    const auto expect_success = !with_delete || write_twice;
    const auto is_updated = with_delete && write_twice;

    VerifyInsert(kExpectSuccess, !kWriteTwice, pattern);
    if (with_delete) VerifyDelete(kExpectSuccess, pattern);
    if (write_twice) VerifyInsert(with_delete, write_twice, pattern);
    VerifyRead(expect_success, is_updated, pattern);
    VerifyScan(expect_success, is_updated);

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

    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(with_write, pattern);
    VerifyUpdate(expect_success, pattern);
    VerifyRead(expect_success, kWriteTwice, pattern);
    VerifyScan(expect_success, kWriteTwice);

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

    const auto expect_success = with_write && !with_delete;

    if (with_write) VerifyWrite(!kWriteTwice, pattern);
    if (with_delete) VerifyDelete(with_write, pattern);
    VerifyDelete(expect_success, pattern);
    VerifyRead(kExpectFailed, !kWriteTwice, pattern);
    VerifyScan(kExpectFailed, !kWriteTwice);

    DestroyData();
  }

  void
  VerifyConcurrentSMOs()
  {
    constexpr size_t kRepeatNum = 5;
    constexpr size_t kReadThread = kThreadNum / 2;
    constexpr size_t kScanThread = kThreadNum * 3 / 4;
    std::atomic_size_t counter{};

    if (kDisableWriteTest          //
        || kDisableDeleteTest      //
        || kDisableScanTest        //
        || (kThreadNum % 4) != 0)  //
    {
      GTEST_SKIP();
    }

    auto read_proc = [&]() -> void {
      for (const auto id : CreateTargetIDsForConcurrentSMOs()) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        const auto &read_val = Read(id);
        if (read_val) {
          AssertEQ(payloads_.at(id % kReadThread), read_val.value(), "Read value");
        }
      }
    };

    auto scan_proc = [&]() -> void {
      Key prev_key{};
      if constexpr (IsVarLen<Key>()) {
        prev_key = reinterpret_cast<Key>(::operator new(kVarDataLength));
      }
      while (counter < kReadThread) {
        if constexpr (IsVarLen<Key>()) {
          memcpy(prev_key, keys_.at(0), GetLength<Key>(keys_.at(0)));
        } else {
          prev_key = keys_.at(0);
        }
        for (auto &&iter = Scan(); iter; ++iter) {
          if (!no_failure_.load(std::memory_order_relaxed)) break;
          const auto &[key, payload] = *iter;
          AssertLT(prev_key, key, "Scan key");
          if constexpr (IsVarLen<Key>()) {
            memcpy(prev_key, key, GetLength<Key>(key));
          } else {
            prev_key = key;
          }
        }
      }
      if constexpr (IsVarLen<Key>()) {
        ::operator delete(prev_key);
      }
    };

    auto write_proc = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, kRandom)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        AssertEQ(Write(id, w_id), 0, "Write: RC");
      }
      counter += 1;
    };

    auto delete_proc = [&](const size_t w_id) -> void {
      for (const auto id : CreateTargetIDs(w_id, kRandom)) {
        if (!no_failure_.load(std::memory_order_relaxed)) break;
        AssertEQ(Delete(id), 0, "Delete: RC");
      }
      counter += 1;
    };

    auto init_worker = [&](const size_t w_id) -> void {
      if (w_id < kReadThread && (w_id % 2) == 0) {
        write_proc(w_id);
      }
    };

    auto even_delete_worker = [&](const size_t w_id) -> void {
      if (w_id >= kScanThread) {
        scan_proc();
      } else if (w_id >= kReadThread) {
        read_proc();
      } else if (w_id % 2 == 0) {
        delete_proc(w_id);
      } else {
        write_proc(w_id);
      }
    };

    auto odd_delete_worker = [&](const size_t w_id) -> void {
      if (w_id >= kScanThread) {
        scan_proc();
      } else if (w_id >= kReadThread) {
        read_proc();
      } else if (w_id % 2 == 0) {
        write_proc(w_id);
      } else {
        delete_proc(w_id);
      }
    };

    PrepareData();
    std::cout << "  [dbgroup] initialization...\n";
    RunMT(init_worker);
    for (size_t i = 0; i < kRepeatNum; ++i) {
      if (!no_failure_.load(std::memory_order_relaxed)) break;
      std::cout << "  [dbgroup] repeat #" << i << "...\n";
      counter = 0;
      RunMT(even_delete_worker);
      counter = 0;
      RunMT(odd_delete_worker);
    }
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

    auto expect_success = true;
    auto is_updated = false;

    std::cout << "  [dbgroup] bulkload...\n";
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

    DestroyData();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// a flag for indicating that there is no assertion.
  std::atomic_bool no_failure_{true};

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

  /// a mutex for outputting messages
  std::mutex io_mtx_{};

  /// a flag for indicating ready.
  bool is_ready_{false};

  /// a condition variable for notifying worker threads.
  std::condition_variable cond_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
