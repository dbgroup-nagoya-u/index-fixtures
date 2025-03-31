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
#include <bit>
#include <cassert>
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

  static constexpr size_t kThreadNum = (DBGROUP_TEST_THREAD_NUM);
  static constexpr size_t kNodeNum = (DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM);
  static constexpr size_t kWorkerNum = kThreadNum * kNodeNum;
  static constexpr size_t kKeyNum = kExecNum * kWorkerNum;
  static constexpr size_t kNodeID = (DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID);
  static constexpr size_t kWaitForThreadCreation = 100;

  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    ready_num_ = 0;
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
  PrepareData()
  {
    index_ = std::make_unique<Index>();
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kWorkerNum * 2);
  }

  void
  DestroyData()
  {
    ReleaseTestData(keys_);
    ReleaseTestData(payloads_);
  }

  static constexpr auto
  GetTargetID(  //
      const size_t i,
      const size_t w_id)  //
      -> size_t
  {
    assert(i < kExecNum);
    assert(w_id < kWorkerNum);
    return kWorkerNum * i + w_id;
  }

  [[nodiscard]] auto
  CreateIDs(  //
      const size_t w_id,
      const AccessPattern pattern)  //
      -> std::vector<size_t>
  {
    std::vector<size_t> target_ids{};
    target_ids.reserve(kExecNum);
    for (size_t i = 0; i < kExecNum; ++i) {
      target_ids.emplace_back(GetTargetID(i, w_id));
    }

    if (pattern == kReverse) {
      std::reverse(target_ids.begin(), target_ids.end());
    } else if (pattern == kRandom) {
      std::mt19937_64 rand_engine{kRandomSeed};
      std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
    }

    ++ready_num_;
    std::unique_lock lock{x_mtx_};
    cond_.wait(lock, [this] { return is_ready_; });

    return target_ids;
  }

  [[nodiscard]] auto
  CreateIDsForConcurrentSMOs()  //
      -> std::vector<size_t>
  {
    std::mt19937_64 rng{kRandomSeed};
    std::uniform_int_distribution<size_t> exec_dist{0, kExecNum - 1};
    std::uniform_int_distribution<size_t> worker_dist{0, kThreadNum / 2 - 1};

    std::vector<size_t> target_ids{};
    target_ids.reserve(kExecNum);
    for (size_t i = 0; i < kExecNum; ++i) {
      target_ids.emplace_back(GetTargetID(exec_dist(rng), worker_dist(rng)));
    }

    ++ready_num_;
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
          [&](const size_t w_id) {
            try {
              func(w_id);
            } catch ([[maybe_unused]] const std::exception &e) {
              no_failure_ = false;
              ADD_FAILURE();
            }
          },
          i + kThreadNum * kNodeID);
    }

    while (ready_num_ < kThreadNum) {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    {
      std::lock_guard guard{x_mtx_};
      is_ready_ = true;
    }
    cond_.notify_all();
    for (auto &&t : threads) {
      t.join();
    }
    ready_num_ = 0;

    if constexpr (kNodeNum > 1) {
      index_->Barrier();
    }
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
      entries.reserve(kKeyNum);
      for (size_t i = 0; i < kKeyNum; ++i) {
        const auto &key = keys_.at(i);
        const auto &payload = payloads_.at(i % kWorkerNum);
        entries.emplace_back(key, payload, GetLength(key), GetLength(payload));
      }
      return index_->Bulkload(entries, kThreadNum);
    }
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  void
  VerifyRead(  //
      const bool expect_success,
      const bool is_update)
  {
    if (kDisableReadTest || !no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      size_t begin_id = kExecNum * w_id;
      size_t end_id = kExecNum * (w_id + 1);
      ++ready_num_;

      for (size_t i = begin_id; i < end_id; ++i) {
        if (!no_failure_) break;
        const auto &read_val = Read(i);
        if (expect_success) {
          AssertTrue(static_cast<bool>(read_val), "Read: RC");
          if (read_val) {
            const auto val_id = i % kWorkerNum + (is_update ? kWorkerNum : 0);
            const auto expected_val = payloads_.at(val_id);
            AssertEQ<PayComp>(expected_val, read_val.value(), "Read: payload");
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
    if (kDisableScanTest || !no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      size_t begin_id = kExecNum * w_id;
      const auto &begin_k = keys_.at(begin_id);
      const auto &begin_key = std::make_tuple(begin_k, GetLength(begin_k), kRangeClosed);
      size_t end_id = kExecNum * (w_id + 1);
      const auto &end_k = keys_.at(end_id);
      const auto &end_key = std::make_tuple(end_k, GetLength(end_k), kRangeOpened);
      ++ready_num_;

      auto &&iter = Scan(begin_key, end_key);
      if (expect_success) {
        for (; iter; ++iter, ++begin_id) {
          if (!no_failure_) return;
          const auto key_id = begin_id;
          const auto val_id = key_id % kWorkerNum + (is_update ? kWorkerNum : 0);
          const auto &[key, payload] = *iter;
          AssertEQ<KeyComp>(keys_.at(key_id), key, "Scan: key");
          AssertEQ<PayComp>(payloads_.at(val_id), payload, "Scan: payload");
        }
        AssertEQ<std::less<size_t>>(begin_id, end_id, "Scan: # of records");
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
    if (!no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, pattern)) {
        if (!no_failure_) return;
        const auto pay_id = (is_update) ? w_id + kWorkerNum : w_id;
        AssertEQ<RCComp>(Write(id, pay_id), kSuccess, "Write: RC");
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
    if (!no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, pattern)) {
        if (!no_failure_) return;
        const auto pay_id = (is_update) ? w_id + kWorkerNum : w_id;
        if (expect_success) {
          AssertEQ<RCComp>(Insert(id, pay_id), kSuccess, "Insert: RC");
        } else {
          AssertEQ<RCComp>(Insert(id, pay_id), kKeyExist, "Insert: RC");
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
    if (!no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, pattern)) {
        if (!no_failure_) return;
        const auto pay_id = w_id + kWorkerNum;
        if (expect_success) {
          AssertEQ<RCComp>(Update(id, pay_id), kSuccess, "Update: RC");
        } else {
          AssertEQ<RCComp>(Update(id, pay_id), kKeyNotExist, "Update: RC");
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
    if (!no_failure_) return;

    auto mt_worker = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, pattern)) {
        if (!no_failure_) return;
        if (expect_success) {
          AssertEQ<RCComp>(Delete(id), kSuccess, "Delete: RC");
        } else {
          AssertEQ<RCComp>(Delete(id), kKeyNotExist, "Delete: RC");
        }
      }
    };

    std::cout << "  [dbgroup] delete...\n";
    RunMT(mt_worker);
  }

  void
  VerifyBulkload()
  {
    if (!no_failure_) return;

    AssertEQ<RCComp>(Bulkload(), kSuccess, "Bulkload: RC");
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
    VerifyRead(kExpectSuccess, write_twice);
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
    VerifyRead(expect_success, is_updated);
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
    VerifyRead(expect_success, kWriteTwice);
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
    VerifyRead(kExpectFailed, !kWriteTwice);
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
      for (const auto id : CreateIDsForConcurrentSMOs()) {
        if (!no_failure_) return;
        const auto &read_val = Read(id);
        if (read_val) {
          AssertEQ<PayComp>(payloads_.at(id % kReadThread), read_val.value(), "Read value");
        }
      }
    };

    auto scan_proc = [&]() -> void {
      Key prev_key{};
      if constexpr (IsVarLenData<Key>()) {
        prev_key = std::bit_cast<Key>(::operator new(kVarDataLength));
      }
      ++ready_num_;
      while (counter < kReadThread) {
        if constexpr (IsVarLenData<Key>()) {
          memcpy(prev_key, keys_.at(0), GetLength<Key>(keys_.at(0)));
        } else {
          prev_key = keys_.at(0);
        }
        for (auto &&iter = Scan(); iter; ++iter) {
          if (!no_failure_) return;
          const auto &[key, payload] = *iter;
          AssertLT<KeyComp>(prev_key, key, "Scan key");
          if constexpr (IsVarLenData<Key>()) {
            memcpy(prev_key, key, GetLength<Key>(key));
          } else {
            prev_key = key;
          }
        }
      }
      if constexpr (IsVarLenData<Key>()) {
        ::operator delete(prev_key);
      }
    };

    auto write_proc = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, kRandom)) {
        if (!no_failure_) return;
        AssertEQ<RCComp>(Write(id, w_id), kSuccess, "Write: RC");
      }
      counter += 1;
    };

    auto delete_proc = [&](const size_t w_id) -> void {
      for (const auto id : CreateIDs(w_id, kRandom)) {
        if (!no_failure_) return;
        AssertEQ<RCComp>(Delete(id), kSuccess, "Delete: RC");
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
      if (!no_failure_) break;
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
    VerifyRead(expect_success, is_updated);
    VerifyScan(expect_success, is_updated);

    DestroyData();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A flag for indicating that there is no assertion.
  std::atomic_bool no_failure_{true};

  /// @brief Actual keys
  std::vector<Key> keys_{};

  /// @brief Actual payloads
  std::vector<Payload> payloads_{};

  /// @brief An index for testing
  std::unique_ptr<Index> index_{nullptr};

  /// @brief The number of threads that are ready for testing.
  std::atomic_size_t ready_num_{};

  /// @brief A mutex for notifying worker threads.
  std::mutex x_mtx_{};

  /// @brief A flag for indicating ready.
  bool is_ready_{false};

  /// @brief A condition variable for notifying worker threads.
  std::condition_variable cond_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_FIXTURE_MULTI_THREAD_HPP
