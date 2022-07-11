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

#ifndef INDEX_FIXTURES_INDEX_FIXTURE_HPP
#define INDEX_FIXTURES_INDEX_FIXTURE_HPP

#include <algorithm>
#include <memory>
#include <random>

// external libraries
#include "gtest/gtest.h"

// project libraries
#include "common.hpp"

namespace dbgroup::index::test
{
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
class IndexFixture : public testing::Test
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
  static constexpr size_t kRecNumWithoutSMOs = 30;
  static constexpr size_t kRecNumWithLeafSMOs = 1000;
  static constexpr size_t kRecNumWithInternalSMOs = 30000;
  static constexpr size_t kKeyNum = kExecNum + 2;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    keys_ = PrepareTestData<Key>(kKeyNum);
    payloads_ = PrepareTestData<Payload>(kKeyNum);

    index_ = std::make_unique<Index>();
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
      for (int64_t i = rec_num - 1; i >= 0; --i) {
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

  void
  FillIndex()
  {
    for (size_t i = 0; i < kExecNum; ++i) {
      Write(i, i);
    }
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success,
      const bool write_twice = false)
  {
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;

      const auto read_val = index_->Read(keys_.at(key_id));
      if (expect_success) {
        EXPECT_TRUE(read_val);

        const auto expected_val = payloads_.at(pay_id);
        const auto actual_val = read_val.value();
        EXPECT_TRUE(IsEqual<PayComp>(expected_val, actual_val));
      } else {
        EXPECT_FALSE(read_val);
      }
    }
  }

  void
  VerifyScan(  //
      const std::optional<std::pair<size_t, bool>> begin_ref,
      const std::optional<std::pair<size_t, bool>> end_ref,
      const bool expect_success = true,
      const bool write_twice = false)
  {
    std::optional<std::pair<const Key &, bool>> begin_key = std::nullopt;
    size_t begin_pos = 0;
    if (begin_ref) {
      auto &&[begin_id, begin_closed] = *begin_ref;
      begin_key.emplace(keys_.at(begin_id), begin_closed);
      begin_pos = (begin_closed) ? begin_id : begin_id + 1;
    }

    std::optional<std::pair<const Key &, bool>> end_key = std::nullopt;
    size_t end_pos = 0;
    if (end_ref) {
      auto &&[end_id, end_closed] = *end_ref;
      end_key.emplace(keys_.at(end_id), end_closed);
      end_pos = (end_closed) ? end_id + 1 : end_id;
    }

    auto &&iter = index_->Scan(begin_key, end_key);
    if (expect_success) {
      for (; iter.HasNext(); ++iter, ++begin_pos) {
        const auto &[key, payload] = *iter;
        const auto val_id = (write_twice) ? begin_pos + 1 : begin_pos;
        EXPECT_TRUE(IsEqual<KeyComp>(keys_.at(begin_pos), key));
        EXPECT_TRUE(IsEqual<PayComp>(payloads_.at(val_id), payload));
      }
      if (end_ref) {
        EXPECT_EQ(begin_pos, end_pos);
      }
    }
    EXPECT_FALSE(iter.HasNext());
  }

  void
  VerifyWrite(  //
      const std::vector<size_t> &target_ids,
      const bool write_twice = false)
  {
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;

      const auto rc = Write(key_id, pay_id);
      EXPECT_EQ(rc, 0);
    }
  }

  void
  VerifyInsert(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success,
      const bool write_twice = false)
  {
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = (write_twice) ? key_id + 1 : key_id;

      const auto rc = Insert(key_id, pay_id);
      if (expect_success) {
        EXPECT_EQ(rc, 0);
      } else {
        EXPECT_NE(rc, 0);
      }
    }
  }

  void
  VerifyUpdate(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success)
  {
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);
      const auto pay_id = key_id + 1;

      const auto rc = Update(key_id, pay_id);
      if (expect_success) {
        EXPECT_EQ(rc, 0);
      } else {
        EXPECT_NE(rc, 0);
      }
    }
  }

  void
  VerifyDelete(  //
      const std::vector<size_t> &target_ids,
      const bool expect_success)
  {
    for (size_t i = 0; i < target_ids.size(); ++i) {
      const auto key_id = target_ids.at(i);

      const auto rc = Delete(key_id);
      if (expect_success) {
        EXPECT_EQ(rc, 0);
      } else {
        EXPECT_NE(rc, 0);
      }
    }
  }

  void
  VerifyBulkload()
  {
    std::vector<Record> entries{};
    entries.reserve(kExecNum);
    for (size_t i = 0; i < kExecNum; ++i) {
      entries.emplace_back(keys_.at(i), payloads_.at(i), kKeyLen);
    }

    const auto rc = index_->Bulkload(entries, 1);
    EXPECT_EQ(rc, 0);
  }

  void
  VerifyWritesWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern,
      const size_t ops_num = kExecNum)
  {
    const auto &target_ids = CreateTargetIDs(ops_num, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(ops_num, kRangeOpened);

    VerifyWrite(target_ids);
    if (with_delete) VerifyDelete(target_ids, kExpectSuccess);
    if (write_twice) VerifyWrite(target_ids, kWriteTwice);
    VerifyRead(target_ids, !with_delete || write_twice, write_twice);
    VerifyScan(begin_ref, end_ref, kExpectSuccess, write_twice);
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const AccessPattern pattern)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_success = !with_delete || write_twice;
    const bool is_updated = write_twice && with_delete;

    VerifyInsert(target_ids, kExpectSuccess);
    if (with_delete) VerifyDelete(target_ids, kExpectSuccess);
    if (write_twice) VerifyInsert(target_ids, with_delete, kWriteTwice);
    VerifyRead(target_ids, expect_success, is_updated);
    VerifyScan(begin_ref, end_ref, expect_success, is_updated);
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_update = with_write && !with_delete;

    if (with_write) VerifyWrite(target_ids);
    if (with_delete) VerifyDelete(target_ids, with_write);
    VerifyUpdate(target_ids, expect_update);
    VerifyRead(target_ids, expect_update, kWriteTwice);
    VerifyScan(begin_ref, end_ref, expect_update, kWriteTwice);
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const AccessPattern pattern)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    const auto expect_delete = with_write && !with_delete;

    if (with_write) VerifyWrite(target_ids);
    if (with_delete) VerifyDelete(target_ids, with_write);
    VerifyDelete(target_ids, expect_delete);
    VerifyRead(target_ids, kExpectFailed);
    VerifyScan(begin_ref, end_ref, kExpectFailed);
  }

  void
  VerifyBulkloadWith(  //
      const WriteOperation write_ops,
      const AccessPattern pattern)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, pattern);
    const auto &begin_ref = std::make_pair(0, kRangeClosed);
    const auto &end_ref = std::make_pair(kExecNum, kRangeOpened);
    auto expect_success = true;
    auto is_updated = false;

    VerifyBulkload();
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
    VerifyRead(target_ids, expect_success, is_updated);
    VerifyScan(begin_ref, end_ref, expect_success, is_updated);
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
};

}  // namespace dbgroup::index::test

#endif  // INDEX_FIXTURES_INDEX_FIXTURE_HPP
