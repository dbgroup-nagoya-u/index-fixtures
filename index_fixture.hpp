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
      const bool is_shuffled) const  //
      -> std::vector<size_t>
  {
    std::mt19937_64 rand_engine{kRandomSeed};

    std::vector<size_t> target_ids{};
    target_ids.reserve(rec_num);
    for (size_t i = 0; i < rec_num; ++i) {
      target_ids.emplace_back(i);
    }

    if (is_shuffled) {
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

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_success)
  {
    const auto read_val = index_->Read(keys_.at(key_id));
    if (expect_success) {
      EXPECT_TRUE(read_val);

      const auto expected_val = payloads_.at(expected_id);
      const auto actual_val = read_val.value();
      EXPECT_TRUE(IsEqual<PayComp>(expected_val, actual_val));
    } else {
      EXPECT_FALSE(read_val);
    }
  }

  void
  VerifyScan(  //
      const std::optional<std::pair<size_t, bool>> begin_ref,
      const std::optional<std::pair<size_t, bool>> end_ref)
  {
    // fill an index
    for (size_t i = 0; i < kExecNum; ++i) {
      Write(i, i);
    }

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

    auto iter = index_->Scan(begin_key, end_key);

    for (; iter.HasNext(); ++iter, ++begin_pos) {
      auto [key, payload] = *iter;
      EXPECT_TRUE(IsEqual<KeyComp>(keys_.at(begin_pos), key));
      EXPECT_TRUE(IsEqual<PayComp>(payloads_.at(begin_pos), payload));
    }
    EXPECT_FALSE(iter.HasNext());

    if (end_ref) {
      EXPECT_EQ(begin_pos, end_pos);
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t pay_id)
  {
    const auto rc = Write(key_id, pay_id);

    EXPECT_EQ(rc, 0);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t pay_id,
      const bool expect_success)
  {
    const auto rc = Insert(key_id, pay_id);
    if (expect_success) {
      EXPECT_EQ(rc, 0);
    } else {
      EXPECT_NE(rc, 0);
    }
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t pay_id,
      const bool expect_success)
  {
    const auto rc = Update(key_id, pay_id);
    if (expect_success) {
      EXPECT_EQ(rc, 0);
    } else {
      EXPECT_NE(rc, 0);
    }
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_success)
  {
    const auto rc = Delete(key_id);
    if (expect_success) {
      EXPECT_EQ(rc, 0);
    } else {
      EXPECT_NE(rc, 0);
    }
  }

  void
  VerifyWritesWith(  //
      const bool write_twice,
      const bool with_delete,
      const bool is_shuffled,
      const size_t ops_num = kExecNum)
  {
    const auto &target_ids = CreateTargetIDs(ops_num, is_shuffled);

    for (size_t i = 0; i < ops_num; ++i) {
      const auto id = target_ids.at(i);
      VerifyWrite(id, id);
    }
    if (with_delete) {
      for (size_t i = 0; i < ops_num; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    if (write_twice) {
      for (size_t i = 0; i < ops_num; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id + 1);
      }
    }
    for (size_t i = 0; i < ops_num; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (write_twice) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, kExpectSuccess);
    }
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, is_shuffled);

    for (size_t i = 0; i < kExecNum; ++i) {
      const auto id = target_ids.at(i);
      VerifyInsert(id, id, kExpectSuccess);
    }
    if (with_delete) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    if (write_twice) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto key_id = target_ids.at(i);
        VerifyInsert(key_id, key_id + 1, with_delete);
      }
    }
    for (size_t i = 0; i < kExecNum; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (write_twice && with_delete) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, kExpectSuccess);
    }
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, is_shuffled);
    const auto expect_update = with_write && !with_delete;

    if (with_write) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id);
      }
    }
    if (with_delete) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    for (size_t i = 0; i < kExecNum; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyUpdate(key_id, key_id + 1, expect_update);
    }
    for (size_t i = 0; i < kExecNum; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (expect_update) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, expect_update);
    }
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kExecNum, is_shuffled);
    const auto expect_delete = with_write && !with_delete;

    if (with_write) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id);
      }
    }
    if (with_delete) {
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    for (size_t i = 0; i < kExecNum; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyDelete(key_id, expect_delete);
    }
    for (size_t i = 0; i < kExecNum; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyRead(key_id, key_id, kExpectFailed);
    }
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
