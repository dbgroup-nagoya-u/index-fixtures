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

#ifndef DBGROUP_INDEX_FIXTURES_INDEX_WRAPPER_HPP
#define DBGROUP_INDEX_FIXTURES_INDEX_WRAPPER_HPP

// C++ standard libraries
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
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
class IndexWrapper
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using Index = typename IndexInfo::Index;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  IndexWrapper() = default;

  IndexWrapper(  //
      const size_t rec_num)
      : index_{std::make_unique<Index>()}, keys_{PrepareTestData<Key>(rec_num)}
  {
    if constexpr (!kDisableRecordMerging) {
      index_->SetRecordMerger(AddMerger);
    }
  }

  ~IndexWrapper()
  {  //
    ReleaseTestData(keys_);
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
      std::optional<Payload> ret;
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        ret = index_->Read(key, GetLength(key));
      }) << "[Read: runtime error]";
      return ret;
    }
  }

  auto
  Scan(  //
      [[maybe_unused]] const std::optional<size_t> &b_id = std::nullopt,
      [[maybe_unused]] const bool b_closed = true,
      [[maybe_unused]] const std::optional<size_t> &e_id = std::nullopt,
      [[maybe_unused]] const bool e_closed = true)
  {
    if constexpr (kDisableScanTest) {
      return DummyIter<Key, Payload>{};
    } else {
      ScanKey b_key{};
      if (b_id) {
        const auto &key = keys_.at(*b_id);
        b_key = std::make_tuple(key, GetLength(key), b_closed);
      }
      ScanKey e_key{};
      if (b_id) {
        const auto &key = keys_.at(*e_id);
        e_key = std::make_tuple(key, GetLength(key), e_closed);
      }

      decltype(index_->Scan()) ret{};
      EXPECT_NO_THROW({
        ret = index_->Scan(b_key, e_key);  //
      }) << "[Scan: runtime error]";
      return ret;
    }
  }

  void
  Write(  //
      [[maybe_unused]] const size_t key_id)
  {
    if constexpr (!kDisableWriteTest) {
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        index_->Write(key, 1, GetLength(key));  //
      }) << "[Write: runtime error]";
    }
  }

  auto
  Upsert(                                    //
      [[maybe_unused]] const size_t key_id)  //
      -> std::optional<Payload>
  {
    if constexpr (kDisableUpsertTest) {
      return std::nullopt;
    } else {
      std::optional<Payload> ret;
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        ret = index_->Upsert(key, 1, GetLength(key));  //
      }) << "[Upsert: runtime error]";
      return ret;
    }
  }

  auto
  Insert(                                    //
      [[maybe_unused]] const size_t key_id)  //
      -> std::optional<Payload>
  {
    if constexpr (kDisableInsertTest) {
      return std::nullopt;
    } else {
      std::optional<Payload> ret;
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        ret = index_->Insert(key, 1, GetLength(key));  //
      }) << "[Insert: runtime error]";
      return ret;
    }
  }

  auto
  Update(                                    //
      [[maybe_unused]] const size_t key_id)  //
      -> std::optional<Payload>
  {
    if constexpr (kDisableUpdateTest) {
      return std::nullopt;
    } else {
      std::optional<Payload> ret;
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        ret = index_->Update(key, 1, GetLength(key));  //
      }) << "[Update: runtime error]";
      return ret;
    }
  }

  auto
  Delete(                                    //
      [[maybe_unused]] const size_t key_id)  //
      -> std::optional<Payload>
  {
    if constexpr (kDisableDeleteTest) {
      return std::nullopt;
    } else {
      std::optional<Payload> ret;
      EXPECT_NO_THROW({
        const auto &key = keys_.at(key_id);
        ret = index_->Delete(key, GetLength(key));  //
      }) << "[Delete: runtime error]";
      return ret;
    }
  }

  void
  Bulkload()
  {
    if constexpr (!kDisableBulkloadTest) {
      std::vector<std::tuple<Key, Payload, size_t>> entries{};
      entries.reserve(kExecNum);
      for (size_t i = 0; i < kExecNum; ++i) {
        const auto &key = keys_.at(i);
        entries.emplace_back(key, 1, GetLength(key));
      }

      EXPECT_NO_THROW({
        index_->Bulkload(entries, kThreadNum);  //
      }) << "[Bulkload: runtime error]";
    }
  }

  /*############################################################################
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      std::is_same_v<Payload, uint32_t> || std::is_same_v<Payload, uint64_t>,
      "Our tests assume that payloads are unsigned 32/64-bits integers.");

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief An index for testing
  std::unique_ptr<Index> index_{};

  /// @brief Actual keys.
  std::vector<Key> keys_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_WRAPPER_HPP
