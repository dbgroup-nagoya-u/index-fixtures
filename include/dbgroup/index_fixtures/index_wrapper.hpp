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
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

// external libraries
#include <gtest/gtest.h>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

// local sources
#include "common.hpp"

namespace dbgroup::index::test
{
/*############################################################################*
 * Fixture class definition
 *############################################################################*/

template <class IndexInfo>
class IndexWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Key = typename IndexInfo::Key::Data;
  using Payload = typename IndexInfo::Payload::Data;
  using Index = typename IndexInfo::Index;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  IndexWrapper() = default;

  explicit IndexWrapper(  //
      const std::vector<Key> &keys)
      : index_{std::make_unique<Index>()}
      , keys_{keys}
  {
  }

  IndexWrapper(const IndexWrapper &) = delete;
  IndexWrapper(IndexWrapper &&) = delete;

  auto operator=(const IndexWrapper &) -> IndexWrapper & = delete;
  auto operator=(IndexWrapper &&) -> IndexWrapper & = delete;

  ~IndexWrapper() = default;

  /*##########################################################################*
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
      if (e_id) {
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

  auto
  ScanBackward(  //
      [[maybe_unused]] const std::optional<size_t> &b_id = std::nullopt,
      [[maybe_unused]] const bool b_closed = true,
      [[maybe_unused]] const std::optional<size_t> &e_id = std::nullopt,
      [[maybe_unused]] const bool e_closed = true)
  {
    if constexpr (kDisableScanBackwardTest) {
      return DummyIter<Key, Payload>{};
    } else {
      ScanKey b_key{};
      if (b_id) {
        const auto &key = keys_.at(*b_id);
        b_key = std::make_tuple(key, GetLength(key), b_closed);
      }
      ScanKey e_key{};
      if (e_id) {
        const auto &key = keys_.at(*e_id);
        e_key = std::make_tuple(key, GetLength(key), e_closed);
      }

      decltype(index_->ScanBackward()) ret{};
      EXPECT_NO_THROW({
        ret = index_->ScanBackward(b_key, e_key);  //
      }) << "[ScanBackward: runtime error]";
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
        if constexpr (kDisableRecordMerging) {
          index_->Write(key, 1, GetLength(key));
        } else {
          index_->Write(key, 1, GetLength(key), AddMerger);
        }
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
        if constexpr (kDisableRecordMerging) {
          ret = index_->Upsert(key, 1, GetLength(key));
        } else {
          ret = index_->Upsert(key, 1, GetLength(key), AddMerger);
        }
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
        if constexpr (kDisableRecordMerging) {
          ret = index_->Update(key, 1, GetLength(key));
        } else {
          ret = index_->Update(key, 1, GetLength(key), AddMerger, sizeof(Payload));
        }
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

  void
  Barrier()
  {
    if constexpr (kNodeNum > 1) {
      index_->Barrier();
    }
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief An index for testing
  std::unique_ptr<Index> index_{};

  /// @brief Actual keys.
  const std::vector<Key> &keys_{};
};

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_INDEX_WRAPPER_HPP
