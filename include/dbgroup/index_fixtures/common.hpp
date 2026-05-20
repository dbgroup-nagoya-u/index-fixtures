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

#ifndef DBGROUP_INDEX_FIXTURES_COMMON_HPP
#define DBGROUP_INDEX_FIXTURES_COMMON_HPP

// C++ standard libraries
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <type_traits>
#include <vector>

// external libraries
#include <gtest/gtest.h>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

namespace dbgroup::index
{

template <>
constexpr auto
IsVarLenData<char*>() noexcept  //
    -> bool
{
  return true;
}

namespace test
{
/*############################################################################*
 * Constants for testing
 *############################################################################*/

enum AccessPattern {
  kSequential,
  kReverse,
  kRandom,
};

enum WriteOperation {
  kWrite,
  kUpsert,
  kInsert,
  kUpdate,
  kDelete,
  kWithoutWrite,
};

constexpr size_t kExecNum = (DBGROUP_TEST_EXEC_NUM);

constexpr size_t kRandomSeed = (DBGROUP_TEST_RANDOM_SEED);

constexpr size_t kThreadNum = (DBGROUP_TEST_THREAD_NUM);

constexpr size_t kNodeNum = (DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM);

constexpr size_t kNodeID = (DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID);

constexpr size_t kWorkerNum = kThreadNum * kNodeNum;

constexpr size_t kVarDataLength = (DBGROUP_TEST_MAX_VARLEN_DATA_SIZE);

constexpr int32_t kPadNum = kVarDataLength / 10;

constexpr bool kExpectSuccess = true;

constexpr bool kExpectFailed = false;

constexpr bool kHasRange = true;

constexpr bool kWriteTwice = true;

constexpr bool kWithWrite = true;

constexpr bool kWithDelete = true;

#ifdef DBGROUP_TEST_DISABLE_READ_TEST
constexpr bool kDisableReadTest = true;
#else
constexpr bool kDisableReadTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_SCAN_TEST
constexpr bool kDisableScanTest = true;
#else
constexpr bool kDisableScanTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_SCAN_BACKWARD_TEST
constexpr bool kDisableScanBackwardTest = true;
#else
constexpr bool kDisableScanBackwardTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_WRITE_TEST
constexpr bool kDisableWriteTest = true;
#else
constexpr bool kDisableWriteTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_UPSERT_TEST
constexpr bool kDisableUpsertTest = true;
#else
constexpr bool kDisableUpsertTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_INSERT_TEST
constexpr bool kDisableInsertTest = true;
#else
constexpr bool kDisableInsertTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_UPDATE_TEST
constexpr bool kDisableUpdateTest = true;
#else
constexpr bool kDisableUpdateTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_DELETE_TEST
constexpr bool kDisableDeleteTest = true;
#else
constexpr bool kDisableDeleteTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_BULKLOAD_TEST
constexpr bool kDisableBulkloadTest = true;
#else
constexpr bool kDisableBulkloadTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_RECORD_MERGING
constexpr bool kDisableRecordMerging = true;
#else
constexpr bool kDisableRecordMerging = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST
constexpr bool kDisableScanVerifyTest = true;
#else
constexpr bool kDisableScanVerifyTest = false;
#endif

/*############################################################################*
 * Global utility classes
 *############################################################################*/

template <template <class K, class V, class C, class... Others> class IndexT,
          class KeyT,
          class PayloadT>
struct IndexInfo {
  using Key = KeyT;
  using Payload = PayloadT;
  using Index = IndexT<typename Key::Data, typename Payload::Data, typename Key::Comp>;
};

struct VarData {
  char data[kVarDataLength]{};
};

template <class Key, class Payload>
struct DummyIter {
  constexpr explicit
  operator bool() const noexcept
  {
    return false;
  }

  constexpr auto
  operator*() const noexcept  //
      -> std::pair<Key, Payload>
  {
    return {Key{}, Payload{}};
  }

  constexpr void
  operator++() const
  {
  }

  constexpr void
  PrepareVerifier() const
  {
  }

  [[nodiscard]] constexpr auto
  VerifySnapshot() const  //
      -> bool
  {
    return false;
  }

  [[nodiscard]] constexpr auto
  VerifyNoPhantom() const  //
      -> bool
  {
    return false;
  }
};

/*############################################################################*
 * Type definitions for templated tests
 *############################################################################*/

struct UInt8 {
  using Data = uint64_t;
  using Comp = std::less<Data>;
};

struct Int8 {
  using Data = int64_t;
  using Comp = std::less<Data>;
};

struct UInt4 {
  using Data = uint32_t;
  using Comp = std::less<Data>;
};

struct Int4 {
  using Data = int32_t;
  using Comp = std::less<Data>;
};

struct UInt16 {
  class Data
  {
   public:
    constexpr Data() = default;

    template <std::integral T>
    constexpr Data(  // NOLINT
        const T val) noexcept
        : prefix{static_cast<uint64_t>(val)}
        , suffix{static_cast<uint64_t>(val)}
    {
    }

    ~Data() = default;

    constexpr Data(const Data&) noexcept = default;
    constexpr Data(Data&&) noexcept = default;

    constexpr auto operator=(const Data&) noexcept -> Data& = default;
    constexpr auto operator=(Data&&) noexcept -> Data& = default;

    constexpr auto
    operator=(                          //
        const uint64_t value) noexcept  //
        -> Data&
    {
      prefix = value;
      suffix = value;
      return *this;
    }

    constexpr auto
    operator<(                            //
        const Data& comp) const noexcept  //
        -> bool
    {
      return prefix < comp.prefix;
    }

    constexpr auto
    operator<=(                           //
        const Data& comp) const noexcept  //
        -> bool
    {
      return prefix <= comp.prefix;
    }

    constexpr auto
    operator>(                            //
        const Data& comp) const noexcept  //
        -> bool
    {
      return prefix > comp.prefix;
    }

    constexpr auto
    operator>=(                           //
        const Data& comp) const noexcept  //
        -> bool
    {
      return prefix >= comp.prefix;
    }

    constexpr auto
    operator==(                           //
        const Data& comp) const noexcept  //
        -> bool
    {
      return prefix == comp.prefix;
    }

    constexpr auto
    operator+(                           //
        const Data& rhs) const noexcept  //
        -> Data
    {
      Data ret{*this};
      ret.prefix += rhs.prefix;
      ret.suffix += rhs.suffix;
      return ret;
    }

    constexpr explicit
    operator uint32_t() const noexcept
    {
      return static_cast<uint32_t>(prefix);
    }

    uint64_t prefix{};
    uint64_t suffix{};
  };

  using Comp = std::less<Data>;
};

struct Ptr {
  using Data = uint64_t*;

  struct Comp {
    constexpr auto
    operator()(  //
        const uint64_t* const a,
        const uint64_t* const b) const noexcept  //
        -> bool
    {
      if (a == nullptr) return false;
      if (b == nullptr) return true;
      return *a < *b;
    }
  };
};

struct Var {
  using Data = char*;

  struct Comp {
    constexpr auto
    operator()(  //
        const char* const a,
        const char* const b) const noexcept  //
        -> bool
    {
      if (a == nullptr) return false;
      if (b == nullptr) return true;
      return std::strcmp(a, b) < 0;
    }
  };
};

/*############################################################################*
 * Global utility functions
 *############################################################################*/

template <class T>
auto
GetLength(                   //
    const T& data) noexcept  //
    -> size_t
{
  if constexpr (std::is_same_v<T, char*>) {
    return std::strlen(data) + 1;
  } else {
    return sizeof(T);
  }
}

inline void
CreateDummyString(  // NOLINT
    const size_t data_num,
    const size_t base_level,
    std::vector<char*>& data_vec,
    VarData var_arr[],
    size_t& i,
    VarData& base)
{
  if (base_level > kVarDataLength - 2) return;

  constexpr char kPad = '0';
  constexpr int32_t kDigitsNum = 10;
  for (int32_t j = 0; j < kDigitsNum && i < data_num; ++j) {
    auto level = base_level;
    base.data[level++] = static_cast<char>(kPad + j);
    base.data[level] = '\0';

    auto* const data = std::bit_cast<char*>(var_arr + i);
    std::memcpy(data, &base, kVarDataLength);
    data_vec.emplace_back(data);
    if (++i >= data_num) return;
    if (level > kVarDataLength - kPadNum) continue;

    for (int32_t k = 0; k < kPadNum; ++k) {
      base.data[level++] = kPad;
    }
    CreateDummyString(data_num, level, data_vec, var_arr, i, base);
  }
}

template <class T>
auto
PrepareTestData(            //
    const size_t data_num)  //
    -> std::vector<T>
{
  std::vector<T> data_vec{};
  data_vec.reserve(data_num);

  if constexpr (std::is_same_v<T, char*>) {
    auto* const var_arr = new VarData[data_num];
    VarData base{};
    std::memset(static_cast<void*>(base.data), 0, kVarDataLength);

    size_t count = 0;
    CreateDummyString(data_num, 0, data_vec, var_arr, count, base);
  } else if constexpr (std::is_same_v<T, uint64_t*>) {
    auto* const ptr_arr = new uint64_t[data_num];
    for (size_t i = 0; i < data_num; ++i) {
      ptr_arr[i] = i;
      data_vec.emplace_back(ptr_arr + i);
    }
  } else {
    for (size_t i = 0; i < data_num; ++i) {
      data_vec.emplace_back(i);
    }
  }

  return data_vec;
}

template <class T>
void
ReleaseTestData(  //
    [[maybe_unused]] std::vector<T>& data_vec)
{
  if constexpr (std::is_same_v<T, char*>) {
    delete[] std::bit_cast<VarData*>(data_vec.front());
  } else if constexpr (std::is_same_v<T, uint64_t*>) {
    delete[] data_vec.front();
  }
}

template <class T>
constexpr auto
AddMerger(  //
    const T& lhs,
    const T& rhs)  //
    -> T
{
  return lhs + rhs;
}

}  // namespace test
}  // namespace dbgroup::index

inline auto
operator<<(  //
    std::ostream& os,
    const ::dbgroup::index::test::UInt16::Data& obj)  //
    -> std::ostream&
{
  os << static_cast<uint32_t>(obj);
  return os;
}

#endif  // DBGROUP_INDEX_FIXTURES_COMMON_HPP
