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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <type_traits>
#include <vector>

// external libraries
#include "gtest/gtest.h"

/*##############################################################################
 * Classes for testing
 *############################################################################*/

/**
 * @brief A dummy class for representing record iterator.
 *
 */
template <class Key, class Payload>
struct DummyIter {
  constexpr explicit
  operator bool()
  {
    return false;
  }

  constexpr auto
  operator*() const  //
      -> std::pair<Key, Payload>
  {
    return {Key{}, Payload{}};
  }

  constexpr void
  operator++()
  {
  }
};

/**
 * @brief An example class to represent CAS-updatable data.
 *
 */
class MyClass
{
 public:
  constexpr MyClass() : data{}, control_bits{0} {}

  constexpr explicit MyClass(  //
      const uint64_t val)
      : data{val}, control_bits{0}
  {
  }

  ~MyClass() = default;

  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;

  constexpr auto operator=(const MyClass &) -> MyClass & = default;
  constexpr auto operator=(MyClass &&) -> MyClass & = default;

  constexpr auto
  operator=(                 //
      const uint64_t value)  //
      -> MyClass &
  {
    data = value;
    return *this;
  }

  constexpr auto
  operator<(                      //
      const MyClass &comp) const  //
      -> bool
  {
    return data < comp.data;
  }

  constexpr auto
  operator==(                     //
      const MyClass &comp) const  //
      -> bool
  {
    return data == comp.data;
  }

  uint64_t data : 61;
  uint64_t control_bits : 3;  // NOLINT
};

auto
operator<<(  //
    std::ostream &os,
    const MyClass &obj)  //
    -> std::ostream &
{
  os << obj.data;
  return os;
}

namespace dbgroup::index::test
{
/*##############################################################################
 * Constants for testing
 *############################################################################*/

enum AccessPattern {
  kSequential,
  kReverse,
  kRandom,
};

enum WriteOperation {
  kWrite,
  kInsert,
  kUpdate,
  kDelete,
  kWithoutWrite,
};

constexpr size_t kExecNum = DBGROUP_TEST_EXEC_NUM;

constexpr size_t kRandomSeed = DBGROUP_TEST_RANDOM_SEED;

constexpr size_t kVarDataLength = 18;

constexpr bool kExpectSuccess = true;

constexpr bool kExpectFailed = false;

constexpr bool kHasRange = true;

constexpr bool kRangeClosed = true;

constexpr bool kRangeOpened = false;

constexpr bool kWriteTwice = true;

constexpr bool kWithWrite = true;

constexpr bool kWithDelete = true;

/*##############################################################################
 * Global utility classes
 *############################################################################*/

template <template <class K, class V, class C> class IndexT, class KeyT, class PayloadT>
struct IndexInfo {
  using Key = KeyT;
  using Payload = PayloadT;
  using Index = IndexT<typename Key::Data, typename Payload::Data, typename Key::Comp>;
};

struct VarData {
  char data[kVarDataLength]{};
};

/*##############################################################################
 * Type definitions for templated tests
 *############################################################################*/

struct UInt8 {
  using Data = uint64_t;
  using Comp = std::less<uint64_t>;
};

struct Int8 {
  using Data = int64_t;
  using Comp = std::less<int64_t>;
};

struct UInt4 {
  using Data = uint32_t;
  using Comp = std::less<uint32_t>;
};

struct Int4 {
  using Data = int32_t;
  using Comp = std::less<int32_t>;
};

struct Ptr {
  using Data = uint64_t *;

  struct Comp {
    constexpr auto
    operator()(  //
        const uint64_t *a,
        const uint64_t *b) const  //
        -> bool
    {
      if (a == nullptr) return false;
      if (b == nullptr) return true;
      return *a < *b;
    }
  };
};

struct Var {
  using Data = char *;

  struct Comp {
    constexpr auto
    operator()(  //
        const char *a,
        const char *b) const noexcept  //
        -> bool
    {
      if (a == nullptr) return false;
      if (b == nullptr) return true;
      return std::strcmp(a, b) < 0;
    }
  };
};

struct Original {
  using Data = MyClass;
  using Comp = std::less<MyClass>;
};

/*##############################################################################
 * Global utility functions
 *############################################################################*/

/**
 * @tparam Compare a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false otherwise.
 */
template <class Compare, class T>
constexpr auto
IsEqual(  //
    const T &obj_1,
    const T &obj_2)  //
    -> bool
{
  return !Compare{}(obj_1, obj_2) && !Compare{}(obj_2, obj_1);
}

template <class T>
auto
GetLength(          //
    const T &data)  //
    -> size_t
{
  if constexpr (std::is_same_v<T, char *>) {
    return std::strlen(data) + 1;
  } else {
    return sizeof(T);
  }
}

inline void
CreateDummyString(  // NOLINT
    const size_t data_num,
    const size_t level,
    std::vector<char *> &data_vec,
    VarData var_arr[],
    size_t &i,
    VarData &base)
{
  if (level + 1 > kVarDataLength) return;

  for (size_t j = 0; j < 10 && i < data_num; ++j) {  // NOLINT
    base.data[level] = '0' + j;                      // NOLINT
    base.data[level + 1] = '\0';

    auto *data = reinterpret_cast<char *>(&(var_arr[i]));
    memcpy(data, &base, kVarDataLength);
    data_vec.emplace_back(data);
    if (++i >= data_num) return;

    base.data[level + 1] = '0';
    CreateDummyString(data_num, level + 2, data_vec, var_arr, i, base);
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

  if constexpr (std::is_same_v<T, char *>) {
    auto *var_arr = new VarData[data_num];
    VarData base{};
    std::memset(base.data, '0', kVarDataLength);

    size_t count = 0;
    CreateDummyString(data_num, 0, data_vec, var_arr, count, base);
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    auto *ptr_arr = new uint64_t[data_num];
    for (size_t i = 0; i < data_num; ++i) {
      ptr_arr[i] = i;
      data_vec.emplace_back(&(ptr_arr[i]));
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
    [[maybe_unused]] std::vector<T> &data_vec)
{
  if constexpr (std::is_same_v<T, char *>) {
    delete[] reinterpret_cast<VarData *>(data_vec.front());
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    delete[] data_vec.front();
  }
}

template <class T>
constexpr auto
IsVarLen()  //
    -> bool
{
  return std::is_same_v<T, char *>;
}

/*##############################################################################
 * Template functions for disabling tests of each operation
 *############################################################################*/

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_READ_TEST
constexpr bool kDisableReadTest = true;
#else
constexpr bool kDisableReadTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_SCAN_TEST
constexpr bool kDisableScanTest = true;
#else
constexpr bool kDisableScanTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_WRITE_TEST
constexpr bool kDisableWriteTest = true;
#else
constexpr bool kDisableWriteTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_INSERT_TEST
constexpr bool kDisableInsertTest = true;
#else
constexpr bool kDisableInsertTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_UPDATE_TEST
constexpr bool kDisableUpdateTest = true;
#else
constexpr bool kDisableUpdateTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_DELETE_TEST
constexpr bool kDisableDeleteTest = true;
#else
constexpr bool kDisableDeleteTest = false;
#endif

#ifdef DBGROUP_INDEX_FIXTURES_DISABLE_BULKLOAD_TEST
constexpr bool kDisableBulkloadTest = true;
#else
constexpr bool kDisableBulkloadTest = false;
#endif

}  // namespace dbgroup::index::test

#endif  // DBGROUP_INDEX_FIXTURES_COMMON_HPP
