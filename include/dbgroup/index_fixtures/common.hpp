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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>

// external libraries
#include "dbgroup/index/utility.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index
{

template <>
constexpr auto
IsVarLenData<char *>() noexcept  //
    -> bool
{
  return true;
}

namespace test
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

#ifdef DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST
constexpr bool kDisableScanVerifyTest = true;
#else
constexpr bool kDisableScanVerifyTest = false;
#endif

#ifdef DBGROUP_TEST_DISABLE_WRITE_TEST
constexpr bool kDisableWriteTest = true;
#else
constexpr bool kDisableWriteTest = false;
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

/*##############################################################################
 * Global utility classes
 *############################################################################*/

template <template <class K, class V, class C> class IndexT, class KeyT, class PayloadT>
struct IndexInfo {
  using Key = KeyT;
  using Payload = PayloadT;
  using Index = IndexT<typename Key::Data, typename Payload::Data, typename Key::Comp>;
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
      const uint64_t val) noexcept
      : data{val}, control_bits{0}
  {
  }

  ~MyClass() = default;

  constexpr MyClass(const MyClass &) noexcept = default;
  constexpr MyClass(MyClass &&) noexcept = default;

  constexpr auto operator=(const MyClass &) noexcept -> MyClass & = default;
  constexpr auto operator=(MyClass &&) noexcept -> MyClass & = default;

  constexpr auto
  operator=(                          //
      const uint64_t value) noexcept  //
      -> MyClass &
  {
    data = value;
    return *this;
  }

  constexpr auto
  operator<(                               //
      const MyClass &comp) const noexcept  //
      -> bool
  {
    return data < comp.data;
  }

  constexpr auto
  operator==(                              //
      const MyClass &comp) const noexcept  //
      -> bool
  {
    return data == comp.data;
  }

  uint64_t data : 61;
  uint64_t control_bits : 3;  // NOLINT
};

struct VarData {
  char data[kVarDataLength]{};
};

template <class Key, class Payload>
struct DummyIter {
  constexpr explicit
  operator bool() noexcept
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
  operator++()
  {
  }

  constexpr void
  PrepareVerifier()
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
        const uint64_t *b) const noexcept  //
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

struct RCComp {
  constexpr auto
  operator()(  //
      const ReturnCode &lhs,
      const ReturnCode &rhs) const noexcept  //
      -> bool
  {
    return lhs < rhs;
  }
};

/*##############################################################################
 * Global utility functions
 *############################################################################*/

template <class T>
auto
GetLength(                   //
    const T &data) noexcept  //
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

    auto *data = std::bit_cast<char *>(&(var_arr[i]));
    std::memcpy(data, &base, kVarDataLength);
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
    delete[] std::bit_cast<VarData *>(data_vec.front());
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    delete[] data_vec.front();
  }
}

/*##############################################################################
 * Utility functions for assertion
 *############################################################################*/

/// a mutex for outputting messages
std::mutex _io_mtx{};  // NOLINT

void
AssertTrue(  //
    const bool expect_true,
    const std::string_view &tag)
{
  if (!expect_true) {
    const std::lock_guard lock{_io_mtx};
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
    const std::lock_guard lock{_io_mtx};
    std::cout << "  [" << tag << "] The actual value was not false.\n";
#ifdef NDEBUG
    throw std::runtime_error{""};
#else
    FAIL();
#endif
  }
}

template <class Comp, class T>
void
AssertEQ(  //
    const T &actual,
    const T &expected,
    const std::string_view &tag)
{
  if (!IsEqual<Comp>(actual, expected)) {
    const std::lock_guard lock{_io_mtx};
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

template <class Comp, class T>
void
AssertNE(  //
    const T &actual,
    const T &expected,
    const std::string_view &tag)
{
  if (IsEqual<Comp>(actual, expected)) {
    const std::lock_guard lock{_io_mtx};
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

template <class Comp, class T>
void
AssertLT(  //
    const T &lhs,
    const T &rhs,
    const std::string_view &tag)
{
  if (!Comp{}(lhs, rhs)) {
    const std::lock_guard lock{_io_mtx};
    std::cout << "  [" << tag << "] The left-hand side value was larger the right-hand side one.\n"
              << "    lhs: " << lhs << "\n"
              << "    rhs: " << rhs << "\n";
#ifdef NDEBUG
    throw std::runtime_error{""};
#else
    FAIL();
#endif
  }
}

}  // namespace test
}  // namespace dbgroup::index

auto
operator<<(  //
    std::ostream &os,
    const ::dbgroup::index::ReturnCode &obj)  //
    -> std::ostream &
{
  if (obj == ::dbgroup::index::ReturnCode::kSuccess) {
    os << "kSuccess";
  } else if (obj == ::dbgroup::index::ReturnCode::kKeyNotExist) {
    os << "kKeyNotExist";
  } else {
    os << "kKeyExist";
  }
  return os;
}

auto
operator<<(  //
    std::ostream &os,
    const ::dbgroup::index::test::MyClass &obj)  //
    -> std::ostream &
{
  os << obj.data;
  return os;
}

#endif  // DBGROUP_INDEX_FIXTURES_COMMON_HPP
