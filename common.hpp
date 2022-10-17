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

#ifndef INDEX_FIXTURES_COMMON_HPP
#define INDEX_FIXTURES_COMMON_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <type_traits>
#include <vector>

/*######################################################################################
 * Classes for testing
 *####################################################################################*/

/**
 * @brief An example class to represent CAS-updatable data.
 *
 */
class MyClass
{
 public:
  constexpr MyClass() : data_{}, control_bits_{0} {}
  constexpr explicit MyClass(const uint64_t val) : data_{val}, control_bits_{0} {}

  ~MyClass() = default;

  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;
  constexpr auto operator=(const MyClass &) -> MyClass & = default;
  constexpr auto operator=(MyClass &&) -> MyClass & = default;

  constexpr auto
  operator=(const uint64_t value)  //
      -> MyClass &
  {
    data_ = value;
    return *this;
  }

  // enable std::less to compare this class
  constexpr auto
  operator<(const MyClass &comp) const  //
      -> bool
  {
    return data_ < comp.data_;
  }

 private:
  uint64_t data_ : 61;
  uint64_t control_bits_ : 3;  // NOLINT
};

namespace dbgroup::index::test
{
/*######################################################################################
 * Constants for testing
 *####################################################################################*/

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

#ifdef INDEX_FIXTURE_EXEC_NUM_PER_THREAD
constexpr size_t kExecNum = INDEX_FIXTURE_EXEC_NUM_PER_THREAD;
#else
constexpr size_t kExecNum = 1E6;
#endif

#ifdef INDEX_FIXTURE_RANDOM_SEED
constexpr size_t kRandomSeed = INDEX_FIXTURE_RANDOM_SEED;
#else
constexpr size_t kRandomSeed = 0;
#endif

constexpr size_t kVarDataLength = 18;

constexpr bool kExpectSuccess = true;

constexpr bool kExpectFailed = false;

constexpr bool kRangeClosed = true;

constexpr bool kRangeOpened = false;

constexpr bool kWriteTwice = true;

constexpr bool kWithWrite = true;

constexpr bool kWithDelete = true;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

template <template <class K, class V, class C> class IndexType,
          class KeyType,
          class PayloadType,
          class ImplementationStatus = void>
struct IndexInfo {
  using Key = KeyType;
  using Payload = PayloadType;
  using Index_t = IndexType<typename Key::Data, typename Payload::Data, typename Key::Comp>;
  using ImplStatus = ImplementationStatus;
};

struct VarData {
  char data[kVarDataLength]{};
};

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
GetLength(const T &data)  //
    -> size_t
{
  if constexpr (std::is_same_v<T, char *>) {
    return strlen(data) + 1;
  } else {
    return sizeof(T);
  }
}

inline void
CreateDummyString(  //
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
PrepareTestData(const size_t data_num)  //
    -> std::vector<T>
{
  std::vector<T> data_vec{};
  data_vec.reserve(data_num);

  if constexpr (std::is_same_v<T, char *>) {
    auto *var_arr = new VarData[data_num];
    VarData base{};
    memset(base.data, '0', kVarDataLength);

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
ReleaseTestData([[maybe_unused]] std::vector<T> &data_vec)
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
  if constexpr (std::is_same_v<T, char *>) {
    return true;
  } else {
    return false;
  }
}

template <class ImplStat>
constexpr auto
HasScanOperation()  //
    -> bool
{
  return true;
}

template <class ImplStat>
constexpr auto
HasWriteOperation()  //
    -> bool
{
  return true;
}

template <class ImplStat>
constexpr auto
HasInsertOperation()  //
    -> bool
{
  return true;
}

template <class ImplStat>
constexpr auto
HasUpdateOperation()  //
    -> bool
{
  return true;
}

template <class ImplStat>
constexpr auto
HasDeleteOperation()  //
    -> bool
{
  return true;
}

template <class ImplStat>
constexpr auto
HasBulkloadOperation()  //
    -> bool
{
  return true;
}

/*######################################################################################
 * Type definitions for templated tests
 *####################################################################################*/

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
    operator()(const uint64_t *a, const uint64_t *b) const  //
        -> bool
    {
      return *a < *b;
    }
  };
};

struct Var {
  using Data = char *;

  struct Comp {
    constexpr auto
    operator()(const char *a, const char *b) const noexcept  //
        -> bool
    {
      if (a == nullptr) return false;
      if (b == nullptr) return true;
      return strcmp(a, b) < 0;
    }
  };
};

struct Original {
  using Data = MyClass;
  using Comp = std::less<MyClass>;
};

}  // namespace dbgroup::index::test

#endif  // INDEX_FIXTURES_COMMON_HPP
