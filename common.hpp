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

#ifdef INDEX_FIXTURE_THREAD_NUM
static constexpr size_t kThreadNum = INDEX_FIXTURE_THREAD_NUM;
#else
static constexpr size_t kThreadNum = 8;
#endif

constexpr size_t kVarDataLength = 12;

constexpr size_t kRandomSeed = 10;

constexpr bool kExpectSuccess = true;

constexpr bool kExpectFailed = false;

constexpr bool kRangeClosed = true;

constexpr bool kRangeOpened = false;

constexpr bool kWriteTwice = true;

constexpr bool kWithWrite = true;

constexpr bool kWithDelete = true;

constexpr bool kShuffled = true;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

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
constexpr auto
GetDataLength()  //
    -> size_t
{
  if constexpr (std::is_same_v<T, char *>) {
    return kVarDataLength;
  } else {
    return sizeof(T);
  }
}

template <class T>
auto
PrepareTestData(const size_t data_num)  //
    -> std::vector<T>
{
  std::vector<T> data_vec{};
  data_vec.reserve(data_num);

  for (size_t i = 0; i < data_num; ++i) {
    if constexpr (std::is_same_v<T, char *>) {
      // variable-length data
      auto *data = reinterpret_cast<char *>(::operator new(kVarDataLength));
      snprintf(data, kVarDataLength, "%011lu", i);  // NOLINT
      data_vec.emplace_back(reinterpret_cast<T>(data));
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      // pointer data
      data_vec.emplace_back(new uint64_t{i});
    } else {
      // static-length data
      data_vec.emplace_back(i);
    }
  }

  return data_vec;
}

template <class T>
void
ReleaseTestData(std::vector<T> &data_vec)
{
  for (auto &&elem : data_vec) {
    if constexpr (std::is_same_v<T, char *>) {
      // variable-length data
      ::operator delete(elem);
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      // pointer data
      delete elem;
    }
  }
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
