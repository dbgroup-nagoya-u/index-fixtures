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
constexpr size_t kRandomSeed = 8;
#endif

constexpr size_t kVarDataLength = 12;

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

struct VarData {
  char data[kVarDataLength]{};
};

template <class Key, class Payload>
class Record
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Record() = default;

  constexpr Record(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key))
      : key_{key}, payload_{payload}, key_length_{key_length}
  {
  }

  constexpr Record(const Record &) = default;
  constexpr Record(Record &&) noexcept = default;

  constexpr auto operator=(const Record &) -> Record & = default;
  constexpr auto operator=(Record &&) noexcept -> Record & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Record() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> const Key &
  {
    return key_;
  }

  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> const Payload &
  {
    return payload_;
  }

  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return key_length_;
  }

  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return sizeof(Payload);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Key key_{};

  Payload payload_{};

  size_t key_length_{};
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

  // reeserve memory if needed
  VarData *var_arr{nullptr};
  uint64_t *ptr_arr{nullptr};
  if constexpr (std::is_same_v<T, char *>) {
    var_arr = new VarData[data_num];
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    ptr_arr = new uint64_t[data_num];
  }

  for (size_t i = 0; i < data_num; ++i) {
    if constexpr (std::is_same_v<T, char *>) {
      // variable-length data
      auto *data = reinterpret_cast<char *>(&(var_arr[i]));
      snprintf(data, kVarDataLength, "%011lu", i);  // NOLINT
      data_vec.emplace_back(data);
    } else if constexpr (std::is_same_v<T, uint64_t *>) {
      // pointer data
      ptr_arr[i] = i;
      data_vec.emplace_back(&(ptr_arr[i]));
    } else {
      // static-length data
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
