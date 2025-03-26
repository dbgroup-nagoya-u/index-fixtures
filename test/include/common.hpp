/*
 * Copyright 2024 Database Group, Nagoya University
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

// external libraries
#include "dbgroup/index_fixtures/common.hpp"

namespace dbgroup::index::test
{

template <class Key, class Payload, class Comp>
struct Index {
  // dummy struct for index implementations
};

struct ImplState {
  // dummy struct for specifying implementation states
};

template <>
constexpr auto
HasReadOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasScanOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasWriteOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasInsertOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasUpdateOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasDeleteOperation<ImplState>()  //
    -> bool
{
  return false;
}

template <>
constexpr auto
HasBulkloadOperation<ImplState>()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup::index::test
