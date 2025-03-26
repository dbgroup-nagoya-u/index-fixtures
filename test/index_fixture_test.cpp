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

// the corresponding header
#include "dbgroup/index_fixtures/index_fixture.hpp"

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::index::test
{
/*##############################################################################
 * Preparation for typed testing
 *############################################################################*/

using TestTargets = ::testing::Types<                //
    IndexInfo<Index, UInt8, Int8, ImplState>,        // fixed-length keys
    IndexInfo<Index, UInt4, Int8, ImplState>,        // small keys
    IndexInfo<Index, UInt8, UInt4, ImplState>,       // small payloads
    IndexInfo<Index, UInt4, UInt4, ImplState>,       // small keys/payloads
    IndexInfo<Index, Var, Var, ImplState>,           // variable-length keys/payloads
    IndexInfo<Index, Ptr, Ptr, ImplState>,           // pointer keys/payloads
    IndexInfo<Index, Original, Original, ImplState>  // user-defined keys/payloads
    >;
TYPED_TEST_SUITE(IndexFixture, TestTargets);

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

#include "dbgroup/index_fixtures/index_fixture_test_definitions.hpp"

}  // namespace dbgroup::index::test
