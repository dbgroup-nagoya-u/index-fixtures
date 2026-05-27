cmake_minimum_required(VERSION "3.11")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  index_fixtures
  VERSION "0.3.0"
  DESCRIPTION "An open-source index fixture classes."
  LANGUAGES "CXX"
)

if(NOT TARGET dbgroup::${PROJECT_NAME})
  # enable features
  include(FetchContent)

  #----------------------------------------------------------------------------#
  # Build options
  #----------------------------------------------------------------------------#

  option(
    DBGROUP_TEST_DISABLE_RECORD_MERGING
    "Disable delta-update related tests."
    ON
  )

  option(
    DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST
    "Disable scan-verification related tests."
    ON
  )

  set(
    DBGROUP_TEST_THREAD_NUM
    "2" CACHE STRING
    "The maximum number of threads unit testing."
  )

  set(
    DBGROUP_TEST_RANDOM_SEED
    "0" CACHE STRING
    "A fixed seed value to reproduce unit tests."
  )

  set(
    DBGROUP_TEST_EXEC_NUM
    "1E5" CACHE STRING
    "The number of executions per a thread."
  )

  set(
    DBGROUP_TEST_MAX_VARLEN_DATA_SIZE
    "32" CACHE STRING
    "The expected maximum size of a variable-length data."
  )

  option(
    DBGROUP_TEST_OVERRIDE_MIMALLOC
    "Override entire memory allocation with mimalloc."
    OFF
  )

  set(
    DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM
    "1" CACHE STRING
    "The number of servers in a cluster (for distributed indexes)."
  )

  set(
    DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID
    "0" CACHE STRING
    "The ID of this server in a cluster (for distributed indexes)."
  )

  #----------------------------------------------------------------------------#
  # Configurations
  #----------------------------------------------------------------------------#

  # use multi-threads
  find_package(Threads)

  # use GTest
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY "https://github.com/google/googletest"
    GIT_TAG "52eb8108c5bdec04579160ae17225d66034bd723" # v1.17.0
  )
  FetchContent_MakeAvailable(googletest)

  # use our utility library
  FetchContent_Declare(
    cpp-utility
    GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/cpp-utility"
    GIT_TAG "8da07347db75c1498a849a8817534590ceb67738"
  )
  FetchContent_MakeAvailable(cpp-utility)

  if(${DBGROUP_TEST_OVERRIDE_MIMALLOC})
    message(STATUS "[index-fixture] override malloc/free using mimalloc.")
    find_package(mimalloc QUIET)
    if(NOT mimalloc_FOUND)
      message(STATUS "[index-fixture] mimalloc not found, so fetching from GitHub.")
      set(MI_BUILD_SHARED OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
      set(MI_BUILD_OBJECT OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
      set(MI_BUILD_TESTS OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
      FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY "https://github.com/microsoft/mimalloc"
        GIT_TAG "30b2d9d89099bee08e9f67a1ffb3e12e7ba45227" # v3.3.2
      )
      FetchContent_MakeAvailable(mimalloc)
    endif()
  endif()

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_20"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    $<$<BOOL:${DBGROUP_TEST_DISABLE_RECORD_MERGING}>:DBGROUP_TEST_DISABLE_RECORD_MERGING>
    $<$<BOOL:${DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST}>:DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST>
    DBGROUP_TEST_THREAD_NUM=${DBGROUP_TEST_THREAD_NUM}
    DBGROUP_TEST_RANDOM_SEED=${DBGROUP_TEST_RANDOM_SEED}
    DBGROUP_TEST_EXEC_NUM=${DBGROUP_TEST_EXEC_NUM}
    DBGROUP_TEST_MAX_VARLEN_DATA_SIZE=${DBGROUP_TEST_MAX_VARLEN_DATA_SIZE}
    DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM=${DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM}
    DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID=${DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID}
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    Threads::Threads
    dbgroup::cpp_utility
    GTest::gtest_main
    $<$<BOOL:${DBGROUP_TEST_OVERRIDE_MIMALLOC}>:mimalloc-static>
  )

  #----------------------------------------------------------------------------#
  # Build unit tests
  #----------------------------------------------------------------------------#

  option(DBGROUP_INDEX_FIXTURES_BUILD_TESTS "Build unit tests" OFF)
  if(${DBGROUP_INDEX_FIXTURES_BUILD_TESTS})
    enable_testing()
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
  endif()
endif()
