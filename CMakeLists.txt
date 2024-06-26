cmake_minimum_required(VERSION "3.11")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  index_fixtures
  VERSION "0.1.0"
  DESCRIPTION "An open-source index fixture classes."
  LANGUAGES "CXX"
)

if(NOT TARGET dbgroup::${PROJECT_NAME})
  #----------------------------------------------------------------------------#
  # Build options
  #----------------------------------------------------------------------------#

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

  #----------------------------------------------------------------------------#
  # Configurations
  #----------------------------------------------------------------------------#

  # use multi-threads
  find_package(Threads)

  # use GTest
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY "https://github.com/google/googletest.git"
    GIT_TAG "f8d7d77c06936315286eb55f8de22cd23c188571" # v1.14.0
  )
  FetchContent_MakeAvailable(googletest)

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    DBGROUP_TEST_THREAD_NUM=${DBGROUP_TEST_THREAD_NUM}
    DBGROUP_TEST_RANDOM_SEED=${DBGROUP_TEST_RANDOM_SEED}
    DBGROUP_TEST_EXEC_NUM=${DBGROUP_TEST_EXEC_NUM}
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    Threads::Threads
    GTest::gtest_main
  )

  #----------------------------------------------------------------------------#
  # Build unit tests
  #----------------------------------------------------------------------------#

  option(DBGROUP_INDEX_FIXTURES_BUILD_TESTS "Build unit tests for BzTree" OFF)
  if(${DBGROUP_INDEX_FIXTURES_BUILD_TESTS})
    enable_testing()
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
  endif()
endif()
