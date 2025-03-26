#------------------------------------------------------------------------------#
# Build Unit Tests
#------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(DBGROUP_ADD_TEST DBGROUP_TEST_TARGET)
  add_executable(${DBGROUP_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${DBGROUP_TEST_TARGET}.cpp"
  )
  target_compile_options(${DBGROUP_TEST_TARGET} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_include_directories(${DBGROUP_TEST_TARGET} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${DBGROUP_TEST_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
  )

  add_test(NAME ${DBGROUP_TEST_TARGET} COMMAND $<TARGET_FILE:${DBGROUP_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
DBGROUP_ADD_TEST("sample_test")
DBGROUP_ADD_TEST("sample_multi_thread_test")
