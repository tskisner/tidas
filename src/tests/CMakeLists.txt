
# Name of the serial internal static library
set(TIDAS_STATIC tidas_)

# Add internal googletest framework.  Must be added before the library
# in order to define some variables.

set(gtest_force_shared_crt ON CACHE BOOL "Always use shared." FORCE)

add_subdirectory(gtest)
include_directories("${gtest_SOURCE_DIR}/include")


add_executable(tidas_test
    tidas_test.cpp
    tidas_test_block.cpp
    tidas_test_cereal.cpp
    tidas_test_dict.cpp
    tidas_test_group.cpp
    tidas_test_indexdb.cpp
    tidas_test_intervals.cpp
    tidas_test_runner.cpp
    tidas_test_schema.cpp
    tidas_test_utils.cpp
    tidas_test_volume.cpp
)

target_include_directories(tidas_test PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

target_link_libraries(tidas_test ${TIDAS_STATIC} gtest gtest_main)

if (HDF5_FOUND)
    target_compile_definitions(tidas_test PRIVATE HAVE_HDF5=1)
endif (HDF5_FOUND)

install(TARGETS tidas_test DESTINATION ${CMAKE_INSTALL_BINDIR})

add_test(NAME serial_tests COMMAND tidas_test)
