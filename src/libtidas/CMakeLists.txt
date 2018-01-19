
# Add internal googletest framework.  Must be added before the library
# in order to define some variables.

set(gtest_force_shared_crt TRUE)
add_subdirectory(gtest)
include_directories("${gtest_SOURCE_DIR}/include")

# Add the library target

add_library(tidas
    src/sqlite3.c
    src/tidas_backend_getdata.cpp
    src/tidas_backend_hdf5.cpp
    src/tidas_block.cpp
    src/tidas_copy.cpp
    src/tidas.cpp
    src/tidas_dict.cpp
    src/tidas_dict_getdata.cpp
    src/tidas_dict_hdf5.cpp
    src/tidas_group.cpp
    src/tidas_group_getdata.cpp
    src/tidas_group_hdf5.cpp
    src/tidas_indexdb_mem.cpp
    src/tidas_indexdb_sql.cpp
    src/tidas_intervals.cpp
    src/tidas_intervals_getdata.cpp
    src/tidas_intervals_hdf5.cpp
    src/tidas_schema.cpp
    src/tidas_schema_getdata.cpp
    src/tidas_schema_hdf5.cpp
    src/tidas_test_block.cpp
    src/tidas_test_cereal.cpp
    src/tidas_test_dict.cpp
    src/tidas_test_group.cpp
    src/tidas_test_indexdb.cpp
    src/tidas_test_intervals.cpp
    src/tidas_test_runner.cpp
    src/tidas_test_schema.cpp
    src/tidas_test_utils.cpp
    src/tidas_test_volume.cpp
    src/tidas_utils.cpp
    src/tidas_volume.cpp
)

if (HDF5_FOUND)
    target_compile_definitions(tidas PRIVATE HAVE_HDF5=1)
    target_include_directories(tidas PUBLIC ${HDF5_INCLUDE_DIRS})
endif (HDF5_FOUND)

target_include_directories(tidas PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_link_libraries(tidas gtest gtest_main ${HDF5_C_LIBRARIES})
