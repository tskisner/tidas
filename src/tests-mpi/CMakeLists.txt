
add_executable(tidas_test_mpi
    tidas_test_mpi.cpp
    tidas_mpi_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_block.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_dict.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_group.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_intervals.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_schema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_volume.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/tidas_test_runner.cpp
)

target_include_directories(tidas_test_mpi PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas-mpi/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/../tests"
)

target_include_directories(tidas_test_mpi PRIVATE ${MPI_CXX_INCLUDE_PATH})
target_compile_options(tidas_test_mpi PRIVATE "${MPI_CXX_COMPILE_FLAGS}")
set_target_properties(tidas_test_mpi
    PROPERTIES LINK_FLAGS "${MPI_CXX_LINK_FLAGS}"
)

target_link_libraries(tidas_test_mpi
    tidas-mpi gtest gtest_main "${MPI_CXX_LIBRARIES}"
)

if (HDF5_FOUND)
    target_compile_definitions(tidas_test_mpi PRIVATE HAVE_HDF5=1)
endif (HDF5_FOUND)

install(TARGETS tidas_test_mpi DESTINATION ${CMAKE_INSTALL_BINDIR})
