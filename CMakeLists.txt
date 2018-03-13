# TIDAS

cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(tidas VERSION 0.3.0 LANGUAGES C CXX)

# Auxiliary files
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# Use GNUInstallDirs to install libraries into correct locations on all
# platforms.
include(GNUInstallDirs)

# External packages

find_package(HDF5 REQUIRED)
find_package(MPI)

find_package(PythonInterp)
find_package(MPI4PY)

# Internal products

enable_testing()
add_subdirectory(src)

add_test(NAME serial_tests COMMAND tidas_test)
