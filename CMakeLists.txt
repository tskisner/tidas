# TIDAS

# This minimum version is mostly set in order to get a newer version
# of the FindMPI check.
cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(tidas VERSION 0.3.0 LANGUAGES C CXX)

# Auxiliary files
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# Use GNUInstallDirs to install libraries into correct locations on all
# platforms.
include(GNUInstallDirs)

# Build defaults
include(BuildType)

# External packages

find_package(HDF5 REQUIRED)
find_package(MPI)

find_package(PythonInterp)
find_package(MPI4PY)

# Internal products

enable_testing()
add_subdirectory(src)
