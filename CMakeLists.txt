# TIDAS

# This minimum version is mostly set in order to get a newer version
# of the FindMPI check.
cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

foreach(policy
    CMP0048
    CMP0074
    CMP0077
  )
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

project(tidas VERSION 0.3.4 LANGUAGES C CXX)

# Force C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
