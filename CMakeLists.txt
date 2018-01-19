# TIDAS

cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(tidas VERSION 0.3.0)

# Auxiliary files
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# Use GNUInstallDirs to install libraries into correct locations on all
# platforms.
include(GNUInstallDirs)

# External packages

find_package(HDF5 REQUIRED)
find_package(MPI)

# Internal products

enable_testing()
add_subdirectory(src)
