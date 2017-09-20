#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export FC=ifort
export MPICC=cc
export MPICXX=CC
export MPIFC=ftn
export CFLAGS="-O3 -g -fPIC -xcore-avx2 -pthread"
export CXXFLAGS="-O3 -g -fPIC -xcore-avx2 -pthread"
export FCFLAGS="-O3 -g -fPIC -xcore-avx2 -pthread"

./configure ${OPTS}


