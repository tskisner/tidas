#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export FC=ifort
export MPICC=cc
export MPICXX=CC
export MPIFC=ftn
export CFLAGS="-no-gcc -O3 -g -fPIC -xcore-avx2 -pthread"
export CXXFLAGS="-no-gcc -O3 -g -fPIC -xcore-avx2 -pthread"
export FCFLAGS="-no-gcc -O3 -g -fPIC -xcore-avx2 -pthread"
export OPENMP_CFLAGS="-qopenmp"
export OPENMP_CXXFLAGS="-qopenmp"

./configure ${OPTS}


