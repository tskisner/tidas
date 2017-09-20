#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export FC=ifort
export MPICC=cc
export MPICXX=CC
export MPIFC=ftn
export CFLAGS="-O3 -g -fPIC -xmic-avx512 -pthread"
export CXXFLAGS="-O3 -g -fPIC -xmic-avx512 -pthread"
export FCFLAGS="-O3 -g -fPIC -xmic-avx512 -pthread"
export OPENMP_CFLAGS="-qopenmp"
export OPENMP_CXXFLAGS="-qopenmp"

./configure ${OPTS} \
    --build x86_64-pc-linux-gnu --host x86_64-unknown-linux-gnu

