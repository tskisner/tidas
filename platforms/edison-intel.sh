#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export MPICC=cc
export MPICXX=CC
export CFLAGS="-O3 -g -fPIC -xavx -pthread"
export CXXFLAGS="-O3 -g -fPIC -xavx -pthread"

./configure ${OPTS}

