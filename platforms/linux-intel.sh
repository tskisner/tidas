#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export FC=ifort
export MPICC=mpiicc
export MPICXX=mpiicpc
export MPIFC=mpiifort
export CFLAGS="-O3 -g -no-gcc"
export CXXFLAGS="-O3 -g -no-gcc"
export FCFLAGS="-O3 -g -no-gcc"

./configure ${OPTS} \
--with-hdf5=/usr/bin/h5cc

