#!/bin/bash

OPTS="$@"

export CC=gcc
export CXX=g++
export FC=gfortran
export MPICC=mpicc
export MPICXX=mpicxx
export MPIFC=mpifort
export CFLAGS="-O3 -g"
export CXXFLAGS="-O3 -g"
export FCFLAGS="-O3 -g"
export PYTHON=/usr/bin/python3

./configure ${OPTS} \
--with-hdf5=/usr/bin/h5cc

