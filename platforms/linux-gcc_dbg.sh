#!/bin/bash

OPTS="$@"

export CC=gcc
export CXX=g++
export FC=gfortran
export MPICC=mpicc
export MPICXX=mpicxx
export MPIFC=mpifort
export CFLAGS="-O0 -g"
export CXXFLAGS="-O0 -g"
export FCFLAGS="-O0 -g"
export PYTHON=/usr/bin/python3

./configure ${OPTS} \
--with-hdf5=/usr/bin/h5cc

