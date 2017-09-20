#!/bin/bash

OPTS="$@"

export CC=gcc
export CXX=g++
export MPICC=cc
export MPICXX=CC
export CFLAGS="-O3 -g -fPIC"
export CXXFLAGS="-O3 -g -fPIC"

./configure ${OPTS}

