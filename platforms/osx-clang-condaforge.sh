#!/bin/bash

OPTS="$@"

cmake \
-DCMAKE_C_COMPILER="clang" \
-DCMAKE_CXX_COMPILER="clang++" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC" \
-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
${OPTS} \
..
