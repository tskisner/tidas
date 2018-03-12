#!/bin/bash

OPTS="$@"

LDFLAGS='-dynamic' \
cmake \
-DCMAKE_C_COMPILER="cc" \
-DCMAKE_CXX_COMPILER="CC" \
-DMPI_C_COMPILER="cc" \
-DMPI_CXX_COMPILER="CC" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC -xavx -axmic-avx512 -dynamic -pthread -DSQLITE_DISABLE_INTRINSIC" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC -xavx -axmic-avx512 -dynamic -pthread -DSQLITE_DISABLE_INTRINSIC" \
-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
${OPTS} \
..
