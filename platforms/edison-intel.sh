#!/bin/bash

OPTS="$@"

LDFLAGS='-dynamic' \
cmake \
-DCMAKE_C_COMPILER="cc" \
-DCMAKE_CXX_COMPILER="CC" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC -xavx -dynamic -pthread -DSQLITE_DISABLE_INTRINSIC" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC -xavx -dynamic -pthread -DSQLITE_DISABLE_INTRINSIC" \
-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
${OPTS} \
..
