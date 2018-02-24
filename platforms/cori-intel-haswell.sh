#!/bin/bash

OPTS="$@"

cmake \
-DCMAKE_C_COMPILER="cc" \
-DCMAKE_CXX_COMPILER="CC" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC -xcore-avx2 -pthread" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC -xcore-avx2 -pthread" \
${OPTS} \
..
