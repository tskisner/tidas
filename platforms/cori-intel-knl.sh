#!/bin/bash

OPTS="$@"

cmake \
-DCMAKE_C_COMPILER="cc" \
-DCMAKE_CXX_COMPILER="CC" \
-DCMAKE_C_FLAGS="-O3 -g -no-gcc -fPIC -xmic-avx512 -pthread" \
-DCMAKE_CXX_FLAGS="-O3 -g -no-gcc -fPIC -xmic-avx512 -pthread" \
${OPTS} \
..
