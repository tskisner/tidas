#!/bin/bash

OPTS="$@"

cmake \
-DCMAKE_C_COMPILER="cc" \
-DCMAKE_CXX_COMPILER="CC" \
-DMPI_C_COMPILER="cc" \
-DMPI_CXX_COMPILER="CC" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC -xavx -axmic-avx512 -pthread" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC -xavx -axmic-avx512 -pthread" \
${OPTS} \
..
