#!/bin/bash

OPTS="$@"

cmake \
-DCMAKE_C_COMPILER="gcc" \
-DCMAKE_CXX_COMPILER="g++" \
-DCMAKE_C_FLAGS="-O3 -g -fPIC" \
-DCMAKE_CXX_FLAGS="-O3 -g -fPIC" \
-DPYTHON_EXECUTABLE:FILEPATH=/usr/bin/python3 \
${OPTS} \
..
