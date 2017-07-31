#!/bin/bash

rm -rf xml
doxygen tidas.dox

pushd ./sphinx >/dev/null
make clean; make html
popd >/dev/null

