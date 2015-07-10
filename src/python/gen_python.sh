#!/bin/sh

LIBTIDAS_PATH="${1}"

sed \
-e "s#@LIBTIDAS_PATH@#${LIBTIDAS_PATH}#g" \
ctidas.py.in > ctidas.py

