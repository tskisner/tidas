#!/bin/sh

LIBTIDAS_PATH="${1}"
LIBTIDAS_MPI_PATH="${2}"

sed \
-e "s#@LIBTIDAS_PATH@#${LIBTIDAS_PATH}#g" \
-e "s#@LIBTIDAS_MPI_PATH@#${LIBTIDAS_MPI_PATH}#g" \
ctidas.py.in > ctidas.py

sed \
-e "s#@LIBTIDAS_PATH@#${LIBTIDAS_PATH}#g" \
-e "s#@LIBTIDAS_MPI_PATH@#${LIBTIDAS_MPI_PATH}#g" \
ctidas_mpi.py.in > ctidas_mpi.py
