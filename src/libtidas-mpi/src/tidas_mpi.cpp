/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#include <tidas_mpi_internal.hpp>


using namespace std;
using namespace tidas;


// Initialize MPI if it has not already been done

void tidas::mpi_init(int argc, char * argv[]) {
    int ret;
    int initialized;

    ret = MPI_Initialized(&initialized);

    if (!initialized) {
        ret = MPI_Init(&argc, &argv);
    }

    return;
}

void tidas::mpi_finalize() {
    int ret;
    ret = MPI_Finalize();
    return;
}

void tidas::mpi_dist_uniform(MPI_Comm comm, size_t n, size_t * offset,
                             size_t * nlocal) {
    int irank;
    int inproc;
    int ret = MPI_Comm_rank(comm, &irank);
    ret = MPI_Comm_size(comm, &inproc);

    size_t rank = (size_t)irank;
    size_t nproc = (size_t)inproc;

    size_t off;

    size_t myn = (size_t)(n / nproc);
    size_t leftover = n % nproc;
    if (rank < leftover) {
        myn += 1;
        off = rank * myn;
    } else {
        off = ((myn + 1) * leftover) + (myn * (rank - leftover));
    }

    (*offset) = off;
    (*nlocal) = myn;

    return;
}
