/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_mpi_internal.hpp>

#include <tidas_mpi_test.hpp>


int main ( int argc, char *argv[] ) {
	tidas::mpi_init ( argc, argv );
    MPI_Comm comm = MPI_COMM_WORLD;
    int rank;
    int ret = MPI_Comm_rank ( comm, &rank );
    std::string dir = tidas::test::output_dir();
    if ( rank == 0 ) {
        tidas::fs_mkdir ( dir.c_str() );
    }
    ret = tidas::test::mpi_runner ( argc, argv );
    tidas::mpi_finalize();
    return ret;
}
