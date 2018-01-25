/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_mpi_internal.hpp>

#include <tidas_mpi_test.hpp>


int main ( int argc, char *argv[] ) {
	tidas::mpi_init ( argc, argv );
    int ret = tidas::test::mpi_runner ( argc, argv );
    tidas::mpi_finalize();
    return ret;
}
