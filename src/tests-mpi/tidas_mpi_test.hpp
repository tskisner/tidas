/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_MPI_TEST_HPP
#define TIDAS_MPI_TEST_HPP

#include <tidas_test.hpp>


namespace tidas { namespace test {

    int mpi_runner ( int argc, char *argv[] );

    void mpi_volume_setup ( tidas::mpi_volume & vol, size_t n_samp, size_t n_intr,
        size_t n_block );

    void mpi_volume_verify ( tidas::mpi_volume & vol );

} }


class MPIvolumeTest : public ::testing::Test {

    public :

        MPIvolumeTest ();
        ~MPIvolumeTest () { }
        virtual void SetUp();
        virtual void TearDown() { }

        size_t chunk;
        size_t n_samp;
        size_t n_intr;
        size_t n_block;
        size_t n_big;
};



#endif
