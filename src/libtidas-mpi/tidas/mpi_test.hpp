/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_MPI_TEST_HPP
#define TIDAS_MPI_TEST_HPP

#include <tidas_test.hpp>


class mpivolumeTest : public ::testing::Test {

    public :

        mpivolumeTest ();
        ~mpivolumeTest () { }
        virtual void SetUp();
        virtual void TearDown() { }

        size_t chunk;
        size_t n_samp;
        size_t n_intr;
        size_t n_block;
        size_t n_big;
};

void mpi_volume_setup ( tidas::volume & vol, size_t n_samp, size_t n_intr, size_t n_block );

void mpi_volume_verify ( tidas::volume & vol );



#endif
