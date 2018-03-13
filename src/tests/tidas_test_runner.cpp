/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>



std::string tidas::test::output_dir ( std::string const & path ) {
    static std::string outdir = "tidas_test_output";

    if ( path != "" ) {
        outdir = path;
    }

    return outdir;
}



int tidas::test::runner ( int argc, char *argv[] ) {
    std::string dir = tidas::test::output_dir();
    tidas::fs_mkdir ( dir.c_str() );
    ::testing::InitGoogleTest ( &argc, argv );
    ::testing::GTEST_FLAG(filter) = "-MPI*";
    return RUN_ALL_TESTS();
}
