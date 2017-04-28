/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
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

    ::testing::InitGoogleTest ( &argc, argv );

    int result = RUN_ALL_TESTS();

    return result;
}

