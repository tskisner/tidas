/*
Copyright (c) 2015-2017 by the parties listed in the AUTHORS file.
All rights reserved.  Use of this source code is governed by 
a BSD-style license that can be found in the LICENSE file.
*/


#include <tidas_test.hpp>


int tidas::test::runner ( int argc, char *argv[] ) {

    ::testing::InitGoogleTest ( &argc, argv );

    int result = RUN_ALL_TESTS();

    return result;
}

