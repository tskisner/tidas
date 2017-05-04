/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>

#include <fstream>


using namespace std;
using namespace tidas;


class cereal_test {

    public :

        cereal_test() {
            itest = 1;
            stest = "test";
            dtest = -1.0;
        }

        ~cereal_test() {}

        int itest;
        string stest;
        double dtest;

        template < class Archive >
        void serialize ( Archive & ar ) {
            ar ( CEREAL_NVP( itest ) );
            ar ( CEREAL_NVP( stest ) );
            ar ( CEREAL_NVP( dtest ) );
            return;
        }

};


// recall EXPECT_EQ(expected, actual)


TEST( cerealtest, basic ) {

    cereal_test test;

    string path = tidas::test::output_dir();
    fs_mkdir ( path.c_str() );

    string fpath = path + "/cereal_test_bin.out";

    {
        ofstream os( fpath, ios::binary );
        cereal::PortableBinaryOutputArchive outarch ( os );
        outarch ( test );
    }

    {
        ifstream is( fpath, ios::binary );
        cereal::PortableBinaryInputArchive inarch ( is );
        inarch ( test );
    }

    EXPECT_EQ( 1, test.itest );
    EXPECT_EQ( string("test"), test.stest );
    EXPECT_EQ( -1.0, test.dtest );

    fpath = path + "/cereal_test_xml.out";

    {
        ofstream os( fpath );
        cereal::XMLOutputArchive outarch ( os );
        outarch ( test );
    }

    {
        ifstream is( fpath );
        cereal::XMLInputArchive inarch ( is );
        inarch ( test );
    }

    EXPECT_EQ( 1, test.itest );
    EXPECT_EQ( string("test"), test.stest );
    EXPECT_EQ( -1.0, test.dtest );

}


TEST( cerealtest, field ) {

    field test;

    string path = tidas::test::output_dir();
    fs_mkdir ( path.c_str() );

    path += "/cereal_test_field.out";

    {
        ofstream os( path, ios::binary );
        cereal::PortableBinaryOutputArchive outarch ( os );
        outarch ( test );
    }

    {
        ifstream is( path, ios::binary );
        cereal::PortableBinaryInputArchive inarch ( is );
        inarch ( test );
    }

    EXPECT_EQ( data_type::none, test.type );
    EXPECT_EQ( string(""), test.name );
    EXPECT_EQ( string(""), test.units );

}

