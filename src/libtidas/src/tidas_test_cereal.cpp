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


TEST( cerealtest, transaction ) {

    std::deque < indexdb_transaction > data;

    indexdb_transaction trans1;
    indexdb_transaction trans2;

    trans1.op = indexdb_op::add;
    indexdb_intervals * idbint = new indexdb_intervals();
    idbint->size = 123;
    trans1.obj.reset ( idbint );
    data.push_back ( trans1 );

    trans2.op = indexdb_op::add;
    idbint = new indexdb_intervals();
    idbint->size = 456;
    trans2.obj.reset ( idbint );
    data.push_back ( trans2 );

    std::ostringstream ostrm;
    {
        cereal::PortableBinaryOutputArchive outarch ( ostrm );
        outarch ( data );
    }

    std::string ostr = ostrm.str();

    size_t bufsize = ostr.size();

    std::vector < char > charbuf ( bufsize );
    std::copy ( ostr.begin(), ostr.end(), charbuf.begin() );

    std::string strbuf ( &charbuf[0], bufsize );
    std::istringstream istrm ( strbuf );

    std::deque < indexdb_transaction > check;

    {
        cereal::PortableBinaryInputArchive inarch ( istrm );
        inarch ( check );
    }

    for ( size_t i = 0; i < 2; ++i ) {
        EXPECT_EQ( data[i].op, check[i].op );
        indexdb_intervals * dp = dynamic_cast < indexdb_intervals * > ( data[i].obj.get() );
        indexdb_intervals * cp = dynamic_cast < indexdb_intervals * > ( check[i].obj.get() );
        EXPECT_EQ( dp->type, cp->type );
        EXPECT_EQ( dp->path, cp->path );
        EXPECT_EQ( dp->size, cp->size );
    }

}




