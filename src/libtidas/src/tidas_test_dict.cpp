/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>

#include <tidas_backend_hdf5.hpp>

#include <cstdlib>
#include <limits>


using namespace std;
using namespace tidas;


void tidas::test::dict_setup ( dict & dct ) {

    string sval = "blahblahblah";
    double dval = numeric_limits < double > :: max();
    float fval = numeric_limits < float > :: max();
    int8_t i8val = numeric_limits < int8_t > :: min();
    uint8_t u8val = numeric_limits < uint8_t > :: max();
    int16_t i16val = numeric_limits < int16_t > :: min();
    uint16_t u16val = numeric_limits < uint16_t > :: max();
    int32_t i32val = numeric_limits < int32_t > :: min();
    uint32_t u32val = numeric_limits < uint32_t > :: max();
    int64_t i64val = numeric_limits < int64_t > :: min();
    uint64_t u64val = numeric_limits < uint64_t > :: max();

    dct.clear();
    dct.put ( "string", sval );
    dct.put ( "double", dval );
    dct.put ( "float", fval );
    dct.put ( "int8", i8val );
    dct.put ( "uint8", u8val );
    dct.put ( "int16", i16val );
    dct.put ( "uint16", u16val );
    dct.put ( "int32", i32val );
    dct.put ( "uint32", u32val );
    dct.put ( "int64", i64val );
    dct.put ( "uint64", u64val );

}


void tidas::test::dict_verify ( dict const & d ) {

    string sval = "blahblahblah";
    double dval = numeric_limits < double > :: max();
    float fval = numeric_limits < float > :: max();
    int8_t i8val = numeric_limits < int8_t > :: min();
    uint8_t u8val = numeric_limits < uint8_t > :: max();
    int16_t i16val = numeric_limits < int16_t > :: min();
    uint16_t u16val = numeric_limits < uint16_t > :: max();
    int32_t i32val = numeric_limits < int32_t > :: min();
    uint32_t u32val = numeric_limits < uint32_t > :: max();
    int64_t i64val = numeric_limits < int64_t > :: min();
    uint64_t u64val = numeric_limits < uint64_t > :: max();

    EXPECT_EQ( sval, d.get < string > ( "string" ) );
    EXPECT_EQ( dval, d.get < double > ( "double" ) );
    EXPECT_EQ( fval, d.get < float > ( "float" ) );
    EXPECT_EQ( i8val, d.get < int8_t > ( "int8" ) );
    EXPECT_EQ( u8val, d.get < uint8_t > ( "uint8" ) );
    EXPECT_EQ( i16val, d.get < int16_t > ( "int16" ) );
    EXPECT_EQ( u16val, d.get < uint16_t > ( "uint16" ) );
    EXPECT_EQ( i32val, d.get < int32_t > ( "int32" ) );
    EXPECT_EQ( u32val, d.get < uint32_t > ( "uint32" ) );
    EXPECT_EQ( i64val, d.get < int64_t > ( "int64" ) );
    EXPECT_EQ( u64val, d.get < uint64_t > ( "uint64" ) );

    return;
}


void dictTest::SetUp () {
    test::dict_setup ( dct );
}


void dictTest::TearDown () {
}


TEST_F( dictTest, MetaOps ) {

    tidas::test::dict_verify ( dct );

    dict test ( dct );

    tidas::test::dict_verify ( test );

    backend_path loc;

    tidas::test::dict_verify ( test );

}


TEST_F( dictTest, HDF5Backend ) {

    // HDF5 backend

#ifdef HAVE_HDF5

    backend_path loc;
    loc.type = backend_type::hdf5;
    loc.mode = access_mode::write;
    loc.path = tidas::test::output_dir();
    fs_mkdir ( loc.path.c_str() );
    loc.name = "test_dict.hdf5.out";
    loc.meta = "/fakedata";

    // create a fake dataset in order to modify its attributes.

    string fspath = loc.path + path_sep + loc.name;
    int64_t fsize = fs_stat ( fspath.c_str() );

    hid_t file;
    if ( fsize > 0 ) {
        file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

        // check if dataset exists and delete it

        htri_t check = H5Lexists ( file, loc.meta.c_str(), H5P_DEFAULT );
        if ( check ) {
            herr_t ulink = H5Ldelete ( file, loc.meta.c_str(), H5P_DEFAULT );
        }

    } else {
        file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );
    }

    // create dataset

    hsize_t dims = 1;

    hid_t dataspace = H5Screate_simple ( 1, &dims, NULL );
    hid_t datatype = H5Tcopy ( H5T_NATIVE_DOUBLE );
    herr_t status = H5Tset_order ( datatype, H5T_ORDER_LE );
    hid_t dataset = H5Dcreate ( file, loc.meta.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

    status = H5Tclose ( datatype );
    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );
    status = H5Fclose ( file );

    // write / read dictionary

    dict test ( dct, ".*", loc );
    test.flush();

    test.sync();
    tidas::test::dict_verify ( test );

    // now test symlink


#else

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}
