/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



void group_setup ( group & grp, size_t offset, size_t full_nsamp ) {

    // write data

    size_t nsamp = (size_t)( full_nsamp / 2 );

    vector < time_type > time ( full_nsamp );

    vector < int8_t > int8_data ( nsamp );
    vector < uint8_t > uint8_data ( nsamp );

    vector < int16_t > int16_data ( nsamp );
    vector < uint16_t > uint16_data ( nsamp );

    vector < int32_t > int32_data ( nsamp );
    vector < uint32_t > uint32_data ( nsamp );

    vector < int64_t > int64_data ( nsamp );
    vector < uint64_t > uint64_data ( nsamp );

    vector < float > float_data ( nsamp );
    vector < double > double_data ( nsamp );

    for ( size_t i = 0; i < full_nsamp; ++i ) {
        time[i] = (double)( offset + i ) / 10.0;
    }

    for ( size_t i = 0; i < nsamp; ++i ) {
        uint8_data[i] = (uint8_t)i;
        int8_data[i] = -(int8_t)(i);
        uint16_data[i] = (uint16_t)i;
        int16_data[i] = -(int16_t)(i);
        uint32_data[i] = (uint32_t)i;
        int32_data[i] = -(int32_t)(i);
        uint64_data[i] = (uint64_t)i;
        int64_data[i] = -(int64_t)(i);
        float_data[i] = (float)i / 10.0;
        double_data[i] = (double)i / 10.0;
    }

    size_t off = offset + full_nsamp - nsamp;

    grp.write_times ( time );
    grp.write_field ( "int8", off, int8_data );
    grp.write_field ( "uint8", off, uint8_data );
    grp.write_field ( "int16", off, int16_data );
    grp.write_field ( "uint16", off, uint16_data );
    grp.write_field ( "int32", off, int32_data );
    grp.write_field ( "uint32", off, uint32_data );
    grp.write_field ( "int64", off, int64_data );
    grp.write_field ( "uint64", off, uint64_data );
    grp.write_field ( "float32", off, float_data );
    grp.write_field ( "float64", off, double_data );

    return;
}


void group_verify ( group & grp, size_t offset, size_t full_nsamp ) {

    // read data and check

    size_t nsamp = (size_t)( full_nsamp / 2 );

    vector < time_type > time ( full_nsamp );
    vector < time_type > check ( full_nsamp );

    vector < int8_t > int8_data ( nsamp );
    vector < int8_t > int8_check ( nsamp );
    vector < uint8_t > uint8_data ( nsamp );
    vector < uint8_t > uint8_check ( nsamp );

    vector < int16_t > int16_data ( nsamp );
    vector < int16_t > int16_check ( nsamp );
    vector < uint16_t > uint16_data ( nsamp );
    vector < uint16_t > uint16_check ( nsamp );

    vector < int32_t > int32_data ( nsamp );
    vector < int32_t > int32_check ( nsamp );
    vector < uint32_t > uint32_data ( nsamp );
    vector < uint32_t > uint32_check ( nsamp );

    vector < int64_t > int64_data ( nsamp );
    vector < int64_t > int64_check ( nsamp );
    vector < uint64_t > uint64_data ( nsamp );
    vector < uint64_t > uint64_check ( nsamp );

    vector < float > float_data ( nsamp );
    vector < float > float_check ( nsamp );
    vector < double > double_data ( nsamp );
    vector < double > double_check ( nsamp );

    for ( size_t i = 0; i < full_nsamp; ++i ) {
        time[i] = (double)( offset + i ) / 10.0;
    }

    for ( size_t i = 0; i < nsamp; ++i ) {
        uint8_data[i] = (uint8_t)i;
        int8_data[i] = -(int8_t)(i);
        uint16_data[i] = (uint16_t)i;
        int16_data[i] = -(int16_t)(i);
        uint32_data[i] = (uint32_t)i;
        int32_data[i] = -(int32_t)(i);
        uint64_data[i] = (uint64_t)i;
        int64_data[i] = -(int64_t)(i);
        float_data[i] = (float)i / 10.0;
        double_data[i] = (double)i / 10.0;
    }

    check.assign ( full_nsamp, 0 );
    int8_check.assign ( nsamp, 0 );
    uint8_check.assign ( nsamp, 0 );
    int16_check.assign ( nsamp, 0 );
    uint16_check.assign ( nsamp, 0 );
    int32_check.assign ( nsamp, 0 );
    uint32_check.assign ( nsamp, 0 );
    int64_check.assign ( nsamp, 0 );
    uint64_check.assign ( nsamp, 0 );
    float_check.assign ( nsamp, 0 );
    double_check.assign ( nsamp, 0 );

    size_t off = offset + full_nsamp - nsamp;

    grp.read_times ( check );
    grp.read_field ( "int8", off, int8_check );
    grp.read_field ( "uint8", off, uint8_check );
    grp.read_field ( "int16", off, int16_check );
    grp.read_field ( "uint16", off, uint16_check );
    grp.read_field ( "int32", off, int32_check );
    grp.read_field ( "uint32", off, uint32_check );
    grp.read_field ( "int64", off, int64_check );
    grp.read_field ( "uint64", off, uint64_check );
    grp.read_field ( "float32", off, float_check );
    grp.read_field ( "float64", off, double_check );

    for ( index_type i = 0; i < full_nsamp; ++i ) {
        EXPECT_EQ( time[i], check[i] );
    }

    for ( index_type i = 0; i < nsamp; ++i ) {
        EXPECT_EQ( int8_data[i], int8_check[i] );
        EXPECT_EQ( uint8_data[i], uint8_check[i] );
        EXPECT_EQ( int16_data[i], int16_check[i] );
        EXPECT_EQ( uint16_data[i], uint16_check[i] );
        EXPECT_EQ( int32_data[i], int32_check[i] );
        EXPECT_EQ( uint32_data[i], uint32_check[i] );
        EXPECT_EQ( int64_data[i], int64_check[i] );
        EXPECT_EQ( uint64_data[i], uint64_check[i] );
        EXPECT_EQ( float_data[i], float_check[i] );
        EXPECT_EQ( double_data[i], double_check[i] );
    }

    return;
}


void group_verify_int ( group & grp, size_t offset, size_t full_nsamp ) {

    // read data and check

    size_t nsamp = (size_t)( full_nsamp / 2 );

    vector < time_type > time ( full_nsamp );
    vector < time_type > check ( full_nsamp );

    vector < int8_t > int8_data ( nsamp );
    vector < int8_t > int8_check ( nsamp );

    vector < int16_t > int16_data ( nsamp );
    vector < int16_t > int16_check ( nsamp );

    vector < int32_t > int32_data ( nsamp );
    vector < int32_t > int32_check ( nsamp );

    vector < int64_t > int64_data ( nsamp );
    vector < int64_t > int64_check ( nsamp );

    for ( size_t i = 0; i < full_nsamp; ++i ) {
        time[i] = (double)( offset + i ) / 10.0;
    }

    for ( size_t i = 0; i < nsamp; ++i ) {
        int8_data[i] = (int8_t)(-i);
        int16_data[i] = (int16_t)(-i);
        int32_data[i] = (int32_t)(-i);
        int64_data[i] = (int64_t)(-i);
    }

    check.assign ( full_nsamp, 0 );
    int8_check.assign ( nsamp, 0 );
    int16_check.assign ( nsamp, 0 );
    int32_check.assign ( nsamp, 0 );
    int64_check.assign ( nsamp, 0 );

    size_t off = offset + full_nsamp - nsamp;

    grp.read_times ( check );
    grp.read_field ( "int8", off, int8_check );
    grp.read_field ( "int16", off, int16_check );
    grp.read_field ( "int32", off, int32_check );
    grp.read_field ( "int64", off, int64_check );

    for ( index_type i = 0; i < full_nsamp; ++i ) {
        EXPECT_EQ( time[i], check[i] );
    }

    for ( index_type i = 0; i < nsamp; ++i ) {
        EXPECT_EQ( int8_data[i], int8_check[i] );
        EXPECT_EQ( int16_data[i], int16_check[i] );
        EXPECT_EQ( int32_data[i], int32_check[i] );
        EXPECT_EQ( int64_data[i], int64_check[i] );
    }

    return;
}


groupTest::groupTest () {

}


void groupTest::SetUp () {
    gnsamp = 10 + hdf5_chunk_default;
}


TEST_F( groupTest, MetaOps ) {

    dict dt;
    dict_setup ( dt, NULL );

    field_list flist;
    schema_setup ( flist );
    schema schm ( flist );

    group grp ( schm, dt, gnsamp );

}


TEST_F( groupTest, HDF5Backend ) {

    // HDF5 backend

#ifdef HAVE_HDF5

    dict dt;
    dict_setup ( dt, NULL );

    field_list flist;
    schema_setup ( flist );
    schema schm ( flist );

    group grp ( schm, dt, gnsamp );

    backend_path loc;
    loc.type = backend_type::hdf5;
    loc.path = tidas::test::output_dir();
    fs_mkdir ( loc.path.c_str() );
    loc.name = "test_group.hdf5.out";
    loc.mode = access_mode::write;

    group grp2 ( grp, "", loc );
    grp2.flush();

    // write test data
    group_setup ( grp2, 0, grp2.size() );

    // read and verify
    group_verify ( grp2, 0, grp2.size() );

    // construct from location and verify

    group grp3 ( loc );
    group_verify ( grp3, 0, grp3.size() );

    // create filtered copy and check

    backend_path filtloc = loc;
    filtloc.name = "test_group_filt.hdf5.out";

    string filter = submatch_begin + schema_submatch_key + submatch_assign + string("int.*") + submatch_end;

    group grp4 ( grp2, filter, filtloc );
    grp4.flush();

    data_copy ( grp2, grp4 );

    group_verify_int ( grp4, 0, grp4.size() );

    // test resizing and compression

    backend_path resizeloc = loc;
    resizeloc.name = "test_group_resize.hdf.out";
    resizeloc.comp = compression_type::gzip;

    group grp5 ( grp2, "", resizeloc );
    grp5.flush();

    data_copy ( grp2, grp5 );

    grp5.resize ( 2 * gnsamp );
    group_setup ( grp5, 0, 2 * gnsamp );

    group_verify ( grp5, 0, (2 * gnsamp) );

#else

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}

