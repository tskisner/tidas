/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>

#include <cstdlib>


using namespace std;
using namespace tidas;



// recall EXPECT_EQ(expected, actual)


TEST( utilstest, mem ) {

    size_t ntest = 10;

    double * test_double;
    float * test_float;
    int * test_int;
    long * test_long;
    size_t * test_sizet;
    int8_t * test_int8;
    uint8_t * test_uint8;
    int16_t * test_int16;
    uint16_t * test_uint16;
    int32_t * test_int32;
    uint32_t * test_uint32;
    int64_t * test_int64;
    uint64_t * test_uint64;

    test_double = mem_alloc < double > ( ntest );
    test_float = mem_alloc < float > ( ntest );
    test_int = mem_alloc < int > ( ntest );
    test_long = mem_alloc < long > ( ntest );
    test_sizet = mem_alloc < size_t > ( ntest );
    test_int8 = mem_alloc < int8_t > ( ntest );
    test_uint8 = mem_alloc < uint8_t > ( ntest );
    test_int16 = mem_alloc < int16_t > ( ntest );
    test_uint16 = mem_alloc < uint16_t > ( ntest );
    test_int32 = mem_alloc < int32_t > ( ntest );
    test_uint32 = mem_alloc < uint32_t > ( ntest );
    test_int64 = mem_alloc < int64_t > ( ntest );
    test_uint64 = mem_alloc < uint64_t > ( ntest );

    for ( size_t i = 0; i < ntest; ++i ) {
        test_double[i] = 1.234567890123456;
        test_float[i] = 1.234567;
        test_int8[i] = -123;
        test_uint8[i] = 123;
        test_int16[i] = -12345;
        test_uint16[i] = 12345;
        test_int32[i] = -1234567890;
        test_uint32[i] = 1234567890;
        test_int[i] = -1234567890;
        test_long[i] = -1234567890;
        test_sizet[i] = 1234567890;
        test_int64[i] = -123456789012345;
        test_uint64[i] = 123456789012345;
    }

    free ( test_double );
    free ( test_float );
    free ( test_int );
    free ( test_long );
    free ( test_sizet );
    free ( test_int8 );
    free ( test_uint8 );
    free ( test_int16 );
    free ( test_uint16 );
    free ( test_int32 );
    free ( test_uint32 );
    free ( test_int64 );
    free ( test_uint64 );

}


TEST( utilstest, datatype ) {

    data_type type;

    int8_t vint8;
    uint8_t vuint8;
    int16_t vint16;
    uint16_t vuint16;
    int32_t vint32;
    uint32_t vuint32;
    int64_t vint64;
    uint64_t vuint64;
    float vfloat32;
    double vfloat64;
    char * vstring;

    std::vector < int > bad;

    string typestr;
    data_type check;

    type = data_type_get ( typeid ( vint8 ) );
    EXPECT_EQ( data_type::int8, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "int8", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vuint8 ) );
    EXPECT_EQ( data_type::uint8, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "uint8", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vint16 ) );
    EXPECT_EQ( data_type::int16, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "int16", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vuint16 ) );
    EXPECT_EQ( data_type::uint16, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "uint16", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vint32 ) );
    EXPECT_EQ( data_type::int32, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "int32", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vuint32 ) );
    EXPECT_EQ( data_type::uint32, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "uint32", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vint64 ) );
    EXPECT_EQ( data_type::int64, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "int64", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vuint64 ) );
    EXPECT_EQ( data_type::uint64, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "uint64", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vfloat32 ) );
    EXPECT_EQ( data_type::float32, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "float32", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vfloat64 ) );
    EXPECT_EQ( data_type::float64, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "float64", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( vstring ) );
    EXPECT_EQ( data_type::string, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "string", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

    type = data_type_get ( typeid ( bad ) );
    EXPECT_EQ( data_type::none, type );
    typestr = data_type_to_string ( type );
    EXPECT_EQ( "none", typestr );
    check = data_type_from_string ( typestr );
    EXPECT_EQ( type, check );

}


TEST( utilstest, filesystem ) {

    string dir = "test_fs.out";
    fs_mkdir ( dir.c_str() );

    string file = "zeros.dat";
    size_t count = 10;

    string path = dir + "/" + file;

    ostringstream com;
    com.str("");

    com << "dd if=/dev/zero of=" << path << " bs=1 count=" << count;

    int ret = system ( com.str().c_str() );

    int64_t check = fs_stat ( path.c_str() );
    EXPECT_EQ( count, check );

    fs_rm ( path.c_str() );

    file = "blah.rnd";
    path = dir + "/" + file;
    com.str("");
    com << "dd if=/dev/random of=" << path << " bs=1 count=" << count;
    ret = system ( com.str().c_str() );

    fs_rm_r ( dir.c_str() );

}


TEST( utilstest, filter ) {

    string filt = "";
    string check = filter_default ( filt );
    EXPECT_EQ( ".*", check );

    filt = string("foo") + path_sep + string("bar");
    string local;
    string sub;
    bool stop;

    filter_block ( filt, local, sub, stop );
    EXPECT_EQ( "foo", local );
    EXPECT_EQ( "bar", sub );
    EXPECT_FALSE( stop );

    filt = string("foobar") + path_sep;
    filter_block ( filt, local, sub, stop );
    EXPECT_EQ( "foobar", local );
    EXPECT_EQ( "", sub );
    EXPECT_TRUE( stop );

    filt = string("foo") + path_sep + string("bar");
    check = path_sep + string("bar");
    filter_sub ( filt, local, sub );
    EXPECT_EQ( "foo", local );
    EXPECT_EQ( check, sub );

    filt = string("foo") + submatch_begin + string("bar");
    check = submatch_begin + string("bar");
    filter_sub ( filt, local, sub );
    EXPECT_EQ( "foo", local );
    EXPECT_EQ( check, sub );

    filt = string("foo") + submatch_sep + string("bar");
    check = submatch_sep + string("bar");
    filter_sub ( filt, local, sub );
    EXPECT_EQ( "foo", local );
    EXPECT_EQ( check, sub );

    string nested = submatch_begin
                    + string("a") + submatch_assign + string("b") + submatch_sep
                    + string("c") + submatch_assign + string("d") + submatch_begin
                    + string("x") + submatch_assign + string("y") + submatch_sep
                    + string("w") + submatch_assign + string("z") + submatch_end
                    + submatch_end;

    filt = submatch_begin 
            + string("foo1") + submatch_assign + string("bar1") + submatch_sep
            + string("foo2") + submatch_assign + string("bar2") + nested + submatch_sep
            + string("foo3") + submatch_assign + string("bar3") + submatch_sep
            + string("foo4") + submatch_assign + string("bar4") + nested
            + submatch_end;

    map < string, string > filts = filter_split ( filt );

    EXPECT_EQ( "bar1", filts[ "foo1" ] );

    check = "bar2" + nested;
    EXPECT_EQ( check, filts[ "foo2" ] );
    
    EXPECT_EQ( "bar3", filts[ "foo3" ] );

    check = "bar4" + nested;
    EXPECT_EQ( check, filts[ "foo4" ] );

    // now for a bigger test.  build up our filter from the inside out


    filt = string(".*") + path_sep;
    string blkcheck1 = filt;

    string grpsub = submatch_begin + schema_submatch_key + submatch_assign + string("field.*") + submatch_sep
                    + dict_submatch_key + submatch_assign + string("prop.*") + submatch_end;

    filts = filter_split ( grpsub );
    EXPECT_EQ( "field.*", filts[ schema_submatch_key ] );
    EXPECT_EQ( "prop.*", filts[ dict_submatch_key ] );

    string grpmatch = string(".*") + grpsub;
    filter_sub ( grpmatch, local, sub );
    EXPECT_EQ( ".*", local );
    EXPECT_EQ( grpsub, sub );

    string intrsub = submatch_begin + dict_submatch_key + submatch_assign + string("pr.*") + submatch_end;
    filts = filter_split ( intrsub );
    EXPECT_EQ( "pr.*", filts[ dict_submatch_key ] );

    string intrmatch = string("base.*") + intrsub;
    filter_sub ( intrmatch, local, sub );
    EXPECT_EQ( "base.*", local );
    EXPECT_EQ( intrsub, sub );

    string blksub = submatch_begin + group_submatch_key + submatch_assign + grpmatch + submatch_sep
                    + intervals_submatch_key + submatch_assign + intrmatch + submatch_end;

    filts = filter_split ( blksub );
    EXPECT_EQ( grpmatch, filts[ group_submatch_key ] );
    EXPECT_EQ( intrmatch, filts[ intervals_submatch_key ] );

    filt = string("13.*") + blksub + path_sep + filt;
    string blkcheck2 = filt;

    filt = string("2012.*") + blksub + path_sep + filt;
    string blkcheck3 = filt;

    filt = blksub + path_sep + filt;


    filter_block ( filt, local, sub, stop );
    EXPECT_EQ( blkcheck3, sub );
    EXPECT_FALSE( stop );

    EXPECT_EQ( blksub, local );
    
    string temp = sub;
    filter_block ( temp, local, sub, stop );
    EXPECT_EQ( blkcheck2, sub );
    EXPECT_FALSE( stop );

    string blkmatch;
    filter_sub ( local, blkmatch, check );
    EXPECT_EQ( "2012.*", blkmatch );
    EXPECT_EQ( blksub, check );

    temp = sub;
    filter_block ( temp, local, sub, stop );
    EXPECT_EQ( blkcheck1, sub );
    EXPECT_FALSE( stop );

    blkmatch;
    filter_sub ( local, blkmatch, check );
    EXPECT_EQ( "13.*", blkmatch );
    EXPECT_EQ( blksub, check );

    temp = sub;
    filter_block ( temp, local, sub, stop );
    EXPECT_EQ( "", sub );
    EXPECT_EQ( ".*", local );
    EXPECT_TRUE( stop );

}




