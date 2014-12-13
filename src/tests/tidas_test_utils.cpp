/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>

#include <cstdlib>


using namespace std;
using namespace tidas;



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
	string vstring;

	std::vector < int > bad;

	string typestr;
	data_type check;

	type = data_type_get ( typeid ( vint8 ) );
	EXPECT_EQ( type, TYPE_INT8 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_INT8" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vuint8 ) );
	EXPECT_EQ( type, TYPE_UINT8 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_UINT8" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vint16 ) );
	EXPECT_EQ( type, TYPE_INT16 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_INT16" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vuint16 ) );
	EXPECT_EQ( type, TYPE_UINT16 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_UINT16" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vint32 ) );
	EXPECT_EQ( type, TYPE_INT32 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_INT32" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vuint32 ) );
	EXPECT_EQ( type, TYPE_UINT32 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_UINT32" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vint64 ) );
	EXPECT_EQ( type, TYPE_INT64 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_INT64" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vuint64 ) );
	EXPECT_EQ( type, TYPE_UINT64 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_UINT64" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vfloat32 ) );
	EXPECT_EQ( type, TYPE_FLOAT32 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_FLOAT32" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vfloat64 ) );
	EXPECT_EQ( type, TYPE_FLOAT64 );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_FLOAT64" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( vstring ) );
	EXPECT_EQ( type, TYPE_STRING );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_STRING" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

	type = data_type_get ( typeid ( bad ) );
	EXPECT_EQ( type, TYPE_NONE );
	typestr = data_type_to_string ( type );
	EXPECT_EQ( typestr, "TYPE_NONE" );
	check = data_type_from_string ( typestr );
	EXPECT_EQ( check, type );

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
	EXPECT_EQ( check, count );

	fs_rm ( path.c_str() );

	file = "blah.rnd";
	path = dir + "/" + file;
	com.str("");
	com << "dd if=/dev/random of=" << path << " bs=1 count=" << count;
	ret = system ( com.str().c_str() );

	fs_rm_r ( dir.c_str() );

}





