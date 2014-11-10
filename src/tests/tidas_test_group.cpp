/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;


TEST( grouptest, all ) {

	field f_int8;
	field f_uint8;
	field f_int16;
	field f_uint16;
	field f_int32;
	field f_uint32;
	field f_int64;
	field f_uint64;
	field f_float32;
	field f_float64;

	size_t nf = 10;

	f_int8.type = TYPE_INT8;
	f_int8.name = "int8";
	f_int8.units = "int8";

	f_uint8.type = TYPE_UINT8;
	f_uint8.name = "uint8";
	f_uint8.units = "uint8";

	f_int16.type = TYPE_INT16;
	f_int16.name = "int16";
	f_int16.units = "int16";

	f_uint16.type = TYPE_UINT16;
	f_uint16.name = "uint16";
	f_uint16.units = "uint16";

	f_int32.type = TYPE_INT32;
	f_int32.name = "int32";
	f_int32.units = "int32";

	f_uint32.type = TYPE_UINT32;
	f_uint32.name = "uint32";
	f_uint32.units = "uint32";

	f_int64.type = TYPE_INT64;
	f_int64.name = "int64";
	f_int64.units = "int64";

	f_uint64.type = TYPE_UINT64;
	f_uint64.name = "uint64";
	f_uint64.units = "uint64";

	f_float32.type = TYPE_FLOAT32;
	f_float32.name = "float32";
	f_float32.units = "float32";

	f_float64.type = TYPE_FLOAT64;
	f_float64.name = "float64";
	f_float64.units = "float64";

	field_list flist;
	flist.clear();
	flist.push_back ( f_int8 );
	flist.push_back ( f_uint8 );
	flist.push_back ( f_int16 );
	flist.push_back ( f_uint16 );
	flist.push_back ( f_int32 );
	flist.push_back ( f_uint32 );
	flist.push_back ( f_int64 );
	flist.push_back ( f_uint64 );
	flist.push_back ( f_float32 );
	flist.push_back ( f_float64 );

	schema schm ( flist );

	string sval = "blahblahblah";
	double dval = 12345.6;
	int ival = 12345;
	long long lval = 1234567890123456L;

	dict d;

	d.put ( "string", sval );
	d.put ( "double", dval );
	d.put ( "int", ival );
	d.put ( "longlong", lval );

	index_type nsamp = 10;

	// instantiate group

	group grp ( schm, d, nsamp );

	// write / read time data to memory

	vector < time_type > time ( nsamp );
	vector < time_type > check ( nsamp );

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

	for ( size_t i = 0; i < nsamp; ++i ) {
		time[i] = (double)i / 10.0;
		uint8_data[i] = (uint8_t)i;
		int8_data[i] = (int8_t)(-i);
		uint16_data[i] = (uint16_t)i;
		int16_data[i] = (int16_t)(-i);
		uint32_data[i] = (uint32_t)i;
		int32_data[i] = (int32_t)(-i);
		uint64_data[i] = (uint64_t)i;
		int64_data[i] = (int64_t)(-i);
		float_data[i] = (float)i / 10.0;
		double_data[i] = (double)i / 10.0;
	}

	int8_check.clear();
	uint8_check.clear();
	int16_check.clear();
	uint16_check.clear();
	int32_check.clear();
	uint32_check.clear();
	int64_check.clear();
	uint64_check.clear();
	float_check.clear();
	double_check.clear();

	grp.write_times ( time );
	grp.write_field ( "int8", 0, int8_data );
	grp.write_field ( "uint8", 0, uint8_data );
	grp.write_field ( "int16", 0, int16_data );
	grp.write_field ( "uint16", 0, uint16_data );
	grp.write_field ( "int32", 0, int32_data );
	grp.write_field ( "uint32", 0, uint32_data );
	grp.write_field ( "int64", 0, int64_data );
	grp.write_field ( "uint64", 0, uint64_data );
	grp.write_field ( "float32", 0, float_data );
	grp.write_field ( "float64", 0, double_data );

	grp.read_times ( check );
	grp.read_field ( "int8", 0, int8_check );
	grp.read_field ( "uint8", 0, uint8_check );
	grp.read_field ( "int16", 0, int16_check );
	grp.read_field ( "uint16", 0, uint16_check );
	grp.read_field ( "int32", 0, int32_check );
	grp.read_field ( "uint32", 0, uint32_check );
	grp.read_field ( "int64", 0, int64_check );
	grp.read_field ( "uint64", 0, uint64_check );
	grp.read_field ( "float32", 0, float_check );
	grp.read_field ( "float64", 0, double_check );

	for ( index_type i = 0; i < nsamp; ++i ) {
		EXPECT_EQ( time[i], check[i] );
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

	int8_check.clear();
	uint8_check.clear();
	int16_check.clear();
	uint16_check.clear();
	int32_check.clear();
	uint32_check.clear();
	int64_check.clear();
	uint64_check.clear();
	float_check.clear();
	double_check.clear();

	// HDF5 backend

#ifdef HAVE_HDF5

	backend_path loc;
	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_group.hdf5.out";
	loc.mode = MODE_RW;

	grp.duplicate ( loc );
	group grp2 ( loc );

	data_copy ( grp, grp2 );

	grp2.read_times ( check );
	grp2.read_field ( "int8", 0, int8_check );
	grp2.read_field ( "uint8", 0, uint8_check );
	grp2.read_field ( "int16", 0, int16_check );
	grp2.read_field ( "uint16", 0, uint16_check );
	grp2.read_field ( "int32", 0, int32_check );
	grp2.read_field ( "uint32", 0, uint32_check );
	grp2.read_field ( "int64", 0, int64_check );
	grp2.read_field ( "uint64", 0, uint64_check );
	grp2.read_field ( "float32", 0, float_check );
	grp2.read_field ( "float64", 0, double_check );

	for ( index_type i = 0; i < nsamp; ++i ) {
		EXPECT_EQ( time[i], check[i] );
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

	int8_check.clear();
	uint8_check.clear();
	int16_check.clear();
	uint16_check.clear();
	int32_check.clear();
	uint32_check.clear();
	int64_check.clear();
	uint64_check.clear();
	float_check.clear();
	double_check.clear();

	loc.name = "test_group_filt.hdf5.out";

	string filter = submatch_begin + schema_submatch_key + submatch_assign + string("int.*") + submatch_end;
	group grp3 ( grp, filter, loc );

	data_copy ( grp2, grp3 );

	grp3.read_times ( check );
	grp3.read_field ( "int8", 0, int8_check );
	grp3.read_field ( "int16", 0, int16_check );
	grp3.read_field ( "int32", 0, int32_check );
	grp3.read_field ( "int64", 0, int64_check );

	for ( index_type i = 0; i < nsamp; ++i ) {
		EXPECT_EQ( time[i], check[i] );
		EXPECT_EQ( int8_data[i], int8_check[i] );
		EXPECT_EQ( int16_data[i], int16_check[i] );
		EXPECT_EQ( int32_data[i], int32_check[i] );
		EXPECT_EQ( int64_data[i], int64_check[i] );
	}

	int8_check.clear();
	uint8_check.clear();
	int16_check.clear();
	uint16_check.clear();
	int32_check.clear();
	uint32_check.clear();
	int64_check.clear();
	uint64_check.clear();
	float_check.clear();
	double_check.clear();

	loc.name = "test_group_dup.hdf5.out";

	group grp4 ( grp2, "", loc );	
	data_copy ( grp2, grp4 );

	grp4.read_times ( check );
	grp4.read_field ( "int8", 0, int8_check );
	grp4.read_field ( "uint8", 0, uint8_check );
	grp4.read_field ( "int16", 0, int16_check );
	grp4.read_field ( "uint16", 0, uint16_check );
	grp4.read_field ( "int32", 0, int32_check );
	grp4.read_field ( "uint32", 0, uint32_check );
	grp4.read_field ( "int64", 0, int64_check );
	grp4.read_field ( "uint64", 0, uint64_check );
	grp4.read_field ( "float32", 0, float_check );
	grp4.read_field ( "float64", 0, double_check );

	for ( index_type i = 0; i < nsamp; ++i ) {
		EXPECT_EQ( time[i], check[i] );
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



#endif

}

