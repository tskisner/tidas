/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



TEST( schematest, all ) {

	schema schm1;

	// should return null field
	field ftest = schm1.seek ( "dummy" );
	EXPECT_EQ( ftest.name, "" );
	EXPECT_EQ( ftest.units, "" );
	EXPECT_EQ( ftest.type, TYPE_NONE );

	// should return empty list
	field_list flist = schm1.fields();
	EXPECT_EQ( flist.size(), 0 );

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

	schema schm2 ( flist );

	field_list check = schm2.fields();

	EXPECT_EQ( check.size(), nf );
	for ( size_t i = 0; i < nf; ++i ) {
		EXPECT_EQ( flist[i], check[i] );	
	}

	schema schm3 ( schm2 );

	check = schm3.fields();

	EXPECT_EQ( check.size(), nf );
	for ( size_t i = 0; i < nf; ++i ) {
		EXPECT_EQ( flist[i], check[i] );	
	}

	backend_path loc;

#ifdef HAVE_HDF5

	// test write / read

	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_schema.hdf5.out";
	loc.meta = string("/") + schema_hdf5_dataset;
	loc.mode = MODE_RW;

	schm3.duplicate ( loc );

	schema schm4 ( loc );

	check = schm4.fields();
	for ( size_t i = 0; i < nf; ++i ) {
		EXPECT_EQ( flist[i], check[i] );	
	}

	// test filtered copy

	backend_path memloc;

	schema h5_ischm ( schm4, "int.*", memloc );
	EXPECT_EQ( h5_ischm.fields().size(), 4 );

	schema h5_aischm ( schm4, ".*int.*", memloc );
	EXPECT_EQ( h5_aischm.fields().size(), 8 );

#endif

	// FIXME:  add getdata backend test

	schm3.remove ( "int8" );
	schm3.remove ( "uint8" );
	schm3.remove ( "int16" );
	schm3.remove ( "uint16" );
	schm3.remove ( "int32" );
	schm3.remove ( "uint32" );
	schm3.remove ( "int64" );
	schm3.remove ( "uint64" );
	schm3.remove ( "float32" );
	schm3.remove ( "float64" );

	schm3.append ( f_int8 );
	schm3.append ( f_uint8 );
	schm3.append ( f_int16 );
	schm3.append ( f_uint16 );
	schm3.append ( f_int32 );
	schm3.append ( f_uint32 );
	schm3.append ( f_int64 );
	schm3.append ( f_uint64 );
	schm3.append ( f_float32 );
	schm3.append ( f_float64 );

	check = schm3.fields();

	for ( size_t i = 0; i < nf; ++i ) {
		EXPECT_EQ( flist[i], check[i] );	
	}

}

