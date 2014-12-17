/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



void schema_setup ( tidas::field_list & flist ) {

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



	return;
}


void schema_verify ( tidas::field_list const & flist ) {

	tidas::field_list check;

	schema_setup ( check );

	EXPECT_EQ( flist.size(), check.size() );

	for ( size_t i = 0; i < flist.size(); ++i ) {
		EXPECT_EQ( flist[i], check[i] );	
	}

	return;
}


schemaTest::schemaTest () {

}


TEST_F( schemaTest, MetaOps ) {

	schema schm;

	// should throw
	try {
		field ftest = schm.field_get ( "dummy" );
		EXPECT_EQ( ftest.name, "" );
		EXPECT_EQ( ftest.units, "" );
		EXPECT_EQ( ftest.type, TYPE_NONE );
	} catch(...) {
		cout << "schema::field_get successfully threw exception" << endl;
	}

	// should return empty list
	field_list flist = schm.fields();
	EXPECT_EQ( flist.size(), 0 );

	schema_setup ( flist );

	schema schm2 ( flist );

	schema_verify ( schm2.fields() );

}


TEST_F( schemaTest, AddRemove ) {

	field_list flist;

	schema_setup ( flist );

	schema schm ( flist );

	for ( size_t i = 0; i < flist.size(); ++i ) {
		schm.field_del ( flist[i].name );
	}

	EXPECT_EQ( schm.fields().size(), 0 );

	for ( size_t i = 0; i < flist.size(); ++i ) {
		schm.field_add ( flist[i] );
	}

	schema_verify ( schm.fields() );

}


TEST_F( schemaTest, Filter ) {

	field_list flist;

	schema_setup ( flist );

	schema schm ( flist );

	backend_path loc;

	schema filt_ischm ( schm, "int.*", loc );
	EXPECT_EQ( filt_ischm.fields().size(), 4 );

	schema filt_aischm ( schm, ".*int.*", loc );
	EXPECT_EQ( filt_aischm.fields().size(), 8 );

}


TEST_F( schemaTest, HDF5Backend ) {

	field_list flist;

	schema_setup ( flist );

	schema schm ( flist );

#ifdef HAVE_HDF5

	backend_path loc;
	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_schema.hdf5.out";
	loc.meta = string("/") + schema_hdf5_dataset;
	loc.mode = MODE_RW;

	schema schm2 ( schm, ".*", loc );
	schm2.flush();

	schema schm3 ( loc );

	schema_verify ( schm3.fields() );

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}

