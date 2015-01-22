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

	f_int8.type = data_type::int8;
	f_int8.name = "int8";
	f_int8.units = "int8";

	f_uint8.type = data_type::uint8;
	f_uint8.name = "uint8";
	f_uint8.units = "uint8";

	f_int16.type = data_type::int16;
	f_int16.name = "int16";
	f_int16.units = "int16";

	f_uint16.type = data_type::uint16;
	f_uint16.name = "uint16";
	f_uint16.units = "uint16";

	f_int32.type = data_type::int32;
	f_int32.name = "int32";
	f_int32.units = "int32";

	f_uint32.type = data_type::uint32;
	f_uint32.name = "uint32";
	f_uint32.units = "uint32";

	f_int64.type = data_type::int64;
	f_int64.name = "int64";
	f_int64.units = "int64";

	f_uint64.type = data_type::uint64;
	f_uint64.name = "uint64";
	f_uint64.units = "uint64";

	f_float32.type = data_type::float32;
	f_float32.name = "float32";
	f_float32.units = "float32";

	f_float64.type = data_type::float64;
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

	EXPECT_EQ( check.size(), flist.size() );

	for ( size_t i = 0; i < flist.size(); ++i ) {
		EXPECT_EQ( check[i], flist[i] );	
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
		EXPECT_EQ( ftest.type, data_type::none );
	} catch(...) {
		cout << "schema::field_get successfully threw exception" << endl;
	}

	// should return empty list
	field_list flist = schm.fields();
	EXPECT_EQ( 0, flist.size() );

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

	EXPECT_EQ( 0, schm.fields().size() );

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
	EXPECT_EQ( 4, filt_ischm.fields().size() );

	schema filt_aischm ( schm, ".*int.*", loc );
	EXPECT_EQ( 8, filt_aischm.fields().size() );

}


TEST_F( schemaTest, HDF5Backend ) {

	field_list flist;

	schema_setup ( flist );

	schema schm ( flist );

#ifdef HAVE_HDF5

	backend_path loc;
	loc.type = backend_type::hdf5;
	loc.path = ".";
	loc.name = "test_schema.hdf5.out";
	loc.meta = string("/") + schema_hdf5_dataset;
	loc.mode = access_mode::readwrite;

	schema schm2 ( schm, ".*", loc );
	schm2.flush();

	schema schm3 ( loc );

	schema_verify ( schm3.fields() );

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}

