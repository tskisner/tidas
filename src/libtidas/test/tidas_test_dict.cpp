/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>

#include <tidas_backend_hdf5.hpp>

#include <cstdlib>
#include <limits>


using namespace std;
using namespace tidas;


void dict_setup ( dict & dct, ctidas_dict * cdct ) {

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

	if ( cdct != NULL ) {
		ctidas_dict_clear( cdct );
		ctidas_dict_put_string ( cdct, "string", sval.c_str() );
		ctidas_dict_put_double ( cdct, "double", dval );
		ctidas_dict_put_float ( cdct, "float", fval );
		ctidas_dict_put_int8 ( cdct, "int8", i8val );
		ctidas_dict_put_uint8 ( cdct, "uint8", u8val );
		ctidas_dict_put_int16 ( cdct, "int16", i16val );
		ctidas_dict_put_uint16 ( cdct, "uint16", u16val );
		ctidas_dict_put_int32 ( cdct, "int32", i32val );
		ctidas_dict_put_uint32 ( cdct, "uint32", u32val );
		ctidas_dict_put_int64 ( cdct, "int64", i64val );
		ctidas_dict_put_uint64 ( cdct, "uint64", u64val );
	}

}


void dict_verify ( dict const & d, ctidas_dict const * cdct ) {

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
	
	if ( cdct != NULL ) {
		char * csval = ctidas_dict_get_string ( cdct, "string" );
		string cstr(csval);
		free(csval);
		EXPECT_EQ( sval, cstr );
		EXPECT_EQ( dval, ctidas_dict_get_double ( cdct, "double" ) );
		EXPECT_EQ( fval, ctidas_dict_get_float ( cdct, "float" ) );
		EXPECT_EQ( i8val, ctidas_dict_get_int8 ( cdct, "int8" ) );
		EXPECT_EQ( u8val, ctidas_dict_get_uint8 ( cdct, "uint8" ) );
		EXPECT_EQ( i16val, ctidas_dict_get_int16 ( cdct, "int16" ) );
		EXPECT_EQ( u16val, ctidas_dict_get_uint16 ( cdct, "uint16" ) );
		EXPECT_EQ( i32val, ctidas_dict_get_int32 ( cdct, "int32" ) );
		EXPECT_EQ( u32val, ctidas_dict_get_uint32 ( cdct, "uint32" ) );
		EXPECT_EQ( i64val, ctidas_dict_get_int64 ( cdct, "int64" ) );
		EXPECT_EQ( u64val, ctidas_dict_get_uint64 ( cdct, "uint64" ) );
	}

	return;
}


void dictTest::SetUp () {
	cdct = ctidas_dict_alloc();
	dict_setup ( dct, cdct );
}


void dictTest::TearDown () {
	ctidas_dict_free ( cdct );
}


TEST_F( dictTest, MetaOps ) {

	dict_verify ( dct, cdct );

	dict test ( dct );

	dict_verify ( test, NULL );

	backend_path loc;

	dict_verify ( test, NULL );

}


TEST_F( dictTest, HDF5Backend ) {

	// HDF5 backend

#ifdef HAVE_HDF5

	backend_path loc;
	loc.type = backend_type::hdf5;
	loc.mode = access_mode::write;
	loc.path = ".";
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
	dict_verify ( test, NULL );

	// now test symlink


#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}







