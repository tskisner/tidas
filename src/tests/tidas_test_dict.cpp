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


TEST( dictionarytest, all ) {

	string sval = "blahblahblah";
	double dval = 12345.6;
	int ival = 12345;
	long long lval = 1234567890123456L;

	dict test;

	test.put ( "string", sval );
	test.put ( "double", dval );
	test.put ( "int", ival );
	test.put ( "longlong", lval );

	string scheck = test.get_string ( "string" );
	EXPECT_EQ( sval, scheck );

	double dcheck = test.get_double ( "double" );
	EXPECT_EQ( dval, dcheck );

	int icheck = test.get_int ( "int" );
	EXPECT_EQ( ival, icheck );

	long long lcheck = test.get_ll ( "longlong" );
	EXPECT_EQ( lval, lcheck );

	// memory backend

	backend_path loc;
	loc.mode = MODE_RW;

	test.write ( loc );

	dict test2 ( test );

	scheck = test2.get_string ( "string" );
	EXPECT_EQ( sval, scheck );

	dcheck = test2.get_double ( "double" );
	EXPECT_EQ( dval, dcheck );

	icheck = test2.get_int ( "int" );
	EXPECT_EQ( ival, icheck );

	lcheck = test2.get_ll ( "longlong" );
	EXPECT_EQ( lval, lcheck );

	// HDF5 backend

#ifdef HAVE_HDF5

	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_dict.hdf5.out";
	loc.meta = "/fakedata";

	// create a fake dataset in order to modify its attributes.

	string fspath = loc.path + "/" + loc.name;
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

	test.write ( loc );

	dict test3 ( test );

	scheck = test3.get_string ( "string" );
	EXPECT_EQ( sval, scheck );

	dcheck = test3.get_double ( "double" );
	EXPECT_EQ( dval, dcheck );

	icheck = test3.get_int ( "int" );
	EXPECT_EQ( ival, icheck );

	lcheck = test3.get_ll ( "longlong" );
	EXPECT_EQ( lval, lcheck );

#endif

	// getdata backend

	// FIXME:  implement


}







