/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>
  

using namespace std;
using namespace tidas;



tidas::intervals_backend_hdf5::intervals_backend_hdf5 () : intervals_backend () {

}


tidas::intervals_backend_hdf5::~intervals_backend_hdf5 () {

}


intervals_backend_hdf5 * tidas::intervals_backend_hdf5::clone () {
	intervals_backend_hdf5 * ret = new intervals_backend_hdf5 ( *this );
	return ret;
}


void tidas::intervals_backend_hdf5::read ( backend_path const & loc ) {

#ifdef HAVE_HDF5

	// check if file exists

	string fspath = loc.path + "/" + loc.name;

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize <= 0 ) {
		std::ostringstream o;
		o << "HDF5 intervals file " << fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// can we open file?

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// clean up

	herr_t status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


void tidas::intervals_backend_hdf5::write ( backend_path const & loc ) {

#ifdef HAVE_HDF5

	string fspath = loc.path + "/" + loc.name;

	// delete file if it exists

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize > 0 ) {
		fs_rm ( fspath.c_str() );
	}

	// create file

	hid_t file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );

	// clean up

	herr_t status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


void tidas::intervals_backend_hdf5::read_data ( backend_path const & loc, interval_list & intr ) {

#ifdef HAVE_HDF5

	// check if file exists

	string fspath = loc.path + "/" + loc.name;

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize <= 0 ) {
		std::ostringstream o;
		o << "HDF5 intervals file " << fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// clear out intervals

	intr.clear();

	// open file

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// read times

	string mpath = "/times";

	hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

	hid_t dataspace = H5Dget_space ( dataset );
	hid_t datatype = H5Dget_type ( dataset );

	int ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 1 ) {
		std::ostringstream o;
		o << "HDF5 intervals dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t time_dims;
	hsize_t maxdims;
	int ret = H5Sget_simple_extent_dims ( dataspace, &time_dims, &maxdims );

	double * time_buffer = mem_alloc < double > ( (size_t)time_dims );

	herr_t status = H5Dread ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, time_buffer );

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );

	// read indices

	mpath = "/indices";

	dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

	dataspace = H5Dget_space ( dataset );
	datatype = H5Dget_type ( dataset );

	ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 1 ) {
		std::ostringstream o;
		o << "HDF5 intervals dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t ind_dims;
	ret = H5Sget_simple_extent_dims ( dataspace, &ind_dims, &maxdims );

	if ( ind_dims != time_dims ) {
		std::ostringstream o;
		o << "HDF5 intervals dataset " << fspath << ": index length (" << ind_dims << ") is different than time length (" << time_dims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	int64_t * ind_buffer = mem_alloc < int64_t > ( (size_t)ind_dims );

	status = H5Dread ( dataset, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, ind_buffer );

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );

	// close file

	status = H5Fclose ( file ); 

	// copy buffers into intervals

	size_t n = (size_t)( ind_dims / 2 );

	for ( size_t i = 0; i < n; ++i ) {
		intr.push_back ( intrvl ( time_buffer[ 2 * i ], time_buffer[ 2 * i + 1 ], ind_buffer[ 2 * i ], ind_buffer[ 2 * i + 1 ] ) );
	}

	free ( ind_buffer );
	free ( time_buffer );

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


void tidas::intervals_backend_hdf5::write_data ( backend_path const & loc, interval_list const & intr ) {

#ifdef HAVE_HDF5

	// open file in write mode

	string fspath = loc.path + "/" + loc.name;

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

	// write time dataset

	hsize_t dims[1];
	dims[0] = 2 * intr.size();

	hid_t dataspace = H5Screate_simple ( 1, dims, NULL ); 

	hid_t datatype = H5Tcopy ( H5T_NATIVE_DOUBLE );

	herr_t status = H5Tset_order ( datatype, H5T_ORDER_LE );

	string mpath = "/times";

	hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	double * time_buffer = mem_alloc < double > ( dims[0] );

	size_t i = 0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		time_buffer[ 2 * i ] = it->start;
		time_buffer[ 2 * i + 1 ] = it->stop;
		++i;
	}

	status = H5Dwrite ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, time_buffer );

	free ( time_buffer );

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );

	// write index dataset

	dataspace = H5Screate_simple ( 1, dims, NULL ); 

	datatype = H5Tcopy ( H5T_NATIVE_INT64 );

	status = H5Tset_order ( datatype, H5T_ORDER_LE );

	mpath = "/indices";

	dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	int64_t * ind_buffer = mem_alloc < int64_t > ( dims[0] );

	i = 0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		ind_buffer[ 2 * i ] = it->first;
		ind_buffer[ 2 * i + 1 ] = it->last;
		++i;
	}

	status = H5Dwrite ( dataset, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, ind_buffer );

	free ( ind_buffer );

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );

	// close file

	status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


