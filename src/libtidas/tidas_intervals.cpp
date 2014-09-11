/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <float.h>
#include <math.h>

#ifdef HAVE_HDF5
extern "C" {
	#include <hdf5.h>
}
#endif


using namespace std;
using namespace tidas;


tidas::intervals_backend_mem::intervals_backend_mem () : intervals_backend () {

}

tidas::intervals_backend_mem::~intervals_backend_mem () {

}


intervals_backend_mem * tidas::intervals_backend_mem::clone () {
	intervals_backend_mem * ret = new intervals_backend_mem ( *this );
	return ret;
}


void tidas::intervals_backend_mem::read ( backend_path const & loc, interval_list & intr ) {
	intr = store_;
	return;
}


void tidas::intervals_backend_mem::write ( backend_path const & loc, interval_list const & intr ) {
	store_ = intr;
	return;
}


tidas::intervals_backend_hdf5::intervals_backend_hdf5 () : intervals_backend () {

}


tidas::intervals_backend_hdf5::~intervals_backend_hdf5 () {

}


intervals_backend_hdf5 * tidas::intervals_backend_hdf5::clone () {
	intervals_backend_hdf5 * ret = new intervals_backend_hdf5 ( *this );
	return ret;
}


void tidas::intervals_backend_hdf5::read ( backend_path const & loc, interval_list & intr ) {

#ifdef HAVE_HDF5

	// check if file exists

	int64_t fsize = fs_stat ( loc.fspath.c_str() );
	if ( fsize <= 0 ) {
		std::ostringstream o;
		o << "HDF5 intervals file " << loc.fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// clear out intervals

	intr.clear();

	// open file

	hid_t file = H5Fopen ( loc.fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	/* open dataset and get dimensions */

	string mpath = loc.metapath + "/" + loc.name;

	hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

	hid_t dataspace = H5Dget_space ( dataset );
	hid_t datatype = H5Dget_type ( dataset );

	int ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 1 ) {
		std::ostringstream o;
		o << "HDF5 intervals dataset " << loc.fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t dims;
	hsize_t maxdims;
	int ret = H5Sget_simple_extent_dims ( dataspace, &dims, &maxdims );

	double * buffer = double_alloc ( (size_t)dims );

	herr_t status = H5Dread ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	// clean up

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );
	status = H5Fclose ( file ); 

	// copy buffer into intervals

	size_t n = (size_t)( dims / 2 );

	for ( size_t i = 0; i < n; ++i ) {
		intr.push_back ( make_pair ( buffer[ 2 * i ], buffer[ 2 * i + 1] ) );
	}

	free ( buffer );

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


void tidas::intervals_backend_hdf5::write ( backend_path const & loc, interval_list const & intr ) {

#ifdef HAVE_HDF5

	// open file in write mode

	hid_t file = H5Fopen ( loc.fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

	// create dataset

	hsize_t dims[1];
	dims[0] = 2 * intr.size();

	hid_t dataspace = H5Screate_simple ( 1, dims, NULL ); 

	hid_t datatype = H5Tcopy ( H5T_NATIVE_DOUBLE );

	herr_t status = H5Tset_order ( datatype, H5T_ORDER_LE );

	string mpath = loc.metapath + "/" + loc.name;

	hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	// pack interval data into contiguous buffer

	double * buffer = double_alloc ( dims[0] );

	size_t i = 0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		buffer[ 2 * i ] = it->first;
		buffer[ 2 * i + 1 ] = it->second;
		++i;
	}

	status = H5Dwrite ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	free ( buffer );

	// clean up

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );
	status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif
	
	return;
}


tidas::intervals_backend_getdata::intervals_backend_getdata () : intervals_backend () {

}


tidas::intervals_backend_getdata::~intervals_backend_getdata () {

}


intervals_backend_getdata * tidas::intervals_backend_getdata::clone () {
	intervals_backend_getdata * ret = new intervals_backend_getdata ( *this );
	return ret;
}


void tidas::intervals_backend_getdata::read ( backend_path const & loc, interval_list & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write ( backend_path const & loc, interval_list const & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


tidas::intervals::intervals ( ) {
	backend_ = NULL;
}


tidas::intervals::intervals ( backend_path const & loc ) {
	backend_ = NULL;
	relocate ( loc );
}


tidas::intervals::intervals ( intervals const & orig ) {
	data_ = orig.data_;
	loc_ = orig.loc_;
	backend_ = orig.backend_->clone();
}


tidas::intervals::~intervals () {
	if ( backend_ ) {
		delete backend_;
	}
}


void tidas::intervals::relocate ( backend_path const & loc ) {
	loc_ = loc;

	if ( backend_ ) {
		delete backend_;
	}
	backend_ = NULL;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_ = new intervals_backend_mem ();
			break;
		case BACKEND_HDF5:
			backend_ = new intervals_backend_hdf5 ();
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}
	return;
}


backend_path tidas::intervals::location () {
	return loc_;
}


void tidas::intervals::clear () {
	data_.clear();
	return;
}


void tidas::intervals::append ( intrvl const & intr ) {
	data_.push_back ( intr );
	return;
}


void tidas::intervals::read () {
	data_.clear();
	backend_->read ( loc_, data_ );
	return;
}


void tidas::intervals::write () {
	backend_->write ( loc_, data_ );
	return;
}


intrvl const & tidas::intervals::get ( size_t indx ) const {
	if ( indx >= data_.size() ) {
		std::ostringstream o;
		o << "cannot get interval " << indx << " from list with " << data_.size() << " elements";
		TIDAS_THROW( o.str().c_str() );
	}
	return data_[ indx ];
}


intrvl tidas::intervals::seek ( time_type time ) const {

	intrvl ret = make_pair ( -1.0, -1.0 );

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = data_.begin(); it != data_.end(); ++it ) {
		if ( ( time >= it->first ) && ( time <= it->second ) ) {
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_ceil ( time_type time ) const {

	intrvl ret = make_pair ( -1.0, -1.0 );

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = data_.begin(); it != data_.end(); ++it ) {
		if ( time < it->second ) {
			// we are inside the interval, or after the stop of the previous one
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_floor ( time_type time ) const {

	intrvl ret = make_pair ( -1.0, -1.0 );

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = data_.begin(); it != data_.end(); ++it ) {
		if ( time > it->first ) {
			// we are inside the interval, or before the start of the next one
			ret = (*it);
		}
	}

	return ret;
}

