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


tidas::intrvl::intrvl () {
	start = -1.0;
	stop = -1.0;
	first = -1;
	last = -1;
}


tidas::intrvl::intrvl ( time_type new_start, time_type new_stop, index_type new_first, index_type new_last ) {
	start = new_start;
	stop = new_stop;
	first = new_first;
	last = new_last;
}


tidas::intrvl::~intrvl () {

}


tidas::intervals_backend_mem::intervals_backend_mem () : intervals_backend () {

}


tidas::intervals_backend_mem::~intervals_backend_mem () {

}


intervals_backend_mem * tidas::intervals_backend_mem::clone () {
	intervals_backend_mem * ret = new intervals_backend_mem ( *this );
	return ret;
}


void tidas::intervals_backend_mem::read ( backend_path const & loc ) {

	return;
}


void tidas::intervals_backend_mem::write ( backend_path const & loc ) {

	return;
}


void tidas::intervals_backend_mem::read_data ( backend_path const & loc, interval_list & intr ) {
	intr = store_;
	return;
}


void tidas::intervals_backend_mem::write_data ( backend_path const & loc, interval_list const & intr ) {
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


tidas::intervals_backend_getdata::intervals_backend_getdata () : intervals_backend () {

}


tidas::intervals_backend_getdata::~intervals_backend_getdata () {

}


intervals_backend_getdata * tidas::intervals_backend_getdata::clone () {
	intervals_backend_getdata * ret = new intervals_backend_getdata ( *this );
	return ret;
}


void tidas::intervals_backend_getdata::read ( backend_path const & loc ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write ( backend_path const & loc ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::read_data ( backend_path const & loc, interval_list & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write_data ( backend_path const & loc, interval_list const & intr ) {
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


void tidas::intervals::duplicate ( backend_path const & newloc ) {

	intervals newintervals ( newloc );
	newintervals.write_meta();

	interval_list data;
	read_data ( data );
	newintervals.write_data ( data );

	return;
}


void tidas::intervals::read_meta () {
	backend_->read ( loc_ );
	return;
}


void tidas::intervals::write_meta () {
	backend_->write ( loc_ );
	return;
}


void tidas::intervals::read_data ( interval_list & intr ) {
	backend_->read_data ( loc_, intr );
	return;
}


void tidas::intervals::write_data ( interval_list const & intr ) {
	backend_->write_data ( loc_, intr );
	return;
}


index_type tidas::intervals::total_samples ( interval_list const & intr ) {
	index_type tot = 0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		tot += it->last - it->first + 1;
	}
	return tot;
}


time_type tidas::intervals::total_time ( interval_list const & intr ) {
	time_type tot = 0.0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		tot += it->stop - it->start;
	}
	return tot;
}


intrvl tidas::intervals::seek ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( ( time >= it->start ) && ( time <= it->stop ) ) {
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_ceil ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( time < it->stop ) {
			// we are inside the interval, or after the stop of the previous one
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_floor ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( time > it->start ) {
			// we are inside the interval, or before the start of the next one
			ret = (*it);
		}
	}

	return ret;
}

