/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


tidas::group_backend_hdf5::group_backend_hdf5 ( ) {

}


tidas::group_backend_hdf5::~group_backend_hdf5 () {
	
}


tidas::group_backend_hdf5::group_backend_hdf5 ( group_backend_hdf5 const & other ) {

}


group_backend_hdf5 & tidas::group_backend_hdf5::operator= ( group_backend_hdf5 const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


group_backend * tidas::group_backend_hdf5::clone () {
	group_backend_hdf5 * ret = new group_backend_hdf5 ( *this );
	return ret;
}


void tidas::group_backend_hdf5::read_meta ( backend_path const & loc, string const & filter, index_type & nsamp ) {

#ifdef HAVE_HDF5

	// check if file exists

	string fspath = loc.path + "/" + loc.name;

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize <= 0 ) {
		ostringstream o;
		o << "HDF5 group file " << fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// open file

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// open fake dataset to get nsamp dimensions

	string mpath = "/" + group_meta;

	hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
	
	hid_t dataspace = H5Dget_space ( dataset );

	int ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 1 ) {
		ostringstream o;
		o << "HDF5 group dataset " << fspath << ":" << loc.meta << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t dims;
	hsize_t maxdims;
	int ret = H5Sget_simple_extent_dims ( dataspace, &dims, &maxdims );

	nsamp = dims;

	herr_t status = H5Sclose ( dataspace );
	status = H5Dclose ( dataset );
	status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::write_meta ( backend_path const & loc, string const & filter, schema const & schm, index_type nsamp ) {

#ifdef HAVE_HDF5

	string fspath = loc.path + "/" + loc.name;

	// delete file if it exists

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize > 0 ) {
		fs_rm ( fspath.c_str() );
	}

	// create file

	hid_t file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );

	// create fake dataset for dictionary and dimension check

	hsize_t dims[1];
	dims[0] = nsamp;

	hid_t dataspace = H5Screate_simple ( 1, dims, NULL ); 

	hid_t datatype = hdf5_data_type ( TYPE_INT8 );

	string mpath = "/" + group_meta;

	hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	herr_t status = H5Sclose ( dataspace );
	status = H5Tclose ( datatype );
	status = H5Dclose ( dataset );

	// create datasets for each data type in schema

	data_type numtypes[10] = { TYPE_INT8, TYPE_UINT8, TYPE_INT16, TYPE_UINT16, TYPE_INT32, TYPE_UINT32, TYPE_INT64, TYPE_UINT64, TYPE_FLOAT32, TYPE_FLOAT64 };

	for ( size_t i = 0; i < 10; ++i ) {

		mpath = "/" + group_meta + "_" + data_type_to_string ( numtypes[i] );

		field_list cur = field_filter_type ( schm.fields(), numtypes[i] );

		if ( cur.size() > 0 ) {
			// create dataset

			hsize_t typedims[2];
			typedims[0] = cur.size();
			typedims[1] = nsamp;

			dataspace = H5Screate_simple ( 1, typedims, NULL ); 

			datatype = hdf5_data_type ( numtypes[i] );

			dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

			status = H5Sclose ( dataspace );
			status = H5Tclose ( datatype );
			status = H5Dclose ( dataset );

		}

	}
	
	status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < float > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < float > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < double > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < double > const & data ) {

	return;
}

