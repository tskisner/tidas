/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#ifdef HAVE_HDF5
#include <tidas_group_hdf5.hpp>
#endif


using namespace std;
using namespace tidas;



/*
NOTES:

- enable chunking per field and with some number of samples
- set hash table size to number of chunks
- dataset should be extensible in time direction
- chunk cache size based on few times sample buffer * sizeof(double)




*/


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


string tidas::group_backend_hdf5::dict_meta () const {
	string meta = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( TYPE_FLOAT64 );
	return meta;
}


string tidas::group_backend_hdf5::schema_meta () const {
	string meta = string("/") + schema_hdf5_dataset;
	return meta;
}


void tidas::group_backend_hdf5::read ( backend_path const & loc, index_type & nsamp, map < data_type, size_t > & counts ) {

#ifdef HAVE_HDF5

	// check if file exists

	string fspath = loc.path + path_sep + loc.name;

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize <= 0 ) {
		ostringstream o;
		o << "HDF5 group file " << fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// open file

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// we always have the FLOAT64 dataset, since we always have at least the time field

	string mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( TYPE_FLOAT64 );

	hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
	hid_t dataspace = H5Dget_space ( dataset );

	int ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 2 ) {
		ostringstream o;
		o << "HDF5 group dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t dims[2];
	hsize_t maxdims[2];
	int ret = H5Sget_simple_extent_dims ( dataspace, dims, maxdims );

	counts[ TYPE_FLOAT64 ] = dims[0];
	nsamp = dims[1];

	herr_t status = H5Sclose ( dataspace );
	status = H5Dclose ( dataset );

	// now iterate over all other types and find the number of fields

	vector < data_type > othertypes;
	othertypes.push_back ( TYPE_INT8 );
	othertypes.push_back ( TYPE_UINT8 );
	othertypes.push_back ( TYPE_INT16 );
	othertypes.push_back ( TYPE_UINT16 );
	othertypes.push_back ( TYPE_INT32 );
	othertypes.push_back ( TYPE_UINT32 );
	othertypes.push_back ( TYPE_INT64 );
	othertypes.push_back ( TYPE_UINT64 );
	othertypes.push_back ( TYPE_FLOAT32 );
	othertypes.push_back ( TYPE_FLOAT64 );
	othertypes.push_back ( TYPE_STRING );

	for ( vector < data_type > :: const_iterator it = othertypes.begin(); it != othertypes.end(); ++it ) {

		mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( (*it) );

		// check if dataset exists

		htri_t check = H5Lexists ( file, mpath.c_str(), H5P_DEFAULT );

		if ( check ) {

			dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
			dataspace = H5Dget_space ( dataset );

			ndims = H5Sget_simple_extent_ndims ( dataspace );

			if ( ndims != 2 ) {
				ostringstream o;
				o << "HDF5 group dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
				TIDAS_THROW( o.str().c_str() );
			}

			ret = H5Sget_simple_extent_dims ( dataspace, dims, maxdims );

			counts[ (*it) ] = dims[0];

			index_type nsamp_check = dims[1];
			if ( nsamp_check != nsamp ) {
				ostringstream o;
				o << "HDF5 group dataset " << fspath << ":" << mpath << " has a different number of samples than the time (" << nsamp_check << " != " << nsamp << ")";
				TIDAS_THROW( o.str().c_str() );
			}

			status = H5Sclose ( dataspace );
			status = H5Dclose ( dataset );

		} else {
			counts[ (*it) ] = 0;
		}

	}

	status = H5Fclose ( file );

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::write ( backend_path const & loc, index_type const & nsamp, map < data_type, size_t > const & counts ) const {

#ifdef HAVE_HDF5

	string fspath = loc.path + path_sep + loc.name;

	// delete file if it exists

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize > 0 ) {
		fs_rm ( fspath.c_str() );
	}

	// create file

	hid_t file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );

	// create datasets

	hsize_t dims[2];

	herr_t status = 0;

	for ( map < data_type, size_t > :: const_iterator it = counts.begin(); it != counts.end(); ++it ) {

		string mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( it->first );

		dims[0] = it->second;
		dims[1] = nsamp;

		hid_t dataspace = H5Screate_simple ( 2, dims, NULL ); 

		hid_t datatype = hdf5_data_type ( it->first );

		hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

		status = H5Sclose ( dataspace );
		status = H5Tclose ( datatype );
		status = H5Dclose ( dataset );

	}
	
	status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::link ( backend_path const & loc, link_type type, std::string const & path, std::string const & name ) const {

	return;
}


void tidas::group_backend_hdf5::wipe ( backend_path const & loc ) const {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > & data ) const {
	hdf5_helper_field_read ( loc, type_indx, offset, data );
	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > const & data ) {
	hdf5_helper_field_write ( loc, type_indx, offset, data );
	return;
}




