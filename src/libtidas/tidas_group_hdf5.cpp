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


group_backend_hdf5 * tidas::group_backend_hdf5::clone () {
	group_backend_hdf5 * ret = new group_backend_hdf5 ( *this );
	return ret;
}




void tidas_group_hdf5_lookup ( schema const & schm, string const & name, string & metapath, size_t & row ) {

	field_list fields = schm.fields();

	for ( size_t i = 0; i < fields.size(); ++i ) {


	return;
}


void tidas::group_backend_hdf5::read ( backend_path const & loc, schema & schm, dict & dictionary, index_type & nsamp ) {

#ifdef HAVE_HDF5

	// check if file exists

	string fspath = loc.path + "/" + loc.name;

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize <= 0 ) {
		std::ostringstream o;
		o << "HDF5 intervals file " << fspath << " does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	// open file

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// read schema

	string mpath = "/schema";

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

	// read dictionary

	mpath = "/data";

	dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

	dictionary.clear();

	hsize_t aindex = 0;
	status = H5Aiterate2 ( dataset, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, &aindex, tidas_group_hdf5_attr_parse, (void *) &dictionary );

	H5Dclose ( dataset );

	// clean up

	herr_t status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::write ( backend_path const & loc, schema const & schm, dict const & dictionary, index_type nsamp ) {

#ifdef HAVE_HDF5

	string fspath = loc.path + "/" + loc.name;

	// delete file if it exists

	int64_t fsize = fs_stat ( fspath.c_str() );
	if ( fsize > 0 ) {
		fs_rm ( fspath.c_str() );
	}

	// create file

	hid_t file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );

	// write schema

	field_list fields = schm.fields();

	hsize_t dims[2];
	dims[0] = fields.size();
	dims[1] = 3;

	hid_t dataspace = H5Screate_simple ( 1, dims, NULL ); 

	hid_t datatype = H5Tcopy ( H5T_C_S1 );

	herr_t status = H5Tset_size ( datatype, backend_string_size );

	string mpath = "/schema";

	hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	char * buffer = mem_alloc < char > ( dims[0] * dims[1] * backend_string_size );

	for ( size_t i = 0; i < fields.size(); ++i ) {

		size_t off = 3 * i * backend_string_size;
		
		strncpy ( &(buffer[ off ]), fields[i].name, backend_string_size );
		off += backend_string_size;
		
		strncpy ( &(buffer[ off ]), fields[i].name, backend_string_size );
		off += backend_string_size;

		switch ( fields[i].type ) {
			case TYPE_INT8:
				strncpy ( &(buffer[ off ]), "TYPE_INT8", backend_string_size );
				break;
			case TYPE_UINT8:
				strncpy ( &(buffer[ off ]), "TYPE_UINT8", backend_string_size );
				break;
			case TYPE_INT16:
				strncpy ( &(buffer[ off ]), "TYPE_INT16", backend_string_size );
				break;
			case TYPE_UINT16:
				strncpy ( &(buffer[ off ]), "TYPE_UINT16", backend_string_size );
				break;
			case TYPE_INT32:
				strncpy ( &(buffer[ off ]), "TYPE_INT32", backend_string_size );
				break;
			case TYPE_UINT32:
				strncpy ( &(buffer[ off ]), "TYPE_UINT32", backend_string_size );
				break;
			case TYPE_INT64:
				strncpy ( &(buffer[ off ]), "TYPE_INT64", backend_string_size );
				break;
			case TYPE_UINT64:
				strncpy ( &(buffer[ off ]), "TYPE_UINT64", backend_string_size );
				break;
			case TYPE_FLOAT32:
				strncpy ( &(buffer[ off ]), "TYPE_FLOAT32", backend_string_size );
				break;
			case TYPE_FLOAT64:
				strncpy ( &(buffer[ off ]), "TYPE_FLOAT64", backend_string_size );
				break;
			default:
				TIDAS_THROW( "data type not recognized" );
				break;
		}

	}

	status = H5Dwrite ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	free ( buffer );

	H5Dclose ( dataset );
	H5Sclose ( dataspace );
	H5Tclose ( datatype );

	// create 
	
	// write dictionary


	// clean up

	herr_t status = H5Fclose ( file ); 

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data ) {

	return;
}

