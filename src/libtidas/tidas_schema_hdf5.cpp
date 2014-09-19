/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;



tidas::schema_backend_hdf5::schema_backend_hdf5 () {

}


tidas::schema_backend_hdf5::~schema_backend_hdf5 () {

}


tidas::schema_backend_hdf5::schema_backend_hdf5 ( schema_backend_hdf5 const & other ) {

}


schema_backend_hdf5 & tidas::schema_backend_hdf5::operator= ( schema_backend_hdf5 const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


schema_backend * tidas::schema_backend_hdf5::clone () {
	schema_backend_hdf5 * ret = new schema_backend_hdf5 ( *this );
	return ret;
}


void tidas::schema_backend_hdf5::read_meta ( backend_path const & loc, string const & filter, field_list & fields ) {

#ifdef HAVE_HDF5

	fields.clear();

	// open file in read mode

	string fspath = loc.path + "/" + loc.name;

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// open dataset

	hid_t dataset = H5Dopen ( file, loc.meta.c_str(), H5P_DEFAULT );

	hid_t dataspace = H5Dget_space ( dataset );
	
	hid_t datatype = H5Dget_type ( dataset );

	int ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 2 ) {
		ostringstream o;
		o << "HDF5 schema dataset " << fspath << ":" << loc.meta << " has wrong dimensions (" << ndims << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	hsize_t dims[2];
	
	hsize_t maxdims[2];

	int ret = H5Sget_simple_extent_dims ( dataspace, dims, maxdims );

	if ( dims[1] != 3 ) {
		ostringstream o;
		o << "HDF5 schema dataset " << fspath << ":" << loc.meta << " has wrong second dimension (" << dims[1] << " != " << 3 << ")";
		TIDAS_THROW( o.str().c_str() );
	}

	// read data and repack

	char * buffer = mem_alloc < char > ( dims[0] * dims[1] * backend_string_size );

	herr_t status = H5Dread ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	RE2 re ( filter );

	size_t off = 0;

	field cur;
	char temp[ backend_string_size ];

	for ( size_t i = 0; i < dims[0]; ++i ) {

		strncpy ( temp, &(buffer[ off ]), backend_string_size );
		off += backend_string_size;
		cur.name = temp;

		strncpy ( temp, &(buffer[ off ]), backend_string_size );
		off += backend_string_size;
		cur.units = temp;

		strncpy ( temp, &(buffer[ off ]), backend_string_size );
		off += backend_string_size;
		cur.type = data_type_from_string ( string(temp) );

		if ( RE2::FullMatch ( cur.name, re ) ) {
			fields.push_back ( cur );
		}
	}

	free ( buffer );

	status = H5Tclose ( datatype );
	status = H5Sclose ( dataspace );
	status = H5Dclose ( dataset );
	status = H5Fclose ( file );

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}


void tidas::schema_backend_hdf5::write_meta ( backend_path const & loc, string const & filter, field_list const & fields ) {

#ifdef HAVE_HDF5

	// first construct filtered copy

	field_list sel;

	RE2 re ( filter );

	for ( field_list :: const_iterator it = fields.begin(); it != fields.end(); ++it ) {
		if ( RE2::FullMatch ( it->name, re ) ) {
			sel.push_back( *it );
		}
	}

	// open file in write mode

	string fspath = loc.path + "/" + loc.name;

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

	// create schema dataset and write

	hsize_t dims[2];
	dims[0] = sel.size();
	dims[1] = 3;

	hid_t dataspace = H5Screate_simple ( 1, dims, NULL ); 

	hid_t datatype = hdf5_data_type ( TYPE_STRING );

	hid_t dataset = H5Dcreate ( file, loc.meta.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

	char * buffer = mem_alloc < char > ( 3 * sel.size() * backend_string_size );

	size_t off = 0;	

	for ( field_list :: const_iterator it = sel.begin(); it != sel.end(); ++it ) {

		strncpy ( &(buffer[ off ]), it->name.c_str(), backend_string_size );
		off += backend_string_size;
		
		strncpy ( &(buffer[ off ]), it->units.c_str(), backend_string_size );
		off += backend_string_size;

		string datastring = data_type_to_string ( it->type );
		strncpy ( &(buffer[ off ]), datastring.c_str(), backend_string_size );

	}

	herr_t status = H5Dwrite ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	free ( buffer );

	status = H5Tclose ( datatype );
	status = H5Sclose ( dataspace );
	status = H5Dclose ( dataset );
	status = H5Fclose ( file );

	// mark volume as dirty

	//if ( loc.vol == NULL ) {
	//	TIDAS_THROW( "volume handle is NULL, this should never happen!" );
	//}
	//loc.vol->set_dirty();

#else

	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

	return;
}



