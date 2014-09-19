/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


static const char * tidas_dict_hdf5_type_suffix = "_TIDASTYPE";



tidas::dict_backend_hdf5::dict_backend_hdf5 () {

}


tidas::dict_backend_hdf5::~dict_backend_hdf5 () {

}


tidas::dict_backend_hdf5::dict_backend_hdf5 ( dict_backend_hdf5 const & other ) {

}


dict_backend_hdf5 & tidas::dict_backend_hdf5::operator= ( dict_backend_hdf5 const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


dict_backend * tidas::dict_backend_hdf5::clone () {
	dict_backend_hdf5 * ret = new dict_backend_hdf5 ( *this );

	return ret;
}


#ifdef HAVE_HDF5

typedef struct {
	map < string, string > data;
	map < string, data_type > types;
} tidas_dict_backend_hdf5_attr_data;


herr_t tidas_dict_backend_hdf5_attr_parse ( hid_t location_id, const char * attr_name, const H5A_info_t * ainfo, void * op_data ) {

	tidas_dict_backend_hdf5_attr_data * d = static_cast < tidas_dict_backend_hdf5_attr_data * > ( op_data );

	string key = attr_name;

	herr_t status = 0;

	hid_t hattr = H5Aopen ( location_id, attr_name, H5P_DEFAULT );

	H5A_info_t info;
	status = H5Aget_info( hattr, &info );

	char buf[ info.data_size + 2 ];

	status = H5Aread ( hattr, H5T_C_S1, (void *)buf );

	status = H5Aclose ( hattr );

	string val = buf;

	string match = string ( "(.*)" ) + tidas_dict_hdf5_type_suffix + string ( "$" );
	RE2 re ( match );

	string name;
	if ( RE2::FullMatch ( key, re, &name ) ) {
		d->types[ name ] = data_type_from_string ( val );
	} else {
		d->data[ key ] = val;
	}

	return status;
}


void tidas::dict_backend_hdf5::read_meta ( backend_path const & loc, string const & filter, map < string, string > & data, map < string, data_type > & types ) {

	// open file in write mode

	string fspath = loc.path + "/" + loc.name;

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

	// open dataset

	hid_t dataset = H5Dopen ( file, loc.meta.c_str(), H5P_DEFAULT );

	// iterate over attributes and populate data and types

	tidas_dict_backend_hdf5_attr_data iterdata;

	hsize_t aoff = 0;
	herr_t status = H5Aiterate ( dataset, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, &aoff, tidas_dict_backend_hdf5_attr_parse, (void *)&iterdata );

	// filter out only selected keys

	data.clear();
	types.clear();

	RE2 re ( filter );

	for ( map < string, string > :: const_iterator it = iterdata.data.begin(); it != iterdata.data.end(); ++it ) {

		if ( RE2::FullMatch ( it->first, re ) ) {

			data[ it->first ] = it->second;
			types[ it->first ] = iterdata.types[ it->first ];

		}
	}

	// cleanup

	status = H5Dclose ( dataset );
	status = H5Fclose ( file ); 

	return;
}


void tidas::dict_backend_hdf5::write_meta ( backend_path const & loc, string const & filter, map < string, string > const & data, map < string, data_type > const & types ) {

	// open file in write mode

	string fspath = loc.path + "/" + loc.name;

	hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

	// open dataset

	hid_t dataset = H5Dopen ( file, loc.meta.c_str(), H5P_DEFAULT );

	// get number of existing attributes

	H5O_info_t dataset_info;
	herr_t status = H5Oget_info ( dataset, &dataset_info );

	// clear all attributes.  we just repeatedly delete the first object.

	for ( size_t i = 0; i < dataset_info.num_attrs; ++i ) {
		status = H5Adelete_by_idx ( dataset, ".", H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, 0, H5P_DEFAULT );
	}

	// create attributes for each dictionary item

	hid_t datatype = H5Tcopy ( H5T_C_S1 );

	RE2 re ( filter );

	for ( map < string, string > :: const_iterator it = data.begin(); it != data.end(); ++it ) {

		string key = it->first;
		string val = it->second;

		if ( RE2::FullMatch ( key, re ) ) {

			string key_type = it->first + tidas_dict_hdf5_type_suffix;
			string val_type = data_type_to_string ( types.at( it->first ) );

			status = H5Tset_size ( datatype, val.size() );
			hid_t attr = H5Acreate ( dataset, key.c_str(), datatype, H5S_SCALAR, H5P_DEFAULT, H5P_DEFAULT );
			status = H5Awrite ( attr, datatype, (void *) val.c_str() );
			status = H5Aclose ( attr );

			status = H5Tset_size ( datatype, val_type.size() );
			attr = H5Acreate ( dataset, key_type.c_str(), datatype, H5S_SCALAR, H5P_DEFAULT, H5P_DEFAULT );
			status = H5Awrite ( attr, datatype, (void *) val_type.c_str() );
			status = H5Aclose ( attr );

		}

	}

	// cleanup

	status = H5Tclose ( datatype );
	status = H5Dclose ( dataset );
	status = H5Fclose ( file );

	// mark volume as dirty

	//if ( loc.vol == NULL ) {
	//	TIDAS_THROW( "volume handle is NULL, this should never happen!" );
	//}
	//loc.vol->set_dirty();

	return;
}

#else


void tidas::dict_backend_hdf5::read_meta ( backend_path const & loc, string const & filter, map < string, string > & data, map < string, data_type > & types ) {
	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );
	return;
}


void tidas::dict_backend_hdf5::write_meta ( backend_path const & loc, string const & filter, map < string, string > const & data, map < string, data_type > const & types ) {
	TIDAS_THROW( "TIDAS not compiled with HDF5 support" );
	return;
}

#endif
