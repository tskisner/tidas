/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>

#include <cstring>


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


void tidas::schema_backend_hdf5::read ( backend_path const & loc, field_list & fields ) {

#ifdef HAVE_HDF5

    fields.clear();

    // open file in read mode

    string fspath = loc.path + path_sep + loc.name;

    herr_t status = H5open();

    hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
    H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
    hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, fapl );
    H5Pclose ( fapl );

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

    status = H5Dread ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

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

        fields.push_back ( cur );
    }

    free ( buffer );

    status = H5Tclose ( datatype );
    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );
    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    //status = H5close();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::schema_backend_hdf5::write ( backend_path const & loc, field_list const & fields ) const {

#ifdef HAVE_HDF5

    // open file in write mode or create if it does not exist

    string fspath = loc.path + path_sep + loc.name;

    int64_t fsize = fs_stat ( fspath.c_str() );

    herr_t status = H5open();

    hid_t file;
    if ( fsize > 0 ) {
        hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
        H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
        file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, fapl );
        H5Pclose ( fapl );

        // check if dataset exists and delete it

        htri_t check = H5Lexists ( file, loc.meta.c_str(), H5P_DEFAULT );
        if ( check ) {
            herr_t ulink = H5Ldelete ( file, loc.meta.c_str(), H5P_DEFAULT );
        }

    } else {
        hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
        H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
        file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, fapl );
        H5Pclose ( fapl );
    }

    // create schema dataset and write

    hsize_t dims[2];
    dims[0] = fields.size();
    dims[1] = 3;

    hid_t dataspace = H5Screate_simple ( 2, dims, NULL ); 

    hid_t datatype = hdf5_data_type ( data_type::string );

    hid_t dataset = H5Dcreate ( file, loc.meta.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

    char * buffer = mem_alloc < char > ( 3 * fields.size() * backend_string_size );

    size_t off = 0;    

    for ( field_list :: const_iterator it = fields.begin(); it != fields.end(); ++it ) {

        strncpy ( &(buffer[ off ]), it->name.c_str(), backend_string_size );
        off += backend_string_size;
        
        strncpy ( &(buffer[ off ]), it->units.c_str(), backend_string_size );
        off += backend_string_size;

        string datastring = data_type_to_string ( it->type );
        strncpy ( &(buffer[ off ]), datastring.c_str(), backend_string_size );
        off += backend_string_size;

    }

    status = H5Dwrite ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

    free ( buffer );

    status = H5Tclose ( datatype );
    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );
    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    //status = H5close();

    // mark volume as dirty

    //if ( loc.vol == NULL ) {
    //    TIDAS_THROW( "volume handle is NULL, this should never happen!" );
    //}
    //loc.vol->set_dirty();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}



