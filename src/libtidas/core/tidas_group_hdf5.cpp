/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>

#ifdef HAVE_HDF5
#include <tidas_group_hdf5.hpp>
#endif


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


string tidas::group_backend_hdf5::dict_meta () const {
    string meta = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( data_type::float64 );
    return meta;
}


string tidas::group_backend_hdf5::schema_meta () const {
    string meta = string("/") + schema_hdf5_dataset;
    return meta;
}


void tidas::group_backend_hdf5::read ( backend_path const & loc, index_type & nsamp, time_type & start, time_type & stop, map < data_type, size_t > & counts ) {

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

    herr_t status = H5open();

    hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
    H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
    hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, fapl );
    H5Pclose ( fapl );

    // read range

    string mpath = string("/") + group_hdf5_range_dataset;

    hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
    hid_t dataspace = H5Dget_space ( dataset );

    int ndims = H5Sget_simple_extent_ndims ( dataspace );

    if ( ndims != 1 ) {
        ostringstream o;
        o << "HDF5 group range dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW( o.str().c_str() );
    }

    hsize_t range_dims[1];
    hsize_t max_range_dims[1];
    int ret = H5Sget_simple_extent_dims ( dataspace, range_dims, max_range_dims );

    if ( range_dims[0] != 2 ) {
        ostringstream o;
        o << "HDF5 group range dataset " << fspath << ":" << mpath << " does not have 2 elements";
        TIDAS_THROW( o.str().c_str() );
    }

    hid_t datatype = hdf5_data_type ( data_type::float64 );

    double range[2];

    status = H5Dread ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, (void*)range );

    start = range[0];
    stop = range[1];

    status = H5Tclose ( datatype );
    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );

    // we always have the FLOAT64 dataset, since we always have at least the time field

    mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( data_type::float64 );

    dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
    dataspace = H5Dget_space ( dataset );

    ndims = H5Sget_simple_extent_ndims ( dataspace );

    if ( ndims != 2 ) {
        ostringstream o;
        o << "HDF5 group dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW( o.str().c_str() );
    }

    hsize_t dims[2];
    hsize_t maxdims[2];
    ret = H5Sget_simple_extent_dims ( dataspace, dims, maxdims );

    counts[ data_type::float64 ] = dims[0];
    nsamp = dims[1];

    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );

    // now iterate over all other types and find the number of fields

    vector < data_type > othertypes;
    othertypes.push_back ( data_type::int8 );
    othertypes.push_back ( data_type::uint8 );
    othertypes.push_back ( data_type::int16 );
    othertypes.push_back ( data_type::uint16 );
    othertypes.push_back ( data_type::int32 );
    othertypes.push_back ( data_type::uint32 );
    othertypes.push_back ( data_type::int64 );
    othertypes.push_back ( data_type::uint64 );
    othertypes.push_back ( data_type::float32 );
    othertypes.push_back ( data_type::float64 );
    othertypes.push_back ( data_type::string );

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

    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    status = H5close();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::group_backend_hdf5::write ( backend_path const & loc, index_type const & nsamp, map < data_type, size_t > const & counts ) const {

#ifdef HAVE_HDF5

    if ( loc.comp == compression_type::bzip2 ) {
        TIDAS_THROW( "HDF5 does not support bzip2 compression" );
    }

    string fspath = loc.path + path_sep + loc.name;

    // delete file if it exists

    int64_t fsize = fs_stat ( fspath.c_str() );
    if ( fsize > 0 ) {
        fs_rm ( fspath.c_str() );
    }

    // create file

    herr_t status = H5open();

    hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
    H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
    hid_t file = H5Fcreate ( fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, fapl );
    H5Pclose ( fapl );

    // create range dataset

    string mpath = string("/") + group_hdf5_range_dataset;
    hsize_t range_dims[1];

    range_dims[0] = 2;

    hid_t dataspace = H5Screate_simple ( 1, range_dims, NULL ); 

    hid_t datatype = hdf5_data_type ( data_type::float64 );

    hid_t dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );

    double range_data[2];
    range_data[0] = numeric_limits < time_type > :: max();
    range_data[1] = numeric_limits < time_type > :: min();

    status = H5Dwrite ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, range_data );

    H5Sclose ( dataspace );
    H5Tclose ( datatype );
    H5Dclose ( dataset );

    // determine dataset chunksize to use

    size_t chunksize;

    auto bpiter = loc.backparams.find ( key_hdf5_chunk );
    if ( bpiter != loc.backparams.end() ) {
        chunksize = (size_t) atol ( bpiter->second.c_str() );
    } else {
        chunksize = hdf5_chunk_default;
    }

    // create datasets

    hsize_t dims[2];
    hsize_t maxdims[2];

    for ( map < data_type, size_t > :: const_iterator it = counts.begin(); it != counts.end(); ++it ) {

        mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( it->first );

        dims[0] = it->second;
        dims[1] = nsamp;

        maxdims[0] = it->second;
        maxdims[1] = H5S_UNLIMITED;

        dataspace = H5Screate_simple ( 2, dims, maxdims ); 

        datatype = hdf5_data_type ( it->first );

        // we create the dataset using chunks, and optionally compression.

        

        hsize_t chunk_dims[2] = { 1, chunksize }; 

        hid_t dataprops = H5Pcreate ( H5P_DATASET_CREATE );
        H5Pset_chunk ( dataprops, 2, chunk_dims );

        if ( loc.comp == compression_type::gzip ) {
            status = H5Pset_deflate ( dataprops, 6 );
        }

        dataset = H5Dcreate ( file, mpath.c_str(), datatype, dataspace, H5P_DEFAULT, dataprops, H5P_DEFAULT );

        status = H5Sclose ( dataspace );
        status = H5Tclose ( datatype );
        status = H5Pclose ( dataprops );
        status = H5Dclose ( dataset );

    }
    
    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    status = H5close();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::group_backend_hdf5::resize ( backend_path const & loc, index_type const & nsamp ) {

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

    herr_t status = H5open();

    hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
    H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
    hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, fapl );
    H5Pclose ( fapl );

    // we always have the FLOAT64 dataset, since we always have at least the time field

    string mpath = string("/") + group_hdf5_dataset_prefix + string("_") + data_type_to_string( data_type::float64 );

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

    hsize_t newsize[2];
    newsize[0] = dims[0];
    newsize[1] = nsamp;

    status = H5Dset_extent ( dataset, newsize );

    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );

    // now iterate over all other types and find the number of fields

    vector < data_type > othertypes;
    othertypes.push_back ( data_type::int8 );
    othertypes.push_back ( data_type::uint8 );
    othertypes.push_back ( data_type::int16 );
    othertypes.push_back ( data_type::uint16 );
    othertypes.push_back ( data_type::int32 );
    othertypes.push_back ( data_type::uint32 );
    othertypes.push_back ( data_type::int64 );
    othertypes.push_back ( data_type::uint64 );
    othertypes.push_back ( data_type::float32 );
    othertypes.push_back ( data_type::float64 );
    othertypes.push_back ( data_type::string );

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

            newsize[0] = dims[0];
            newsize[1] = nsamp;

            status = H5Dset_extent ( dataset, newsize );

            status = H5Sclose ( dataspace );
            status = H5Dclose ( dataset );

        }

    }

    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    status = H5close();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::group_backend_hdf5::update_range ( backend_path const & loc, time_type const & start, time_type const & stop ) {

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

    herr_t status = H5open();

    hid_t fapl = H5Pcreate ( H5P_FILE_ACCESS );
    H5Pset_fclose_degree ( fapl, H5F_CLOSE_STRONG );
    hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, fapl );
    H5Pclose ( fapl );

    // open the range dataset

    string mpath = string("/") + group_hdf5_range_dataset;

    hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );
    hid_t dataspace = H5Dget_space ( dataset );

    int ndims = H5Sget_simple_extent_ndims ( dataspace );

    if ( ndims != 1 ) {
        ostringstream o;
        o << "HDF5 group range dataset " << fspath << ":" << mpath << " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW( o.str().c_str() );
    }

    hsize_t dims[1];
    hsize_t maxdims[1];
    int ret = H5Sget_simple_extent_dims ( dataspace, dims, maxdims );

    if ( dims[0] != 2 ) {
        ostringstream o;
        o << "HDF5 group range dataset " << fspath << ":" << mpath << " does not have 2 elements";
        TIDAS_THROW( o.str().c_str() );
    }

    hid_t datatype = hdf5_data_type ( data_type::float64 );

    double range[2];
    range[0] = start;
    range[1] = stop;

    status = H5Dwrite ( dataset, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, range );

    status = H5Tclose ( datatype );
    status = H5Sclose ( dataspace );
    status = H5Dclose ( dataset );
    status = H5Fflush ( file, H5F_SCOPE_GLOBAL );
    status = H5Fclose ( file );
    status = H5close();

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::group_backend_hdf5::link ( backend_path const & loc, link_type type, std::string const & path ) const {

#ifdef HAVE_HDF5

    string fspath = loc.path + path_sep + loc.name;

    fs_link ( fspath.c_str(), path.c_str(), ( type == link_type::hard ) );

#else

    TIDAS_THROW( "TIDAS not compiled with HDF5 support" );

#endif

    return;
}


void tidas::group_backend_hdf5::wipe ( backend_path const & loc ) const {

    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, float * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, float const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, double * data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, double const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, char ** data ) const {
    hdf5_helper_field_read ( loc, type_indx, offset, ndata, data );
    return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, char * const * data ) {
    hdf5_helper_field_write ( loc, type_indx, offset, ndata, data );
    return;
}




