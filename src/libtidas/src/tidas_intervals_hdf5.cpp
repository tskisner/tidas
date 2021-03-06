
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>


using namespace std;
using namespace tidas;


tidas::intervals_backend_hdf5::intervals_backend_hdf5() : intervals_backend() {}

tidas::intervals_backend_hdf5::~intervals_backend_hdf5() {}

tidas::intervals_backend_hdf5::intervals_backend_hdf5(
    intervals_backend_hdf5 const & other) {}

intervals_backend_hdf5 & tidas::intervals_backend_hdf5::operator=(
    intervals_backend_hdf5 const & other) {
    if (this != &other) {}
    return *this;
}

string tidas::intervals_backend_hdf5::dict_meta() const {
    return intervals_hdf5_dataset_time;
}

void tidas::intervals_backend_hdf5::read(backend_path const & loc, size_t & size) {
#ifdef HAVE_HDF5

    // check if file exists

    string fspath = loc.path + path_sep + loc.name;

    int64_t fsize = fs_stat(fspath.c_str());
    if (fsize <= 0) {
        ostringstream o;
        o << "HDF5 intervals file " << fspath << " does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    // open file

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fopen(fspath.c_str(), H5F_ACC_RDONLY, fapl);
    H5Pclose(fapl);

    // read number of intervals

    string mpath = "/" + intervals_hdf5_dataset_time;

    hid_t dataset = H5Dopen(file, mpath.c_str(), H5P_DEFAULT);

    hid_t dataspace = H5Dget_space(dataset);
    hid_t datatype = H5Dget_type(dataset);

    int ndims = H5Sget_simple_extent_ndims(dataspace);

    if (ndims != 1) {
        ostringstream o;
        o << "HDF5 intervals dataset " << fspath << ":" << mpath <<
            " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW(o.str().c_str());
    }

    hsize_t time_dims;
    hsize_t maxdims;
    int ret = H5Sget_simple_extent_dims(dataspace, &time_dims, &maxdims);

    size = (size_t)(time_dims / 2);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);

    // clean up

    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}

void tidas::intervals_backend_hdf5::write(backend_path const & loc,
                                          size_t const & size) const {
#ifdef HAVE_HDF5

    string fspath = loc.path + path_sep + loc.name;

    // delete file if it exists

    int64_t fsize = fs_stat(fspath.c_str());
    if (fsize > 0) {
        fs_rm(fspath.c_str());
    }

    // create file

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fcreate(fspath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, fapl);
    H5Pclose(fapl);

    // create time dataset

    hsize_t dims[1];
    dims[0] = 2 * size;

    hid_t dataspace = H5Screate_simple(1, dims, NULL);

    hid_t datatype = H5Tcopy(H5T_NATIVE_DOUBLE);

    status = H5Tset_order(datatype, H5T_ORDER_LE);

    string mpath = "/" + intervals_hdf5_dataset_time;

    hid_t dataset = H5Dcreate(file,
                              mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT,
                              H5P_DEFAULT);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);

    // create index dataset

    dataspace = H5Screate_simple(1, dims, NULL);

    datatype = H5Tcopy(H5T_NATIVE_INT64);

    status = H5Tset_order(datatype, H5T_ORDER_LE);

    mpath = "/" + intervals_hdf5_dataset_index;

    dataset = H5Dcreate(file,
                        mpath.c_str(), datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT,
                        H5P_DEFAULT);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);

    // clean up

    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

    // mark volume as dirty

    // if ( loc.vol == NULL ) {
    //    TIDAS_THROW( "volume handle is NULL, this should never happen!" );
    // }
    // loc.vol->set_dirty();

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}

void tidas::intervals_backend_hdf5::link(backend_path const & loc, link_type type,
                                         std::string const & path) const {
#ifdef HAVE_HDF5

    string fspath = loc.path + path_sep + loc.name;

    fs_link(fspath.c_str(), path.c_str(), (type == link_type::hard));

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}

void tidas::intervals_backend_hdf5::wipe(backend_path const & loc) const {
#ifdef HAVE_HDF5

    string fspath = loc.path + path_sep + loc.name;

    // delete file if it exists

    int64_t fsize = fs_stat(fspath.c_str());
    if (fsize > 0) {
        fs_rm(fspath.c_str());
    }

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}

void tidas::intervals_backend_hdf5::read_data(backend_path const & loc,
                                              interval_list & intr) const {
#ifdef HAVE_HDF5

    // check if file exists

    string fspath = loc.path + path_sep + loc.name;

    int64_t fsize = fs_stat(fspath.c_str());
    if (fsize <= 0) {
        ostringstream o;
        o << "HDF5 intervals file " << fspath << " does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    // clear out intervals

    intr.clear();

    // open file

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fopen(fspath.c_str(), H5F_ACC_RDONLY, fapl);
    H5Pclose(fapl);

    // read times

    string mpath = "/" + intervals_hdf5_dataset_time;

    hid_t dataset = H5Dopen(file, mpath.c_str(), H5P_DEFAULT);

    hid_t dataspace = H5Dget_space(dataset);
    hid_t datatype = H5Dget_type(dataset);

    int ndims = H5Sget_simple_extent_ndims(dataspace);

    if (ndims != 1) {
        ostringstream o;
        o << "HDF5 intervals dataset " << fspath << ":" << mpath <<
            " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW(o.str().c_str());
    }

    hsize_t time_dims;
    hsize_t maxdims;
    int ret = H5Sget_simple_extent_dims(dataspace, &time_dims, &maxdims);

    double * time_buffer = mem_alloc <double> ((size_t)time_dims);

    status = H5Dread(dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                     (void *)time_buffer);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);

    // read indices

    mpath = "/" + intervals_hdf5_dataset_index;

    dataset = H5Dopen(file, mpath.c_str(), H5P_DEFAULT);

    dataspace = H5Dget_space(dataset);
    datatype = H5Dget_type(dataset);

    ndims = H5Sget_simple_extent_ndims(dataspace);

    if (ndims != 1) {
        ostringstream o;
        o << "HDF5 intervals dataset " << fspath << ":" << mpath <<
            " has wrong dimensions (" << ndims << ")";
        TIDAS_THROW(o.str().c_str());
    }

    hsize_t ind_dims;
    ret = H5Sget_simple_extent_dims(dataspace, &ind_dims, &maxdims);

    if (ind_dims != time_dims) {
        ostringstream o;
        o << "HDF5 intervals dataset " << fspath << ": index length (" << ind_dims <<
            ") is different than time length (" << time_dims << ")";
        TIDAS_THROW(o.str().c_str());
    }

    int64_t * ind_buffer = mem_alloc <int64_t> ((size_t)ind_dims);

    status = H5Dread(dataset, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                     (void *)ind_buffer);

    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);

    // close file

    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

    // copy buffers into intervals

    size_t n = (size_t)(ind_dims / 2);

    for (size_t i = 0; i < n; ++i) {
        intr.push_back(intrvl(time_buffer[2 * i], time_buffer[2 * i + 1],
                              ind_buffer[2 * i], ind_buffer[2 * i + 1]));
    }

    free(ind_buffer);
    free(time_buffer);

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}

void tidas::intervals_backend_hdf5::write_data(backend_path const & loc,
                                               interval_list const & intr) {
#ifdef HAVE_HDF5

    // open file in write mode

    string fspath = loc.path + path_sep + loc.name;

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fopen(fspath.c_str(), H5F_ACC_RDWR, fapl);
    H5Pclose(fapl);

    // write time data

    string mpath = "/" + intervals_hdf5_dataset_time;

    hid_t dataset = H5Dopen(file, mpath.c_str(), H5P_DEFAULT);

    double * time_buffer = mem_alloc <double> (2 * intr.size());

    size_t i = 0;
    for (interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it) {
        time_buffer[i] = it->start;
        ++i;
        time_buffer[i] = it->stop;
        ++i;
    }

    status = H5Dwrite(dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                      (void *)time_buffer);

    free(time_buffer);

    H5Dclose(dataset);

    // write index data

    mpath = "/" + intervals_hdf5_dataset_index;

    dataset = H5Dopen(file, mpath.c_str(), H5P_DEFAULT);

    int64_t * ind_buffer = mem_alloc <int64_t> (2 * intr.size());

    i = 0;
    for (interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it) {
        ind_buffer[i] = it->first;
        ++i;
        ind_buffer[i] = it->last;
        ++i;
    }

    status = H5Dwrite(dataset, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                      (void *)ind_buffer);

    free(ind_buffer);

    H5Dclose(dataset);

    // close file

    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

    // mark volume as dirty

    // if ( loc.vol == NULL ) {
    //    TIDAS_THROW( "volume handle is NULL, this should never happen!" );
    // }
    // loc.vol->set_dirty();

#else // ifdef HAVE_HDF5

    TIDAS_THROW("TIDAS not compiled with HDF5 support");

#endif // ifdef HAVE_HDF5

    return;
}
