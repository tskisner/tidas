/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#ifndef TIDAS_GROUP_HDF5_HPP
#define TIDAS_GROUP_HDF5_HPP


namespace tidas {

size_t hdf5_chunk();


void hdf5_helper_field_read_raw(backend_path const & loc,
                                size_t type_indx, index_type offset, index_type ndata,
                                data_type type, void * data);


void hdf5_helper_field_write_raw(backend_path const & loc,
                                 size_t type_indx, index_type offset, index_type ndata,
                                 data_type type, void const * data);


template <typename T>
void hdf5_helper_field_read(backend_path const & loc, size_t type_indx,
                            index_type offset, index_type ndata, T * data) {
    T testval;
    data_type type = data_type_get(typeid(testval));
    hdf5_helper_field_read_raw(loc, type_indx, offset, ndata, type,
                               (void *)data);
    return;
}

template <typename T>
void hdf5_helper_field_write(backend_path const & loc, size_t type_indx,
                             index_type offset, index_type ndata, T const * data) {
    T testval;
    data_type type = data_type_get(typeid(testval));
    hdf5_helper_field_write_raw(loc, type_indx, offset, ndata, type,
                                (void const *)data);
    return;
}

// Special helpers for strings.

void hdf5_helper_field_read_str(backend_path const & loc, size_t type_indx,
                                index_type offset, index_type ndata, char ** data);

void hdf5_helper_field_write_str(backend_path const & loc, size_t type_indx,
                                 index_type offset, index_type ndata,
                                 char * const * data);

}


#endif // ifndef TIDAS_GROUP_HDF5_HPP
