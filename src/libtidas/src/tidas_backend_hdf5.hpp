/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_HDF5_HPP
#define TIDAS_HDF5_HPP


#ifdef HAVE_HDF5
extern "C" {
    #include <hdf5.h>
}
#endif


namespace tidas {

    static const std::string dict_hdf5_type_suffix = "_TIDASTYPE";

    static const std::string schema_hdf5_dataset = "schema";

    static const std::string intervals_hdf5_dataset_time = "times";
    static const std::string intervals_hdf5_dataset_index = "indices";

    static const std::string group_hdf5_dataset_prefix = "data";
    static const std::string group_hdf5_range_dataset = "range";

    static const std::string extension_hdf5_dataset_prefix = "data";

    static const std::string key_hdf5_chunk = "CHUNK";
    static const size_t hdf5_chunk_default = 256;

#ifdef HAVE_HDF5

    hid_t hdf5_data_type ( data_type type );


#endif

}


#endif
