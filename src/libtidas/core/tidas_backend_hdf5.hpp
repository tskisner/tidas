/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
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

#ifdef HAVE_HDF5

    hid_t hdf5_data_type ( data_type type );


#endif

}


#endif
