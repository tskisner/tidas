/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_BACKEND_HDF5_HPP
#define TIDAS_BACKEND_HDF5_HPP


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
