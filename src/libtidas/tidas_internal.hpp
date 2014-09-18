/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INTERNAL_HPP
#define TIDAS_INTERNAL_HPP

#include <config.h>
#include <tidas.hpp>

#include <tidas/re2/re2.h>


#ifdef HAVE_HDF5
extern "C" {
	#include <hdf5.h>
}
#endif

#ifdef HAVE_GETDATA
extern "C" {
	#include <getdata.h>
}
#endif


namespace tidas {


	static const size_t backend_string_size = 64;



#ifdef HAVE_HDF5

	hid_t hdf5_data_type ( data_type type );

	hid_t hdf5_dataset_create ( hid_t & file, std::string const & metapath, data_type type, size_t nfield, size_t nsamp );

#endif




}


#endif
