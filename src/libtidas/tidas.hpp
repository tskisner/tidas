/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_HPP
#define TIDAS_HPP

#include <iostream>
#include <string>
#include <sstream>

#include <cstdint>



typedef enum {
	TIDAS_TYPE_NONE,
	TIDAS_TYPE_INT8,
	TIDAS_TYPE_UINT8,
	TIDAS_TYPE_INT16,
	TIDAS_TYPE_UINT16,
	TIDAS_TYPE_INT32,
	TIDAS_TYPE_UINT32,
	TIDAS_TYPE_INT64,
	TIDAS_TYPE_UINT64,
	TIDAS_TYPE_FLOAT32,
	TIDAS_TYPE_FLOAT64
} tidas_type;


typedef enum {
	TIDAS_BACKEND_MEM,
	TIDAS_BACKEND_HDF5,
	TIDAS_BACKEND_GETDATA
} tidas_backend;


#define TIDAS_DTYPE_TIME double




#include <tidas/utils.hpp>
//#include <tidas/intervals.hpp>
//#include <tidas/schema.hpp>
//#include <tidas/group.hpp>
//#include <tidas/block.hpp>
//#include <tidas/volume.hpp>


#endif
