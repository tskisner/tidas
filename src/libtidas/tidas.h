/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_H
#define TIDAS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


#define TIDAS_PATH_LEN 512
#define TIDAS_NAME_LEN 64
#define TIDAS_ERR_LEN 32


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





#include <tidas/utils.h>
#include <tidas/intervals.h>
#include <tidas/schema.h>
/*
#include <tidas/group.h>
#include <tidas/block.h>
#include <tidas/volume.h>
*/

#endif
