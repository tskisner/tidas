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
#include <vector>
#include <utility>
#include <map>
#include <typeinfo>

#include <cstdint>


namespace tidas {

	typedef double time_type;

	typedef int64_t index_type;

	typedef enum {
		TYPE_NONE,
		TYPE_INT8,
		TYPE_UINT8,
		TYPE_INT16,
		TYPE_UINT16,
		TYPE_INT32,
		TYPE_UINT32,
		TYPE_INT64,
		TYPE_UINT64,
		TYPE_FLOAT32,
		TYPE_FLOAT64
	} data_type;

	data_type data_type_get ( std::type_info const & test );

	typedef enum {
		BACKEND_MEM,
		BACKEND_HDF5,
		BACKEND_GETDATA
	} backend_type;


	class backend_path {
		
		public :

			backend_path ();

			backend_type type;
			std::string path;
			std::string name;

	};

	static const size_t backend_string_size = 64;

}

#include <tidas/utils.hpp>
#include <tidas/dict.hpp>
#include <tidas/intervals.hpp>
#include <tidas/schema.hpp>
#include <tidas/group.hpp>
#include <tidas/block.hpp>
#include <tidas/volume.hpp>


#endif
