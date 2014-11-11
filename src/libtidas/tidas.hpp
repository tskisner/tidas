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
#include <memory>

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
		TYPE_FLOAT64,
		TYPE_STRING
	} data_type;

	data_type data_type_get ( std::type_info const & test );

	std::string data_type_to_string ( data_type type );

	data_type data_type_from_string ( std::string const & name );

	typedef enum {
		BACKEND_MEM,
		BACKEND_HDF5,
		BACKEND_GETDATA
	} backend_type;

	typedef enum {
		COMPRESS_NONE,
		COMPRESS_GZIP,
		COMPRESS_BZIP2
	} compression_type;

	typedef enum {
		MODE_R,
		MODE_RW
	} access_mode;


	class volume;

	class backend_path {
		
		public :

			backend_path ();

			bool operator== ( const backend_path & other ) const;
			bool operator!= ( const backend_path & other ) const;

			/// Backend type.
			backend_type type;

			/// Compression used.
			compression_type comp;

			/// Access mode.
			access_mode mode;

			/// Parent directory.
			std::string path;

			/// Name of on-disk object (backend-specific).
			std::string name;

			/// Metadata path inside object (backend-specific).
			std::string meta;

			//volume * vol;

	};


	// string constants used for matching and by objects when 
	// writing to filesystem and backend formats.

	static const std::string path_sep = "/";
	static const std::string submatch_begin = "[";
	static const std::string submatch_end = "]";
	static const std::string submatch_sep = ",";
	static const std::string submatch_assign = "=";

	static const std::string dict_hdf5_type_suffix = "_TIDASTYPE";
	static const std::string dict_submatch_key = "dict";

	static const std::string schema_hdf5_dataset = "schema";
	static const std::string schema_getdata_dir = "schema";
	static const std::string schema_submatch_key = "schm";

	static const std::string intervals_hdf5_dataset_time = "times";
	static const std::string intervals_hdf5_dataset_index = "indices";
	static const std::string intervals_getdata_field_start = "start";
	static const std::string intervals_getdata_field_stop = "stop";
	static const std::string intervals_getdata_field_first = "first";
	static const std::string intervals_getdata_field_last = "last";
	static const std::string intervals_submatch_key = "intr";

	static const std::string group_time_field = "TIDAS_TIME";
	static const std::string group_hdf5_dataset_prefix = "data";
	static const std::string group_submatch_key = "grp";

	static const std::string extension_hdf5_dataset_prefix = "data";
	static const std::string extension_submatch_key = "ext";

	static const std::string block_fs_group_dir = "_groups";
	static const std::string block_fs_intervals_dir = "_intervals";
	static const std::string block_fs_ext_dir = "_extensions";


}

#include <tidas/utils.hpp>
#include <tidas/dict.hpp>
#include <tidas/intervals.hpp>
#include <tidas/schema.hpp>
#include <tidas/group.hpp>
#include <tidas/block.hpp>
//#include <tidas/volume.hpp>
#include <tidas/copy.hpp>

#endif
