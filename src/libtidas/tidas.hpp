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
#include <map>
#include <typeinfo>
#include <memory>

#include <cstdint>


namespace tidas {

	typedef double time_type;

	typedef int64_t index_type;

	typedef enum {
		TYPE_NONE, ///< Undefined type.
		TYPE_INT8, ///< C compatible int8_t type.
		TYPE_UINT8, ///< C compatible uint8_t type.
		TYPE_INT16, ///< C compatible int16_t type.
		TYPE_UINT16, ///< C compatible uint16_t type.
		TYPE_INT32, ///< C compatible int32_t type.
		TYPE_UINT32, ///< C compatible uint32_t type.
		TYPE_INT64, ///< C compatible int64_t type.
		TYPE_UINT64, ///< C compatible uint64_t type.
		TYPE_FLOAT32, ///< float type.
		TYPE_FLOAT64, ///< double type.
		TYPE_STRING ///< C++ string type.
	} data_type;

	typedef enum {
		BACKEND_NONE, ///< Backend not assigned.
		BACKEND_HDF5, ///< HDF5 backend.
		BACKEND_GETDATA ///< GetData backend.
	} backend_type;

	typedef enum {
		COMPRESS_NONE, ///< No compression.
		COMPRESS_GZIP, ///< gzip (libz) compression.
		COMPRESS_BZIP2 ///< bzip2 compression.
	} compression_type;

	typedef enum {
		MODE_R, ///< Read-only access.
		MODE_RW ///< Read-write access.
	} access_mode;

	typedef enum {
		LINK_NONE, ///< Not a link.
		LINK_HARD, ///< Hard Link.
		LINK_SOFT   ///< Symlink.
	} link_type;

	typedef enum {
		EXEC_DEPTH_FIRST, ///< Operate on children before parent.
		EXEC_DEPTH_LAST,  ///< Operate on children after parent.
		EXEC_LEAF         ///< Operate only on leaves of the hierarchy.
	} exec_order;


	class volume;

	/// Class representing the metadata location of a single object.
	/// This includes the path on disk, the backend type, etc.

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

			/// Name of on-disk object.
			std::string name;

			/// Metadata path inside object (backend-specific).
			std::string meta;

			//shared_ptr < volume > vol;

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

	static const std::string volume_fs_data_dir = "_data";
	static const std::string volume_fs_index = "index.db";
	static const std::string volume_fs_props = "tidas_volume.txt";


}

#include <tidas/utils.hpp>
#include <tidas/dict.hpp>
#include <tidas/intervals.hpp>
#include <tidas/schema.hpp>
#include <tidas/group.hpp>
#include <tidas/block.hpp>
#include <tidas/volume.hpp>
#include <tidas/copy.hpp>

#endif
