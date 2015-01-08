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
#include <deque>

#include <cstdint>

#include <cereal/types/base_class.hpp>
#include <cereal/types/polymorphic.hpp>
#include <cereal/archives/portable_binary.hpp>



namespace tidas {

	typedef double time_type;

	typedef int64_t index_type;

	enum class data_type {
		none, ///< Undefined type.
		int8, ///< C compatible int8_t type.
		uint8, ///< C compatible uint8_t type.
		int16, ///< C compatible int16_t type.
		uint16, ///< C compatible uint16_t type.
		int32, ///< C compatible int32_t type.
		uint32, ///< C compatible uint32_t type.
		int64, ///< C compatible int64_t type.
		uint64, ///< C compatible uint64_t type.
		float32, ///< float type.
		float64, ///< double type.
		string ///< C++ string type.
	};

	enum class backend_type {
		none, ///< Backend not assigned.
		hdf5, ///< HDF5 backend.
		getdata ///< GetData backend.
	};

	enum class compression_type {
		none, ///< No compression.
		gzip, ///< gzip (libz) compression.
		bzip2 ///< bzip2 compression.
	};

	enum class access_mode {
		read, ///< Read-only access.
		readwrite ///< Read-write access.
	};

	enum class link_type {
		none, ///< Not a link.
		hard, ///< Hard Link.
		soft   ///< Symlink.
	};

	enum class exec_order {
		depth_first, ///< Operate on children before parent.
		depth_last,  ///< Operate on children after parent.
		leaf         ///< Operate only on leaves of the hierarchy.
	};


	/// Class representing the metadata location of a single object.
	/// This includes the path on disk, the backend type, etc.

	class indexdb;

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

			std::shared_ptr < indexdb > idx;

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

	static const std::string env_hdf5_chunk = "TIDAS_HDF5_CHUNK";
	static const size_t env_hdf5_chunk_default = 1000000;


}

#include <tidas/utils.hpp>
#include <tidas/dict.hpp>
#include <tidas/intervals.hpp>
#include <tidas/schema.hpp>
#include <tidas/group.hpp>
#include <tidas/block.hpp>
#include <tidas/indexdb.hpp>
#include <tidas/volume.hpp>
#include <tidas/copy.hpp>

#endif
