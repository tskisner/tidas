/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_GROUP_HDF5_HPP
#define TIDAS_GROUP_HDF5_HPP


namespace tidas {

	template < typename T >
	void hdf5_helper_field_read ( backend_path const & loc, size_t type_indx, index_type offset, std::vector < T > & data ) {

		T testval;
		data_type type = data_type_get ( typeid ( testval ) );

		// check if file exists

		std::string fspath = loc.path + path_sep + loc.name;

		int64_t fsize = fs_stat ( fspath.c_str() );
		if ( fsize <= 0 ) {
			std::ostringstream o;
			o << "HDF5 group file " << fspath << " does not exist";
			TIDAS_THROW( o.str().c_str() );
		}

		// open file and dataset

		hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT );

		std::string mpath = std::string("/") + group_hdf5_dataset_prefix + std::string("_") + data_type_to_string( type );

		hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

		// set up file and memory dataspaces

		hid_t file_dataspace = H5Dget_space ( dataset );

		hsize_t dims[1];
		dims[0] = data.size();

		hid_t mem_dataspace = H5Screate_simple ( 1, dims, NULL );

		// select hyperslab of file dataspace

		hsize_t doff[2];
		hsize_t dspan[2];

		doff[0] = type_indx;
		doff[1] = offset;

		dspan[0] = 1;
		dspan[1] = data.size();

		herr_t status = H5Sselect_hyperslab ( file_dataspace, H5S_SELECT_SET, doff, NULL, dspan, NULL );

		// read data

		hid_t mem_datatype = hdf5_data_type ( type );

		status = H5Dread ( dataset, mem_datatype, mem_dataspace, file_dataspace, H5P_DEFAULT, (void *)data.data() );

		// clean up

		status = H5Tclose ( mem_datatype );
		status = H5Sclose ( mem_dataspace );
		status = H5Sclose ( file_dataspace );
		status = H5Dclose ( dataset );
		status = H5Fclose ( file );

		return;
	}


	template < typename T >
	void hdf5_helper_field_write ( backend_path const & loc, size_t type_indx, index_type offset, std::vector < T > const & data ) {

		T testval;
		data_type type = data_type_get ( typeid ( testval ) );

		// check if file exists

		std::string fspath = loc.path + path_sep + loc.name;

		int64_t fsize = fs_stat ( fspath.c_str() );
		if ( fsize <= 0 ) {
			std::ostringstream o;
			o << "HDF5 group file " << fspath << " does not exist";
			TIDAS_THROW( o.str().c_str() );
		}

		// open file and dataset

		hid_t file = H5Fopen ( fspath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT );

		std::string mpath = std::string("/") + group_hdf5_dataset_prefix + std::string("_") + data_type_to_string( type );

		hid_t dataset = H5Dopen ( file, mpath.c_str(), H5P_DEFAULT );

		// set up file and memory dataspaces

		hid_t file_dataspace = H5Dget_space ( dataset );

		hsize_t dims[1];
		dims[0] = data.size();

		hid_t mem_dataspace = H5Screate_simple ( 1, dims, NULL );

		// select hyperslab of file dataspace

		hsize_t doff[2];
		hsize_t dspan[2];

		doff[0] = type_indx;
		doff[1] = offset;

		dspan[0] = 1;
		dspan[1] = data.size();

		herr_t status = H5Sselect_hyperslab ( file_dataspace, H5S_SELECT_SET, doff, NULL, dspan, NULL );

		// read data

		hid_t mem_datatype = hdf5_data_type ( type );

		status = H5Dwrite ( dataset, mem_datatype, mem_dataspace, file_dataspace, H5P_DEFAULT, (void const *)data.data() );

		// clean up

		status = H5Tclose ( mem_datatype );
		status = H5Sclose ( mem_dataspace );
		status = H5Sclose ( file_dataspace );
		status = H5Dclose ( dataset );
		status = H5Fclose ( file );

		return;
	}

}


#endif
