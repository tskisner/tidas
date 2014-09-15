/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


#ifdef HAVE_HDF5
extern "C" {
	#include <hdf5.h>
}
#endif


using namespace std;
using namespace tidas;


tidas::group_backend_hdf5::group_backend_hdf5 ( ) {

}


tidas::group_backend_hdf5::~group_backend_hdf5 () {

	
}


group_backend_hdf5 * tidas::group_backend_hdf5::clone () {
	group_backend_hdf5 * ret = new group_backend_hdf5 ( *this );
	return ret;
}


void tidas::group_backend_hdf5::read ( backend_path const & loc, schema & schm, index_type & nsamp ) {

	return;
}


void tidas::group_backend_hdf5::write ( backend_path const & loc, schema const & schm, index_type nsamp ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data ) {

	return;
}


void tidas::group_backend_hdf5::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data ) {

	return;
}


void tidas::group_backend_hdf5::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data ) {

	return;
}

