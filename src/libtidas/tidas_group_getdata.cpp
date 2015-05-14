/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas_backend_getdata.hpp>


using namespace std;
using namespace tidas;


tidas::group_backend_getdata::group_backend_getdata () {

}


tidas::group_backend_getdata::~group_backend_getdata () {
	
}


tidas::group_backend_getdata::group_backend_getdata ( group_backend_getdata const & other ) {

}


group_backend_getdata & tidas::group_backend_getdata::operator= ( group_backend_getdata const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


string tidas::group_backend_getdata::dict_meta () const {
	return string("");
}


string tidas::group_backend_getdata::schema_meta () const {
	return string("");
}


void tidas::group_backend_getdata::read ( backend_path const & loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::resize ( backend_path const & loc, index_type const & nsamp ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::update_range ( backend_path const & loc, time_type const & start, time_type const & stop ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::link ( backend_path const & loc, link_type type, std::string const & path ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::wipe ( backend_path const & loc ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, float * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, float const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, double * data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, double const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, char ** data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, index_type ndata, char * const * data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


