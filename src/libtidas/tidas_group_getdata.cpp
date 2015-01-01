/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


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


void tidas::group_backend_getdata::read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) const {
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


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > & data ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


