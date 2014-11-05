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


group_backend * tidas::group_backend_getdata::clone () {
	group_backend_getdata * ret = new group_backend_getdata ( *this );
	return ret;
}


string tidas::group_backend_getdata::dict_meta () {
	return string("");
}


string tidas::group_backend_getdata::schema_meta () {
	return string("");
}


void tidas::group_backend_getdata::read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::group_backend_getdata::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > const & data ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


