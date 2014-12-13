/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;



tidas::intervals_backend_getdata::intervals_backend_getdata () : intervals_backend () {

}


tidas::intervals_backend_getdata::~intervals_backend_getdata () {

}


tidas::intervals_backend_getdata::intervals_backend_getdata ( intervals_backend_getdata const & other ) {

}


intervals_backend_getdata & tidas::intervals_backend_getdata::operator= ( intervals_backend_getdata const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


string tidas::intervals_backend_getdata::dict_meta () const {
	return "";
}


void tidas::intervals_backend_getdata::read ( backend_path const & loc, size_t & size ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write ( backend_path const & loc, size_t const & size ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::link ( backend_path const & loc, link_type type, std::string const & path, std::string const & name ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::purge ( backend_path const & loc ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::read_data ( backend_path const & loc, interval_list & intr ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write_data ( backend_path const & loc, interval_list const & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}

