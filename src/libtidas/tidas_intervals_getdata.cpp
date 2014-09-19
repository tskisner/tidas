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


intervals_backend * tidas::intervals_backend_getdata::clone () {
	intervals_backend_getdata * ret = new intervals_backend_getdata ( *this );
	return ret;
}


void tidas::intervals_backend_getdata::read_meta ( backend_path const & loc ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write_meta ( backend_path const & loc ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::read_data ( backend_path const & loc, interval_list & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write_data ( backend_path const & loc, interval_list const & intr ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}

