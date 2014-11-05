/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;




tidas::dict_backend_getdata::dict_backend_getdata () {

}


tidas::dict_backend_getdata::~dict_backend_getdata () {

}


tidas::dict_backend_getdata::dict_backend_getdata ( dict_backend_getdata const & other ) {

}


dict_backend_getdata & tidas::dict_backend_getdata::operator= ( dict_backend_getdata const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


dict_backend * tidas::dict_backend_getdata::clone () {
	dict_backend_getdata * ret = new dict_backend_getdata ( *this );
	return ret;
}


void tidas::dict_backend_getdata::read ( backend_path const & loc, map < string, string > & data, map < string, data_type > & types ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::dict_backend_getdata::write ( backend_path const & loc, map < string, string > const & data, map < string, data_type > const & types ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


