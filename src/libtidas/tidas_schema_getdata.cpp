/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;




tidas::schema_backend_getdata::schema_backend_getdata () {

}


tidas::schema_backend_getdata::~schema_backend_getdata () {

}


tidas::schema_backend_getdata::schema_backend_getdata ( schema_backend_getdata const & other ) {

}


schema_backend_getdata & tidas::schema_backend_getdata::operator= ( schema_backend_getdata const & other ) {
	if ( this != &other ) {

	}
	return *this;
}


schema_backend * tidas::schema_backend_getdata::clone () {
	schema_backend_getdata * ret = new schema_backend_getdata ( *this );
	return ret;
}


void tidas::schema_backend_getdata::read ( backend_path const & loc, field_list & fields ) const {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::schema_backend_getdata::write ( backend_path const & loc, field_list const & fields ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}

