/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas/re2/re2.h>


using namespace std;
using namespace tidas;




tidas::schema_backend_getdata::schema_backend_getdata () {

}


tidas::schema_backend_getdata::~schema_backend_getdata () {

}


schema_backend_getdata * tidas::schema_backend_getdata::clone () {
	schema_backend_getdata * ret = new schema_backend_getdata ( *this );
	return ret;
}


void tidas::schema_backend_getdata::read_meta ( backend_path const & loc, string const & filter, field_list & fields ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::schema_backend_getdata::write_meta ( backend_path const & loc, string const & filter, field_list const & fields ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}

