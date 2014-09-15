/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


#ifdef HAVE_GETDATA
extern "C" {
	#include <getdata.h>
}
#endif


using namespace std;
using namespace tidas;



tidas::intervals_backend_getdata::intervals_backend_getdata () : intervals_backend () {

}


tidas::intervals_backend_getdata::~intervals_backend_getdata () {

}


intervals_backend_getdata * tidas::intervals_backend_getdata::clone () {
	intervals_backend_getdata * ret = new intervals_backend_getdata ( *this );
	return ret;
}


void tidas::intervals_backend_getdata::read ( backend_path const & loc ) {
	TIDAS_THROW( "GetData backend not supported" );
	return;
}


void tidas::intervals_backend_getdata::write ( backend_path const & loc ) {
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

