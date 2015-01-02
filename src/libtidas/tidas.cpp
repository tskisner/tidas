/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;



tidas::backend_path::backend_path () {
	path = "";
	name = "";
	meta = "";
	type = backend_type::none;
	comp = compression_type::none;
	mode = access_mode::read;
	//vol = NULL;
}


bool tidas::backend_path::operator== ( const backend_path & other ) const {
	if ( path != other.path ) {
		return false;
	}
	if ( name != other.name ) {
		return false;
	}
	if ( meta != other.meta ) {
		return false;
	}
	if ( type != other.type ) {
		return false;
	}
	if ( comp != other.comp ) {
		return false;
	}
	if ( mode != other.mode ) {
		return false;
	}
	return true;
}


bool tidas::backend_path::operator!= ( const backend_path & other ) const {
	return ! ( *this == other );
}


