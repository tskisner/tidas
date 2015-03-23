/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef CTIDAS_HPP
#define CTIDAS_HPP

#include <ctidas.h>

#include <tidas_internal.hpp>


namespace ctidas {  

	// enum conversions

	tidas::data_type convert_type ( ctidas_data_type in );


	// retrieve shared_ptr from wrapped handle.

	template < typename C, typename T >
	std::shared_ptr < T > ptr_extract ( C * object ) {
		std::shared_ptr < T > * addr = reinterpret_cast < std::shared_ptr < T > * > ( object->handle );
		std::shared_ptr < T > ret = (*addr);
		return ret;
	}


	// allocate C object pointer and initialize to shared_ptr address

	template < typename C, typename T >
	C * ptr_init ( std::shared_ptr < T > * ref ) {
		C * object = NULL;

		object = ( C * ) malloc ( sizeof ( C ) );
		if ( ! object ) {
			std::ostringstream o;
			o << "ctidas::ptr_init cannot allocate object";
			TIDAS_THROW( o.str().c_str() );
		}

		object->handle = (void*) ( ref );

		return object;
	}


	// free C object pointer

	template < typename C, typename T >
	void ptr_free ( C * object ) {
		if ( object ) {
			std::shared_ptr < T > * addr = (std::shared_ptr < T > *) object->handle;
			if ( addr ) {
				delete addr;
			}
			free ( object );
		}
		return;
	}


}

#endif