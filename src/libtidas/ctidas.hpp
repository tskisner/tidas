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

	tidas::data_type convert_from_c ( ctidas_data_type in );

	ctidas_data_type convert_to_c ( tidas::data_type in );


}

#endif