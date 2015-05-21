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

    tidas::backend_type convert_from_c ( ctidas_backend_type in );

    ctidas_backend_type convert_to_c ( tidas::backend_type in );

    tidas::compression_type convert_from_c ( ctidas_compression_type in );

    ctidas_compression_type convert_to_c ( tidas::compression_type in );

    tidas::link_type convert_from_c ( ctidas_link_type in );

    ctidas_link_type convert_to_c ( tidas::link_type in );

    tidas::access_mode convert_from_c ( ctidas_access_mode in );

    ctidas_access_mode convert_to_c ( tidas::access_mode in );

    tidas::exec_order convert_from_c ( ctidas_exec_order in );

    ctidas_exec_order convert_to_c ( tidas::exec_order in );

    // wrapper class for calling a C function pointer
    // from block::exec

    class block_operator {

        public :

            void operator() ( tidas::block const & blk );

            CTIDAS_EXEC_OP op;
            void * aux;

    };

}

#endif