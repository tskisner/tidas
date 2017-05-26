/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <ctidas_mpi.hpp>

using namespace std;
using namespace tidas;



ctidas_mpi_volume * ctidas_mpi_volume_create ( MPI_Comm comm, char const * path, ctidas_backend_type type, ctidas_compression_type comp ) {
    return reinterpret_cast < ctidas_mpi_volume * > ( new mpi_volume( comm, string(path), ctidas::convert_from_c(type), ctidas::convert_from_c(comp) ) );
}

ctidas_mpi_volume * ctidas_mpi_volume_open ( MPI_Comm comm, char const * path, ctidas_access_mode mode ) {
    return reinterpret_cast < ctidas_mpi_volume * > ( new mpi_volume( comm, string(path), ctidas::convert_from_c(mode) ) );
}

void ctidas_mpi_volume_close ( ctidas_mpi_volume * vol ) {
    delete reinterpret_cast < mpi_volume * > ( vol );
    return;
}

ctidas_block * ctidas_mpi_volume_root ( ctidas_mpi_volume * vol ) {
    mpi_volume * v = reinterpret_cast < mpi_volume * > ( vol );
    ctidas_block * ret = ctidas_block_alloc();
    block * b = reinterpret_cast < block * > ( ret );
    (*b) = v->root();
    return ret;
}

void ctidas_mpi_volume_exec ( ctidas_mpi_volume * vol, ctidas_exec_order order, char const * filter, CTIDAS_EXEC_OP op, void * aux ) {
    mpi_volume * v = reinterpret_cast < mpi_volume * > ( vol );
    ctidas::block_operator opcpp;
    opcpp.op = op;
    opcpp.aux = aux;
    v->exec ( opcpp, ctidas::convert_from_c(order), string(filter) );
    return;
}



void ctidas_data_mpi_volume_copy ( ctidas_mpi_volume const * in, ctidas_mpi_volume * out ) {
    mpi_volume const * vin = reinterpret_cast < mpi_volume const * > ( in );
    mpi_volume * vout = reinterpret_cast < mpi_volume * > ( out );
    data_copy ( (*vin), (*vout) );
    return;
}




