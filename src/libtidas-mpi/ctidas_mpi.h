/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef CTIDAS_MPI_H
#define CTIDAS_MPI_H

#include <mpi.h>

#include <stdint.h>
#include <stddef.h>

#include <ctidas.h>

#ifdef __cplusplus
extern "C" {
#endif


/* MPI Volume */

struct ctidas_mpi_volume_;
typedef struct ctidas_mpi_volume_ ctidas_mpi_volume;

ctidas_mpi_volume * ctidas_mpi_volume_create ( MPI_Comm comm, char const * path, ctidas_backend_type type, ctidas_compression_type comp );

ctidas_mpi_volume * ctidas_mpi_volume_open ( MPI_Comm comm, char const * path, ctidas_access_mode mode );

void ctidas_mpi_volume_close ( ctidas_mpi_volume * vol );

void ctidas_mpi_volume_duplicate ( ctidas_mpi_volume * vol, char const * path,
    ctidas_backend_type type, ctidas_compression_type comp, 
    char const * filter );

void ctidas_mpi_volume_link ( ctidas_mpi_volume * vol, char const * path,
    ctidas_link_type type, char const * filter );

void ctidas_mpi_volume_meta_sync ( ctidas_mpi_volume * vol );

MPI_Comm ctidas_mpi_volume_comm ( ctidas_mpi_volume * vol );

int ctidas_mpi_volume_comm_rank ( ctidas_mpi_volume * vol );

int ctidas_mpi_volume_comm_size ( ctidas_mpi_volume * vol );

ctidas_block * ctidas_mpi_volume_root ( ctidas_mpi_volume * vol );

void ctidas_mpi_volume_exec ( ctidas_mpi_volume * vol, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux );


/* Data copy */

void ctidas_data_mpi_volume_copy ( ctidas_mpi_volume const * in, ctidas_mpi_volume * out );


/* Utilities */

void ctidas_mpi_init ( int argc, char *argv[] );

void ctidas_mpi_finalize ( );

void ctidas_mpi_dist_uniform ( MPI_Comm comm, size_t n, size_t * offset, size_t * nlocal );


#ifdef __cplusplus
}
#endif

#endif

