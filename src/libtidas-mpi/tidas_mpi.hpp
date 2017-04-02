/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_MPI_HPP
#define TIDAS_MPI_HPP

#include <mpi.h>

#include <tidas.hpp>


namespace tidas {


    template < typename T >
    void mpi_pack ( T const & data, std::ostringstream & sstrm ) {
        sstrm.str("");
        sstrm.clear();
        {
            cereal::PortableBinaryOutputArchive outarch ( sstrm );
            outarch ( data );
        }
        return;
    }


    template < typename T >
    void mpi_unpack ( std::istringstream const & sstrm, T & data ) {
        {
            cereal::PortableBinaryInputArchive inarch ( sstrm );
            inarch ( data );
        }
        return;
    }


    template < typename T >
    void mpi_bcast ( MPI_Comm comm, T & data, int root ) {

        std::ostringstream sendstr;

        int rank;
        int ret = MPI_Comm_rank ( comm, &rank );

        long size;
        std::string buf;

        if ( rank == root ) {
            mpi_pack ( data, sendstr );
            buf = sendstr.str();
            size = buf.size();
        }

        ret = MPI_Bcast ( (void*)&size, 1, MPI_LONG, root, comm );

        if ( rank != root ) {
            buf.reserve ( size + 1 );
            buf[ size ] = '\0';
        }

        ret = MPI_Bcast ( (void*)&buf[0], size, MPI_CHAR, root, comm );

        if ( rank != root ) {
            std::istringstream recvstr ( buf );
            mpi_unpack ( recvstr, data );
        }

        return;
    }


    template < typename T >
    void mpi_send ( MPI_Comm comm, T & data, int receiver, int tag ) {

        std::ostringstream sendstr;

        long size;
        std::string buf;

        mpi_pack ( data, sendstr );
        buf = sendstr.str();
        size = buf.size();

        int ret = MPI_Send ( (void*)&size, 1, MPI_LONG, receiver, tag, comm );

        ret = MPI_Send ( (void*)&buf[0], size, MPI_CHAR, receiver, tag, comm );

        return;
    }


    template < typename T >
    void mpi_recv ( boost::mpi::communicator const & comm, T & data, int sender, int tag ) {

        long size;
        std::string buf;

        MPI_Status status = 0;

        int ret = MPI_Recv ( (void*)&size, 1, MPI_LONG, sender, tag, comm, &status );

        buf.reserve ( size + 1 );
        buf[ size ] = '\0';

        ret = MPI_Recv ( (void*)&buf[0], size, MPI_CHAR, sender, tag, comm, &status );

        std::istringstream recvstr ( buf );
        mpi_unpack ( recvstr, data );

        return;
    }


    template < typename T >
    MPI_Request mpi_isend ( MPI_Comm comm, T & data, int receiver, int tag ) {

        std::ostringstream sendstr;

        long size;
        std::string buf;

        mpi_pack ( data, sendstr );
        buf = sendstr.str();
        size = buf.size();

        MPI_Request req_size;
        MPI_Request req_data;

        int ret = MPI_Isend ( (void*)&size, 1, MPI_LONG, receiver, tag, comm, &req_size );

        ret = MPI_Isend ( (void*)&buf[0], size, MPI_CHAR, receiver, tag, comm, &req_data );

        comm_buffer_type buf;

        comm_pack ( data, buf );

        return req_data;
    }



}

#include <tidas/mpi_volume.hpp>


#endif
