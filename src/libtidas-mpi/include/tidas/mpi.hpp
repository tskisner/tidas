/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_MPI_HPP
#define TIDAS_MPI_HPP

#include <mpi.h>

#include <tidas/tidas.hpp>


namespace tidas {

    typedef std::vector < char > mpi_comm_buffer_type;

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
    void mpi_unpack ( std::istringstream & sstrm, T & data ) {
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

        std::string sbuf;

        if ( rank == root ) {
            mpi_pack ( data, sendstr );
            sbuf = sendstr.str();
            size = sbuf.size();
        }

        ret = MPI_Bcast ( (void*)&size, 1, MPI_LONG, root, comm );

        std::vector < char > vbuf ( size );

        if ( rank == root ) {
            std::copy ( sbuf.begin(), sbuf.end(), vbuf.begin() );
        }

        ret = MPI_Bcast ( (void*)&vbuf[0], size, MPI_CHAR, root, comm );

        if ( rank != root ) {
            std::string buf ( &vbuf[0], size );
            std::istringstream recvstr ( buf );
            mpi_unpack ( recvstr, data );
        }

        return;
    }


    template < typename T >
    void mpi_send ( MPI_Comm comm, T & data, int receiver, int tag ) {

        std::ostringstream sendstr;

        mpi_pack ( data, sendstr );
        std::string sbuf ( sendstr.str() );

        long size = sbuf.size();

        std::vector < char > vbuf ( size );
        std::copy ( sbuf.begin(), sbuf.end(), vbuf.begin() );

        int ret = MPI_Send ( (void*)&size, 1, MPI_LONG, receiver, tag, comm );

        ret = MPI_Send ( (void*)&vbuf[0], size, MPI_CHAR, receiver, tag, comm );

        return;
    }


    template < typename T >
    void mpi_recv ( MPI_Comm comm, T & data, int sender, int tag ) {

        MPI_Status status;

        long size;
        int ret = MPI_Recv ( (void*)&size, 1, MPI_LONG, sender, tag, comm, &status );

        std::vector < char > vbuf ( size );

        ret = MPI_Recv ( (void*)&vbuf[0], size, MPI_CHAR, sender, tag, comm, &status );

        std::string buf ( &vbuf[0], size );
        std::istringstream recvstr ( buf );

        mpi_unpack ( recvstr, data );

        return;
    }


    template < typename T >
    MPI_Request mpi_isend ( MPI_Comm comm, T & data, int receiver, int tag ) {

        std::ostringstream sendstr;

        mpi_pack ( data, sendstr );

        long size = sendstr.str().size();

        std::vector < char > vbuf ( size );
        std::copy ( sendstr.str().begin(), sendstr.str().end(), vbuf.begin() );

        MPI_Request req_size;
        MPI_Request req_data;

        int ret = MPI_Isend ( (void*)&size, 1, MPI_LONG, receiver, tag, comm, &req_size );

        ret = MPI_Isend ( (void*)&vbuf[0], size, MPI_CHAR, receiver, tag, comm, &req_data );

        return req_data;
    }


    template < typename T >
    void mpi_gather ( MPI_Comm comm, T & data, std::vector < T > & result, int root ) {

        typedef T datatype;

        int rank;
        int nproc;
        int ret = MPI_Comm_rank ( comm, &rank );
        ret = MPI_Comm_size ( comm, &nproc );

        // Serialize the data for sending

        std::ostringstream sendstr;
        mpi_pack ( data, sendstr );

        std::string sendstrg = sendstr.str();
        sendstr.str("");

        int size = sendstrg.size();

        std::vector < char > sendbuf ( size );
        std::copy ( sendstrg.begin(), sendstrg.end(), sendbuf.begin() );
        sendstrg.clear();

        // All processes communicate the size of their local blobs.

        std::vector < int > recvsizes;

        void * recvptr = NULL;
        if ( rank == root ) {
            recvsizes.reserve ( nproc );
            recvptr = (void*)&recvsizes[0];
        }

        ret = MPI_Gather ( (void*)&size, 1, MPI_INT, recvptr, 1, MPI_INT,
            root, comm );

        // Create our local receive buffer and displacements.

        size_t total = 0;
        std::vector < int > recvoff;
        std::vector < char > recvbuf;
        int * counts = NULL;
        int * displs = NULL;

        if ( rank == root ) {
            recvoff.resize ( nproc );
            total = recvsizes[0];
            recvoff[0] = 0;
            for ( int i = 1; i < nproc; ++i ) {
                recvoff[i] = recvoff[i - 1] + recvsizes[i - 1];
                total += recvsizes[i];
            }

            recvbuf.resize ( total );
            recvptr = (void*)&recvbuf[0];
            counts = &recvsizes[0];
            displs = &recvoff[0];
        }

        // Communicate

        ret = MPI_Gatherv ( (void*)&sendbuf[0], size, MPI_CHAR, recvptr,
            counts, displs, MPI_CHAR, root, comm );

        // Clear the result vector and deserialize

        result.clear();

        if ( rank == root ) {
            for ( int i = 0; i < nproc; ++i ) {
                std::string buf ( &recvbuf[recvoff[i]], recvsizes[i] );
                std::istringstream recvstr ( buf );
                datatype temp;
                mpi_unpack ( recvstr, temp );
                result.push_back ( temp );
            }
        }

        return;
    }


    template < typename T >
    void mpi_allgather ( MPI_Comm comm, T & data, std::vector < T > & result ) {

        typedef T datatype;

        int rank;
        int nproc;
        int ret = MPI_Comm_rank ( comm, &rank );
        ret = MPI_Comm_size ( comm, &nproc );

        // Serialize the data for sending

        std::ostringstream sendstr;
        mpi_pack ( data, sendstr );

        std::string sendstrg = sendstr.str();
        sendstr.str("");

        int size = sendstrg.size();

        std::vector < char > sendbuf ( size );
        std::copy ( sendstrg.begin(), sendstrg.end(), sendbuf.begin() );
        sendstrg.clear();

        // All processes communicate the size of their local blobs.

        std::vector < int > recvsizes ( nproc );

        ret = MPI_Allgather ( (void*)&size, 1, MPI_INT, (void*)&recvsizes[0],
            1, MPI_INT, comm );

        // Create our local receive buffer and displacements.

        std::vector < int > recvoff ( nproc );
        size_t total = recvsizes[0];
        recvoff[0] = 0;
        for ( int i = 1; i < nproc; ++i ) {
            recvoff[i] = recvoff[i - 1] + recvsizes[i - 1];
            total += recvsizes[i];
        }

        std::vector < char > recvbuf ( total );

        // Communicate

        ret = MPI_Allgatherv ( (void*)&sendbuf[0], size, MPI_CHAR, (void*)&recvbuf[0],
            &recvsizes[0], &recvoff[0], MPI_CHAR, comm );

        // Clear the result vector and deserialize

        result.clear();

        for ( int i = 0; i < nproc; ++i ) {
            std::string buf ( &recvbuf[recvoff[i]], recvsizes[i] );
            std::istringstream recvstr ( buf );
            datatype temp;
            mpi_unpack ( recvstr, temp );
            result.push_back ( temp );
        }

        return;
    }

    void mpi_init ( int argc, char *argv[] );

    void mpi_finalize ( );

    void mpi_dist_uniform ( MPI_Comm comm, size_t n, size_t * offset, size_t * nlocal );

}

#include <tidas/mpi_volume.hpp>


#endif
