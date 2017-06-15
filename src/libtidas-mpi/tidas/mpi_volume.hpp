/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_MPI_VOLUME_HPP
#define TIDAS_MPI_VOLUME_HPP


namespace tidas {

    class mpi_volume {

        public :

            mpi_volume ();
            ~mpi_volume ();
            mpi_volume & operator= ( mpi_volume const & other );

            mpi_volume ( mpi_volume const & other );

            /// Create a new volume.
            mpi_volume ( MPI_Comm comm, std::string const & path, 
                backend_type type, compression_type comp, 
                std::map < std::string, std::string > extra = 
                std::map < std::string, std::string > () );

            /// Open an existing volume.
            mpi_volume ( MPI_Comm comm, std::string const & path, 
                access_mode mode, std::string const & dist = "" );
            
            mpi_volume ( mpi_volume const & other, std::string const & filter, 
                backend_path const & loc );

            mpi_volume ( MPI_Comm comm, mpi_volume const & other, std::string const & filter, 
                backend_path const & loc );

            MPI_Comm comm ( ) const;

            int comm_rank ( ) const;

            int comm_size ( ) const;

            std::string path_make ( std::string const & path ) const;

            std::string path_get ( std::string const & path ) const;

            void open ( );

            void close ( );

            void meta_sync ( );

            // metadata ops

            void copy ( MPI_Comm comm, mpi_volume const & other, std::string const & filter, 
                backend_path const & loc );

            /// Export a filtered subset of the volume to a new location.
            void duplicate ( std::string const & path, backend_type type, 
                compression_type comp, std::string const & filter = "", 
                std::map < std::string, std::string > extra = 
                std::map < std::string, std::string > () ) const;

            /// Create a (hard or soft) linked filtered subset of the volume to a new location.
            void link ( std::string const & path, link_type const & type, 
                std::string const & filter ) const;

            /// Delete the on-disk data and metadata associated with this object.
            /// In-memory metadata is not modified.
            void wipe ( std::string const & filter ) const;

            backend_path location () const;

            /// Get the root block of the volume.
            block & root ();

            /// Get the (const) root block of the volume.
            block const & root () const;

            template < class P >
            void exec ( P & op, exec_order order, std::string const & filter = "" ) {

                if ( filter == "" ) {

                    root_.exec ( op, order );
                
                } else {
                
                    block selected = root_.select ( filter );
                    selected.exec ( op, order );
                
                }

                return;
            }

        private :

            void index_setup ();

            void read_props ( backend_path & loc );

            void write_props ( backend_path const & loc ) const;

            backend_path root_loc ( backend_path const & loc ) const;

            backend_path loc_;

            block root_;

            MPI_Comm comm_;
            int nproc_;
            int rank_;

            std::string dist_;

            std::shared_ptr < indexdb_mem > localdb_;
            std::shared_ptr < indexdb_sql > masterdb_;

    };

    // mpi_volume copy

    void data_copy ( mpi_volume const & in, mpi_volume & out );

}





// Notes:

// - root process has indexdb on on-disk
// - other processes have in-memory indexdb
// - distribution needs to split indexdb as well
// - how to handle merging?
// - OR should we have a command queue???

// - each distributed process gets its own root block, which is a filtered copy of the main block


// methods:

// dist() : each process gets a filtered view of the root block
//   - based on:  leaf nodes?  a specified level of the hierarchy?
//   - also a in-memory indexdb containing only the filtered view.

// exec() : each process calls operator on their block tree.

// --------------

// object::sync()
//     - query DB if it exists
//     - else read from disk

// object::flush()
//     - if indexdb.mem then
//         - add transaction to history
//         - execute
//       else
//           - execute
//     - write to disk

// mpi_volume::ctor()
//     - communicator
//     - rank 0 creates with volume::flush()
//     - distribution given in ctor?
//     - otherwise, all procs get copy of root block
//     - all procs get in-mem indexdb


// mpi_volume::flush()
//     - allreduce of transaction history
//     - bcast of merged transactions
//     - clear per-process in-mem db and replay
//     - replay to on-disk indexdb
//     - clear history on each process
//     - does every object on all processes have to call sync() to pick up
//       changes made on other processes????


// loc_ on each process has shared_ptr to in-memory indexdb

// flush() does indexdb merge



#endif
