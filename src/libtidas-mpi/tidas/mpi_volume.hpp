/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_MPI_VOLUME_HPP
#define TIDAS_MPI_VOLUME_HPP


Notes:

- root process has indexdb on on-disk
- other processes have in-memory indexdb
- distribution needs to split indexdb as well
- how to handle merging?
- OR should we have a command queue???

- each distributed process gets its own root block, which is a filtered copy of the main block


methods:

dist() : each process gets a filtered view of the root block
  - based on:  leaf nodes?  a specified level of the hierarchy?
  - also a in-memory indexdb containing only the filtered view.

exec() : each process calls operator on their block tree.

--------------

object::sync()
	- query DB if it exists
	- else read from disk

object::flush()
	- if indexdb.mem then
		- add transaction to history
		- execute
	  else
	  	- execute
	- write to disk

mpi_volume::ctor()
	- communicator
	- rank 0 creates with volume::flush()
	- distribution given in ctor?
	- otherwise, all procs get copy of root block
	- all procs get in-mem indexdb


mpi_volume::flush()
	- allreduce of transaction history
	- bcast of merged transactions
	- clear per-process in-mem db and replay
	- replay to on-disk indexdb
	- clear history on each process
	- does every object on all processes have to call sync() to pick up
	  changes made on other processes????


loc_ on each process has shared_ptr to in-memory indexdb

flush() does indexdb merge




namespace tidas {

	class mpi_volume {

		public :

			mpi_volume ();
			~mpi_volume ();
			mpi_volume & operator= ( mpi_volume const & other );

			mpi_volume ( mpi_volume const & other );

			mpi_volume ( std::string const & path, backend_type type, compression_type comp );

			mpi_volume ( std::string const & path, access_mode mode );
			
			mpi_volume ( mpi_volume const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void relocate ( backend_path const & loc );

			void sync ();

			void flush () const;

			void copy ( mpi_volume const & other, std::string const & filter, backend_path const & loc );

			/// Create a link at the specified location.
			void link ( link_type const & type, std::string const & path, std::string const & filter ) const;

			/// Delete the on-disk data and metadata associated with this object.
			/// In-memory metadata is not modified.
			void wipe ( std::string const & filter ) const;

			backend_path location () const;

			void reindex ();

			block & root ();

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

			void read_props ( backend_path & loc );

			void write_props ( backend_path const & loc ) const;

			backend_path root_loc ( backend_path const & loc );

			backend_path loc_;

			block root_;

			shared_ptr < indexdb > db_;

	};

}


#endif
