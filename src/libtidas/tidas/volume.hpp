/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_VOLUME_HPP
#define TIDAS_VOLUME_HPP

#include <tidas/sqlite3.h>


namespace tidas {

	static const std::string block_fs_name = "blocks";

	class select {

		public :

			std::string block_filter;
			std::map < std::string, block_select > block_sel;

	};


	class volume {

		public :

			volume ( std::string const & path, backend_type type );
			volume ( std::string const & path );
			~volume ();

			void read_meta ();

			void write_meta ();

			void index ();

			select query ( std::string const & match );

			block & root ();
			
			void duplicate ( std::string const & newpath, backend_type newtype, select const & selection );

			void block_append ( std::string const & name, block const & blk );

		private :
			// copy constructor forbidden
			volume ( volume const & orig ) {}

			backend_path loc_;
			block root_;
			sqlite3 * index_;

	};

}


#endif
