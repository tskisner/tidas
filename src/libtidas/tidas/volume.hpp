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

	class volume {

		public :

			volume ();
			volume ( std::string const & path, std::string const & filter );
			~volume ();

			void write ( std::string const & path, std::string const & filter, backend_type type, compression_type comp );

			void duplicate ( std::string const & path, std::string const & filter, backend_type type, compression_type comp );

			void index ();

			void set_dirty ();

			std::vector < block > & blocks ();

			block & block_append ( std::string const & name, block const & blk );

		private :
			// copy constructor forbidden
			volume ( volume const & orig ) {}

			bool dirty_;

			backend_path loc_;

			block root_;

			sqlite3 * index_;

	};

}


#endif
