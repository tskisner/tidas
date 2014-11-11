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
			~volume ();
			volume & operator= ( volume const & other );

			volume ( volume const & other );
			volume ( backend_path const & loc );
			volume ( volume const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void relocate ( backend_path const & loc );

			void sync ();

			void flush ();

			void copy ( volume const & other, std::string const & filter, backend_path const & loc );

			void duplicate ( backend_path const & loc );

			backend_path location () const;

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

			std::unique_ptr < sqlite3 > index_;

	};

}


#endif
