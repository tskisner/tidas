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

			volume ( std::string const & path, backend_type type, compression_type comp );

			volume ( std::string const & path, access_mode mode );
			
			volume ( volume const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void relocate ( backend_path const & loc );

			void sync ();

			void flush () const;

			void copy ( volume const & other, std::string const & filter, backend_path const & loc );

			/// Create a link at the specified location.
			void link ( link_type const & type, std::string const & path ) const;

			/// Delete the on-disk data and metadata associated with this object.
			/// In-memory metadata is not modified.
			void wipe () const;

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

			std::unique_ptr < sqlite3 > index_;

	};

}


#endif
