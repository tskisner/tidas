/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_VOLUME_HPP
#define TIDAS_VOLUME_HPP


namespace tidas {

	class volume {

		public :

			volume ();
			~volume ();
			volume & operator= ( volume const & other );

			volume ( volume const & other );

			/// Create a new volume.
			volume ( std::string const & path, backend_type type, compression_type comp );

			/// Open an existing volume.
			volume ( std::string const & path, access_mode mode );
			
			volume ( volume const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void copy ( volume const & other, std::string const & filter, backend_path const & loc );

			/// Export a filtered subset of the volume to a new location.
			void duplicate ( std::string const & path, backend_type type, compression_type comp, std::string const & filter ) const;

			/// Create a (hard or soft) linked filtered subset of the volume to a new location.
			void link ( std::string const & path, link_type const & type, std::string const & filter ) const;

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

			backend_path root_loc ( backend_path const & loc );

			backend_path loc_;

			block root_;

			std::shared_ptr < indexdb > db_;

	};

}


#endif
