/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

	// forward declare copy functions

	class block;

	void data_copy ( intervals const & in, intervals & out );
	void data_copy ( group const & in, group & out );
	void data_copy ( block const & in, block & out );


	// block class

	class block {

		public :

			block ();
			~block ();
			block & operator= ( block const & other );

			block ( block const & other );

			block ( backend_path const & loc );
			
			block ( block const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void relocate ( backend_path const & loc );

			void sync ();

			void flush () const;

			void copy ( block const & other, std::string const & filter, backend_path const & loc );

			/// Create a link at the specified location.
			void link ( link_type const & type, std::string const & path, std::string const & filter ) const;

			/// Delete the on-disk data and metadata associated with this object.
			/// In-memory metadata is not modified.
			void wipe ( std::string const & filter ) const;

			backend_path location () const;

			// data ops

			void clear();


			group & group_add ( std::string const & name, group const & grp );

			group & group_get ( std::string const & name );

			group const & group_get ( std::string const & name ) const;
			
			void group_del ( std::string const & name );

			std::vector < std::string > all_groups () const;
			
			void clear_groups();


			intervals & intervals_add ( std::string const & name, intervals const & intr );
			
			intervals & intervals_get ( std::string const & name );

			intervals const & intervals_get ( std::string const & name ) const;
			
			void intervals_del ( std::string const & name );
			
			std::vector < std::string > all_intervals () const;
			
			void clear_intervals();


			block & block_add ( std::string const & name, block const & blk );
			
			block & block_get ( std::string const & name );

			block const & block_get ( std::string const & name ) const;
			
			void block_del ( std::string const & name );
			
			std::vector < std::string > all_blocks () const;
			
			void clear_blocks();


			block select ( std::string const & filter = "" ) const;

			template < class P >
			void exec ( P & op, exec_order order ) {

				if ( order == EXEC_DEPTH_LAST ) {
					op ( *this );
				}

				if ( block_data_.size() == 0 ) {

					// this is a leaf
					if ( order == EXEC_LEAF ) {
						op ( *this );
					}
				
				} else {
					// operate on sub blocks

					for ( std::map < std::string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
						it->second.exec ( op, order );
					}

				}

				if ( order == EXEC_DEPTH_FIRST ) {
					op ( *this );
				}

				return;
			}


		private :

			backend_path group_loc ( backend_path const & loc, std::string const & name ) const;
			
			backend_path intervals_loc ( backend_path const & loc, std::string const & name ) const;
			
			backend_path block_loc ( backend_path const & loc, std::string const & name ) const;

			std::map < std::string, block > block_data_;
			std::map < std::string, group > group_data_;
			std::map < std::string, intervals > intervals_data_;

			backend_path loc_;

	};


	class block_link_operator {

		public :

			void operator() ( block & blk );

			link_type type;
			std::string path;
			std::string start;

	};


	class block_wipe_operator {

		public :

			void operator() ( block & blk );

	};


}

#endif
