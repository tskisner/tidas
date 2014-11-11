/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

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

			void flush ();

			void copy ( block const & other, std::string const & filter, backend_path const & loc );

			void duplicate ( backend_path const & loc );

			backend_path location () const;

			// data ops

			group & group_add ( std::string const & name, group const & grp );
			group & group_get ( std::string const & name );
			void group_del ( std::string const & name );

			intervals & intervals_add ( std::string const & name, intervals const & intr );
			intervals & intervals_get ( std::string const & name );
			void intervals_del ( std::string const & name );

			block & block_add ( std::string const & name, block const & blk );
			block & block_get ( std::string const & name );
			void block_del ( std::string const & name );


		private :

			backend_path group_loc ( backend_path const & loc, std::string const & name );
			backend_path intervals_loc ( backend_path const & loc, std::string const & name );
			backend_path block_loc ( backend_path const & loc, std::string const & name );

			std::map < std::string, block > block_data_;
			std::map < std::string, group > group_data_;
			std::map < std::string, intervals > intervals_data_;

			backend_path loc_;

	};

}

#endif
