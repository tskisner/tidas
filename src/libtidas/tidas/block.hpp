/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

	class block;

	class block {

		public :

			block ();
			block ( backend_path const & loc );
			block ( block const & orig );
			~block ();

			void relocate ( backend_path const & loc );
			backend_path location ();

			void block_append ( std::string const & name, block const & blk );

			void group_append ( std::string const & name, group const & grp );
			
			void intervals_append ( std::string const & name, intervals const & intr );


		private :

			backend_path loc_;
			std::vector < block > block_list_;
			std::vector < group > group_list_;
			std::vector < intervals > intervals_list_;

	};

}

#endif
