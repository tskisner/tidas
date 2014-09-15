/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

	static const std::string group_fs_name = "groups";
	static const std::string interval_fs_name = "intervals";

	class block_select;

	class block_select {

		public :

			std::string intr_match;
			std::string group_filter;
			group_select group_sel;
			std::string block_filter;
			std::map < std::string, block_select > block_sel;

	};


	class block;

	class block {

		public :

			block ();
			block ( backend_path const & loc );
			block ( block const & orig );
			~block ();

			void read_meta ();

			void write_meta ();
			
			void relocate ( backend_path const & loc );
			
			backend_path location () const;
			
			void duplicate ( backend_path const & newloc, block_select const & selection );

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
