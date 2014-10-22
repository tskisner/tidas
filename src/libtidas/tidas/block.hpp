/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {


	// base class for intervals backend interface

	class block_backend {

		public :
			
			block_backend () {}
			virtual ~block_backend () {}

			virtual block_backend * clone () = 0;

			virtual void read_meta ( backend_path const & loc ) = 0;

			virtual void write_meta ( backend_path const & loc ) = 0;

	};


	// memory backend class

	class block_backend_mem : public block_backend {

		public :
			
			block_backend_mem ();
			~block_backend_mem ();
			block_backend_mem ( block_backend_mem const & other );
			block_backend_mem & operator= ( block_backend_mem const & other );

			block_backend * clone ();

			void read_meta ( backend_path const & loc );

			void write_meta ( backend_path const & loc );

		private :

			interval_list store_;

	};


	// HDF5 backend class

	class block_backend_hdf5 : public block_backend {

		public :
			
			block_backend_hdf5 ();
			~block_backend_hdf5 ();
			block_backend_hdf5 ( block_backend_hdf5 const & other );
			block_backend_hdf5 & operator= ( block_backend_hdf5 const & other );

			block_backend * clone ();

			void read_meta ( backend_path const & loc );

			void write_meta ( backend_path const & loc );

	};


	// GetData backend class

	class block_backend_getdata : public block_backend {

		public :
			
			block_backend_getdata ();
			~block_backend_getdata ();
			block_backend_getdata ( block_backend_getdata const & other );
			block_backend_getdata & operator= ( block_backend_getdata const & other );

			block_backend * clone ();

			void read_meta ( backend_path const & loc );
			
			void write_meta ( backend_path const & loc );

	};

	class block;

	class block {

		public :

			block ();
			~block ();
			block ( block const & other );
			block & operator= ( block const & other );
			void copy ( block const & other );

			block ( backend_path const & loc, std::string const & filter );

			void read_meta ( std::string const & filter );

			void write_meta ( std::string const & filter );

			void relocate ( backend_path const & loc );

			backend_path location () const;

			block duplicate ( std::string const & filter, backend_path const & newloc );

			//------------

			dict & dictionary ();

			std::vector < block > & blocks ();

			std::vector < group > & groups ();
			
			std::vector < intervals > & intervals ();


		private :

			dict dict_;
			std::vector < block > block_list_;
			std::vector < group > group_list_;
			std::vector < intervals > intervals_list_;

			backend_path loc_;
			std::unique_ptr < block_backend > backend_;

	};

}

#endif
