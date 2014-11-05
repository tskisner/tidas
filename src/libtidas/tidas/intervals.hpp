/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INTERVALS_HPP
#define TIDAS_INTERVALS_HPP


namespace tidas {

	class intrvl {

		public :

			intrvl ();
			intrvl ( time_type new_start, time_type new_stop, index_type new_first, index_type new_last );

			bool operator== ( const intrvl & other ) const;
			bool operator!= ( const intrvl & other ) const;

			time_type start;
			time_type stop;
			index_type first;
			index_type last;

	};

	typedef std::vector < intrvl > interval_list;


	// base class for intervals backend interface

	class intervals_backend {

		public :
			
			intervals_backend () {}
			virtual ~intervals_backend () {}

			virtual intervals_backend * clone () = 0;

			virtual void read ( backend_path const & loc, size_t & size ) = 0;

			virtual void write ( backend_path const & loc, size_t const & size ) = 0;

			virtual void read_data ( backend_path const & loc, interval_list & intr ) = 0;
			
			virtual void write_data ( backend_path const & loc, interval_list const & intr ) = 0;

			virtual std::string dict_meta () = 0;

	};


	// memory backend class

	class intervals_backend_mem : public intervals_backend {

		public :
			
			intervals_backend_mem ();
			~intervals_backend_mem ();
			intervals_backend_mem ( intervals_backend_mem const & other );
			intervals_backend_mem & operator= ( intervals_backend_mem const & other );

			intervals_backend * clone ();

			void read ( backend_path const & loc, size_t & size );

			void write ( backend_path const & loc, size_t const & size );
			
			void read_data ( backend_path const & loc, interval_list & intr );
			
			void write_data ( backend_path const & loc, interval_list const & intr );

			std::string dict_meta ();

		private :

			size_t size_;
			interval_list store_;

	};


	// HDF5 backend class

	class intervals_backend_hdf5 : public intervals_backend {

		public :
			
			intervals_backend_hdf5 ();
			~intervals_backend_hdf5 ();
			intervals_backend_hdf5 ( intervals_backend_hdf5 const & other );
			intervals_backend_hdf5 & operator= ( intervals_backend_hdf5 const & other );

			intervals_backend * clone ();

			void read ( backend_path const & loc, size_t & size );

			void write ( backend_path const & loc, size_t const & size );

			void read_data ( backend_path const & loc, interval_list & intr );
			
			void write_data ( backend_path const & loc, interval_list const & intr );

			std::string dict_meta ();

	};


	// GetData backend class

	class intervals_backend_getdata : public intervals_backend {

		public :
			
			intervals_backend_getdata ();
			~intervals_backend_getdata ();
			intervals_backend_getdata ( intervals_backend_getdata const & other );
			intervals_backend_getdata & operator= ( intervals_backend_getdata const & other );

			intervals_backend * clone ();

			void read ( backend_path const & loc, size_t & size );
			
			void write ( backend_path const & loc, size_t const & size );

			void read_data ( backend_path const & loc, interval_list & intr );

			void write_data ( backend_path const & loc, interval_list const & intr );

			std::string dict_meta ();

	};


	// a collection of intervals

	class intervals {

		public :

			intervals ();
			intervals ( dict const & d, size_t const & size );

			~intervals ();
			intervals & operator= ( intervals const & other );

			intervals ( intervals const & other );
			intervals ( backend_path const & loc );
			intervals ( intervals const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void read ( backend_path const & loc );

			void copy ( intervals const & other, std::string const & filter, backend_path const & loc );

			void write ( backend_path const & loc );

			backend_path location () const;

			// data ops

			size_t size () const;

			dict const & dictionary () const;

			void read_data ( interval_list & intr );

			void write_data ( interval_list const & intr );

			static index_type total_samples ( interval_list const & intr );

			static time_type total_time ( interval_list const & intr );

			static intrvl seek ( interval_list const & intr, time_type time );

			static intrvl seek_ceil ( interval_list const & intr, time_type time );
			
			static intrvl seek_floor ( interval_list const & intr, time_type time );

		private :

			dict dict_;

			size_t size_;

			backend_path loc_;
			std::unique_ptr < intervals_backend > backend_;

	};

}


#endif
