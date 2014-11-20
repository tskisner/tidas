/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INTERVALS_HPP
#define TIDAS_INTERVALS_HPP


namespace tidas {

	/// intrvl class.
	/// This represents a single span of time and their sample indices for a
	/// particular group.

	class intrvl {

		public :

			intrvl ();
			intrvl ( time_type new_start, time_type new_stop, index_type new_first, index_type new_last );

			bool operator== ( const intrvl & other ) const;
			bool operator!= ( const intrvl & other ) const;

			/// Start time of the interval.
			time_type start;

			/// Stop time of the interval.
			time_type stop;

			/// First sample index in the interval.
			index_type first;

			/// Last sample index in the interval.
			index_type last;

	};

	/// Convenience typedef for a vector of intervals.
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


	/// Intervals class.

	class intervals {

		public :

			/// Default constructor.  The backend is set to be memory based.
			intervals ();

			/// Constructor with specified size and dictionary.
			intervals ( dict const & d, size_t const & size );

			~intervals ();
			intervals & operator= ( intervals const & other );

			/// Copy constructor.  
			/// All members and backend location are copied.
			intervals ( intervals const & other );

			/// Load the intervals from an existing location.  
			/// All meta data operations will apply to this location.
			intervals ( backend_path const & loc );
			
			/// Copy from an existing intervals instance.  
			/// Apply an optional filter to elements and relocate to a new 
			/// location.  If a filter is given, a new location must be specified.
			intervals ( intervals const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void set_backend ( backend_path const & loc, std::unique_ptr < intervals_backend > & backend );

			void relocate ( backend_path const & loc );

			/// Reload metadata from the current location, overwriting the current state in memory.
			void sync ();

			/// Write metadata to the current location, overwriting the information at that location.
			void flush ();

			void copy ( intervals const & other, std::string const & filter, backend_path const & loc );

			/// Copy all metadata to a new location.
			void duplicate ( backend_path const & loc );

			/// The current location.
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
