/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INTERVALS_HPP
#define TIDAS_INTERVALS_HPP


namespace tidas {

	typedef std::pair < time_type, time_type > intrvl;

	typedef std::vector < intrvl > interval_list;


	// base class for intervals backend interface

	class intervals_backend {

		public :
			
			intervals_backend ();
			virtual ~intervals_backend ();

			virtual intervals_backend * clone () = 0;

			virtual void read ( backend_path const & loc, interval_list & intr ) = 0;
			virtual void write ( backend_path const & loc, interval_list const & intr ) = 0;

	};


	// memory backend class

	class intervals_backend_mem : public intervals_backend {

		public :
			
			intervals_backend_mem ();
			~intervals_backend_mem ();

			intervals_backend_mem * clone ();

			void read ( backend_path const & loc, interval_list & intr );
			void write ( backend_path const & loc, interval_list const & intr );

		private :

			interval_list store_;

	};


	// HDF5 backend class

	class intervals_backend_hdf5 : public intervals_backend {

		public :
			
			intervals_backend_hdf5 ();
			~intervals_backend_hdf5 ();

			intervals_backend_hdf5 * clone ();

			void read ( backend_path const & loc, interval_list & intr );
			void write ( backend_path const & loc, interval_list const & intr );

	};


	// GetData backend class

	class intervals_backend_getdata : public intervals_backend {

		public :
			
			intervals_backend_getdata ();
			~intervals_backend_getdata ();

			intervals_backend_getdata * clone ();

			void read ( backend_path const & loc, interval_list & intr );
			void write ( backend_path const & loc, interval_list const & intr );

	};


	// a collection of intervals

	class intervals {

		public :

			intervals ();
			intervals ( backend_path const & loc );
			intervals ( intervals const & orig );
			~intervals ();

			void clear ();
			void append ( intrvl const & intr );

			void read ();
			void write ();
			void relocate ( backend_path const & loc );
			backend_path location ();

			intrvl const & get ( size_t indx ) const;

			intrvl seek ( time_type time ) const;

			intrvl seek_ceil ( time_type time ) const;
			
			intrvl seek_floor ( time_type time ) const;

		private :
			interval_list data_;

			backend_path loc_;
			intervals_backend * backend_;

	};

}


#endif
