/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_DICT_HPP
#define TIDAS_DICT_HPP


namespace tidas {

	// base class for dictionary backend interface

	class dict_backend {

		public :
			
			dict_backend () {}
			virtual ~dict_backend () {}

			virtual dict_backend * clone () = 0;

			virtual void read_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) = 0;

			virtual void write_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) = 0;

	};

	// memory backend class

	class dict_backend_mem : public dict_backend {

		public :
			
			dict_backend_mem ();
			~dict_backend_mem ();
			dict_backend_mem ( dict_backend_mem const & other );
			dict_backend_mem & operator= ( dict_backend_mem const & other );

			dict_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

		private :

			std::map < std::string, std::string > data_;
			std::map < std::string, data_type > types_;

	};

	// HDF5 backend class

	class dict_backend_hdf5 : public dict_backend {

		public :
			
			dict_backend_hdf5 ();
			~dict_backend_hdf5 ();
			dict_backend_hdf5 ( dict_backend_hdf5 const & other );
			dict_backend_hdf5 & operator= ( dict_backend_hdf5 const & other );

			dict_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

	};


	// GetData backend class

	class dict_backend_getdata : public dict_backend {

		public :
			
			dict_backend_getdata ();
			~dict_backend_getdata ();
			dict_backend_getdata ( dict_backend_getdata const & other );
			dict_backend_getdata & operator= ( dict_backend_getdata const & other );

			dict_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write_meta ( backend_path const & loc, std::string const & filter, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

	};

	// dictionary class

	class dict {

		public :

			dict ();
			~dict ();
			dict ( dict const & other );
			dict & operator= ( dict const & other );
			void copy ( dict const & other );

			dict ( backend_path const & loc, std::string const & filter );

			void read_meta ( std::string const & filter );

			void write_meta ( std::string const & filter );

			void relocate ( backend_path const & loc );

			backend_path location () const;

			dict duplicate ( std::string const & filter, backend_path const & newloc );

			//------------

			template < class T >
			void put ( std::string const & key, T const & val ) {
				std::ostringstream o;
				o.precision(18);
				o.str("");
				if ( ! ( o << val ) ) {
					TIDAS_THROW( "cannot convert type to string for dict storage" );
				}
				data_[ key ] = o.str();
				types_[ key ] = data_type_get ( typeid ( val ) );
				return;
			}

			std::string get_string ( std::string const & key ) const;

			double get_double ( std::string const & key ) const;

			int get_int ( std::string const & key ) const;

			long long get_ll ( std::string const & key ) const;

			void clear();

			std::map < std::string, std::string > const & data();

			std::map < std::string, data_type > const & types();

		private :

			std::map < std::string, std::string > data_;
			std::map < std::string, data_type > types_;

			backend_path loc_;
			std::unique_ptr < dict_backend > backend_;

	};


}


#endif
