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

			virtual void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) = 0;

			virtual void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) = 0;

	};

	// memory backend class

	class dict_backend_mem : public dict_backend {

		public :
			
			dict_backend_mem ();
			~dict_backend_mem ();
			dict_backend_mem ( dict_backend_mem const & other );
			dict_backend_mem & operator= ( dict_backend_mem const & other );

			dict_backend * clone ();

			void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

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

			void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

	};


	// GetData backend class

	class dict_backend_getdata : public dict_backend {

		public :
			
			dict_backend_getdata ();
			~dict_backend_getdata ();
			dict_backend_getdata ( dict_backend_getdata const & other );
			dict_backend_getdata & operator= ( dict_backend_getdata const & other );

			dict_backend * clone ();

			void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

	};


	/// Dictionary class.  This class stores elements by name, and explicitly 
	/// tracks the type of the assigned value. 

	class dict {

		public :

			dict ();
			~dict ();
			dict & operator= ( dict const & other );

			dict ( dict const & other );
			dict ( backend_path const & loc );
			dict ( dict const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void set_backend ( backend_path const & loc, std::unique_ptr < dict_backend > & backend );

			void relocate ( backend_path const & loc );

			void sync ();

			void flush ();

			void copy ( dict const & other, std::string const & filter, backend_path const & loc );

			void duplicate ( backend_path const & loc );

			backend_path location () const;

			// data ops

			/// Insert a value into the dictionary. 
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

			/// Return the raw string value stored in the specified key.
			std::string get_string ( std::string const & key ) const;

			/// Return the value of the specified key as a double precision float.
			double get_double ( std::string const & key ) const;

			/// Return the value of the specified key as a native integer.
			int get_int ( std::string const & key ) const;

			/// Return the value of the specified key as a long long integer.
			long long get_ll ( std::string const & key ) const;

			/// Clear all elements of the dictionary
			void clear();

			/// Return a const reference to the underlying data map.
			std::map < std::string, std::string > const & data() const;

			/// Return a const reference to the underlying data type map.
			std::map < std::string, data_type > const & types() const;

		private :

			std::map < std::string, std::string > data_;
			std::map < std::string, data_type > types_;

			backend_path loc_;
			std::unique_ptr < dict_backend > backend_;

	};


}


#endif
