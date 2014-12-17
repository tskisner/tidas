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

			virtual void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) = 0;

			virtual void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) const = 0;

	};

	// HDF5 backend class

	class dict_backend_hdf5 : public dict_backend {

		public :
			
			dict_backend_hdf5 ();
			~dict_backend_hdf5 ();
			dict_backend_hdf5 ( dict_backend_hdf5 const & other );
			dict_backend_hdf5 & operator= ( dict_backend_hdf5 const & other );

			void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) const;

	};


	// GetData backend class

	class dict_backend_getdata : public dict_backend {

		public :
			
			dict_backend_getdata ();
			~dict_backend_getdata ();
			dict_backend_getdata ( dict_backend_getdata const & other );
			dict_backend_getdata & operator= ( dict_backend_getdata const & other );

			void read ( backend_path const & loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void write ( backend_path const & loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) const;

	};


	/// Dictionary class.  
	/// This class stores elements by name, and explicitly 
	/// tracks the type of the assigned value. 

	class dict {

		public :

			/// Default constructor.  Backend is unassigned.
			dict ();
			~dict ();
			dict & operator= ( dict const & other );

			/// Copy constructor.  
			/// All members and backend location are copied.
			dict ( dict const & other );

			/// Load the dictionary from an existing location.  
			/// All meta data operations will apply to this location.
			dict ( backend_path const & loc );

			/// Copy from an existing dictionary.  
			/// Apply an optional filter to elements and relocate to a new 
			/// location.  If a filter is given, a new location must be specified.
			dict ( dict const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void relocate ( backend_path const & loc );

			/// Reload metadata from the current location, overwriting the current state in memory.
			void sync ();

			/// Write metadata to the current location, overwriting the information at that location.
			void flush () const;

			void copy ( dict const & other, std::string const & filter, backend_path const & loc );

			/// The current location.
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

			/// Return a value from the dictionary. 
			template < class T >
			T get ( std::string const & key ) const {
				return data_convert < T > ( data_.at( key ) );
			}

			/// Clear all elements of the dictionary.
			void clear();

			/// Return a const reference to the underlying data map.
			std::map < std::string, std::string > const & data() const;

			/// Return a const reference to the underlying data type map.
			std::map < std::string, data_type > const & types() const;

		private :

			void set_backend ();

			std::map < std::string, std::string > data_;
			std::map < std::string, data_type > types_;

			backend_path loc_;
			std::unique_ptr < dict_backend > backend_;

	};


}


#endif
