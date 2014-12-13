/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_SCHEMA_HPP
#define TIDAS_SCHEMA_HPP


namespace tidas {

	// one field of the schema

	class field {

		public :

			field ();
			field ( std::string const & name, data_type type, std::string const & units );

			bool operator== ( const field & other ) const;
			bool operator!= ( const field & other ) const;

			data_type type;
			std::string name;
			std::string units;

	};

	typedef std::vector < field > field_list;

	field_list field_filter_type ( field_list const & fields, data_type type );


	// base class for schema backend interface

	class schema_backend {

		public :
			
			schema_backend () {}
			virtual ~schema_backend () {}

			virtual schema_backend * clone () = 0;

			virtual void read ( backend_path const & loc, field_list & fields ) const = 0;

			virtual void write ( backend_path const & loc, link_info const & link, field_list const & fields ) = 0;

	};


	// HDF5 backend class

	class schema_backend_hdf5 : public schema_backend {

		public :
			
			schema_backend_hdf5 ();
			~schema_backend_hdf5 ();
			schema_backend_hdf5 ( schema_backend_hdf5 const & other );
			schema_backend_hdf5 & operator= ( schema_backend_hdf5 const & other );

			schema_backend * clone ();

			void read ( backend_path const & loc, field_list & fields ) const;

			void write ( backend_path const & loc, link_info const & link, field_list const & fields );

	};


	// GetData backend class

	class schema_backend_getdata : public schema_backend {

		public :
			
			schema_backend_getdata ();
			~schema_backend_getdata ();
			schema_backend_getdata ( schema_backend_getdata const & other );
			schema_backend_getdata & operator= ( schema_backend_getdata const & other );

			schema_backend * clone ();

			void read ( backend_path const & loc, field_list & fields ) const;

			void write ( backend_path const & loc, link_info const & link, field_list const & fields );

	};


	// a schema used for a group

	class schema {

		public :

			schema ();
			schema ( field_list const & fields );

			~schema ();
			schema & operator= ( schema const & other );

			schema ( schema const & other );
			schema ( backend_path const & loc );
			schema ( schema const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void set_backend ( backend_path const & loc, std::unique_ptr < schema_backend > & backend );

			void relocate ( backend_path const & loc );

			void sync () const;

			void flush ();

			void copy ( schema const & other, std::string const & filter, backend_path const & loc );

			void duplicate ( backend_path const & loc );

			backend_path location () const;

			// data ops

			void clear ();
			
			void append ( field const & fld );
			
			void remove ( std::string const & name );
			
			field seek ( std::string const & name ) const;
			
			field_list const & fields () const;

		private :

			field_list fields_;

			backend_path loc_;
			std::unique_ptr < schema_backend > backend_;

	};

}


#endif
