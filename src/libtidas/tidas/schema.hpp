/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_SCHEMA_HPP
#define TIDAS_SCHEMA_HPP


namespace tidas {

	static const std::string schema_meta = "schema";

	// one field of the schema

	class field {

		public :

			field ();

			bool operator== ( const field & other ) const;
			bool operator!= ( const field & other ) const;

			data_type type;
			std::string name;
			std::string units;

	};

	typedef std::vector < field > field_list;


	// base class for schema backend interface

	class schema_backend {

		public :
			
			schema_backend () {}
			virtual ~schema_backend () {}

			virtual schema_backend * clone () = 0;

			virtual void read_meta ( backend_path const & loc, std::string const & filter, field_list & fields ) = 0;

			virtual void write_meta ( backend_path const & loc, std::string const & filter, field_list const & fields ) = 0;

	};

	// memory backend class

	class schema_backend_mem : public schema_backend {

		public :
			
			schema_backend_mem ();
			~schema_backend_mem ();

			schema_backend_mem * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, field_list & fields );

			void write_meta ( backend_path const & loc, std::string const & filter, field_list const & fields );

		private :

			field_list fields_;

	};

	// HDF5 backend class

	class schema_backend_hdf5 : public schema_backend {

		public :
			
			schema_backend_hdf5 ();
			~schema_backend_hdf5 ();

			schema_backend_hdf5 * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, field_list & fields );

			void write_meta ( backend_path const & loc, std::string const & filter, field_list const & fields );

	};


	// GetData backend class

	class schema_backend_getdata : public schema_backend {

		public :
			
			schema_backend_getdata ();
			~schema_backend_getdata ();

			schema_backend_getdata * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, field_list & fields );

			void write_meta ( backend_path const & loc, std::string const & filter, field_list const & fields );

	};


	// a schema used for a group

	class schema {

		public :

			schema ();
			~schema ();
			schema ( schema const & other );
			schema & operator= ( schema const & other );
			void init ();
			void copy ( schema const & other );
			void clear ();

			schema ( backend_path const & loc, std::string const & filter );

			schema ( field_list const & fields );

			void read_meta ( std::string const & filter );

			void write_meta ( std::string const & filter );

			void relocate ( backend_path const & loc );

			backend_path location () const;

			schema duplicate ( std::string const & filter, backend_path const & newloc );

			//------------
			
			void append ( field const & fld );
			
			void remove ( std::string const & name );
			
			field seek ( std::string const & name ) const;
			
			field_list fields () const;

		private :

			field_list fields_;

			backend_path loc_;
			dict_backend * backend_;

	};

}


#endif
