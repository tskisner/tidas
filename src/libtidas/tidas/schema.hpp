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
			~field ();

			data_type type;
			std::string name;
			std::string units;

	};

	typedef std::vector < field > field_list;


	// a schema used for a group

	class schema {

		public :

			schema ();
			schema ( field_list const & fields );
			schema ( schema const & orig );
			~schema ();

			void append ( field const & fld );
			void remove ( std::string const & name );
			field seek ( std::string const & name );
			field_list fields ();

		private :
			field_list fields_;

	};

}


#endif
