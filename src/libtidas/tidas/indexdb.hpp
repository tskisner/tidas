/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INDEXDB_HPP
#define TIDAS_INDEXDB_HPP


namespace tidas {


	enum class indexdb_op : unsigned short {
		add,
		del,
		update
	};

	enum class indexdb_object_type : unsigned short {
		dict,
		schema,
		group,
		intervals,
		block
	};


	class indexdb_object {

		public :

			virtual indexdb_object * clone () = 0;

			indexdb_object_type type;
			std::string path;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( CEREAL_NVP( type ) );
				ar ( CEREAL_NVP( path ) );
				return;
			}

	};


	class indexdb_dict : public indexdb_object {

		public :

			indexdb_object * clone ();

			std::map < std::string, std::string > data;
			std::map < std::string, data_type > types;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( data ) );
				ar ( CEREAL_NVP( types ) );
				return;
			}
	
	};


	class indexdb_schema : public indexdb_object {

		public :

			indexdb_object * clone ();

			field_list fields;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::virtual_base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( fields ) );
				return;
			}

	};


	class indexdb_group : public indexdb_object {

		public :

			indexdb_object * clone ();

			index_type nsamp;
			std::map < data_type, size_t > counts;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::virtual_base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( nsamp ) );
				ar ( CEREAL_NVP( counts ) );
				return;
			}

	};


	class indexdb_intervals : public indexdb_object {

		public :

			indexdb_object * clone ();

			size_t size;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::virtual_base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( size ) );
				return;
			}

	};


	class indexdb_block : public indexdb_object {

		public :

			indexdb_object * clone ();

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::virtual_base_class < indexdb_object > ( this ) );
				return;
			}

	};


	class indexdb_transaction {

		public :

			indexdb_transaction ();
			
			~indexdb_transaction ();

			indexdb_transaction & operator= ( indexdb_transaction const & other );

			indexdb_transaction ( indexdb_transaction const & other );

			void copy ( indexdb_transaction const & other );

			indexdb_op op;
			std::unique_ptr < indexdb_object > obj;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( CEREAL_NVP( op ) );
				ar ( CEREAL_NVP( obj ) );
				return;
			}

	};


	// class for object index

	class indexdb {

		public :

			indexdb ();
			~indexdb ();
			indexdb & operator= ( indexdb const & other );

			indexdb ( indexdb const & other );

			indexdb ( std::string const & path );

			void copy ( indexdb const & other );

			/*
			void add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );
			void del_dict ( backend_path loc );
			bool query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) const;

			void add_schema ( backend_path loc, field_list const & fields );
			void del_schema ( backend_path loc );
			bool query_schema ( backend_path loc, field_list & fields ) const;

			void add_group ( backend_path loc, index_type const & nsamp, std::map < data_type, size_t > const & counts );
			void del_group ( backend_path loc );
			bool query_group ( backend_path loc, index_type & nsamp, std::map < data_type, size_t > & counts ) const;

			void add_intervals ( backend_path loc, size_t const & size );
			void del_intervals ( backend_path loc );
			bool query_intervals ( backend_path loc, size_t & size ) const;

			void add_block ( backend_path loc );
			void del_block ( backend_path loc );
			bool query_block ( backend_path loc ) const;

			std::deque < indexdb_transaction > const & history () const;
			void history_clear();
			void replay ( std::deque < indexdb_transaction > const & trans );
			*/

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( CEREAL_NVP( path_ ) );
				ar ( CEREAL_NVP( history_ ) );
				return;
			}

		private :

			std::string path_;
			std::deque < indexdb_transaction > history_;

	};


}


#endif
