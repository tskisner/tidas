/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INDEXDB_HPP
#define TIDAS_INDEXDB_HPP


extern "C" {
#include <tidas/sqlite3.h>
}


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
			
			virtual ~indexdb_object ();

			virtual indexdb_object * clone () const = 0;

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

			indexdb_object * clone () const;

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

			indexdb_object * clone () const;

			field_list fields;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( fields ) );
				return;
			}

	};


	class indexdb_group : public indexdb_object {

		public :

			indexdb_object * clone () const;

			index_type nsamp;
			time_type start;
			time_type stop;
			std::map < data_type, size_t > counts;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( nsamp ) );
				ar ( CEREAL_NVP( start ) );
				ar ( CEREAL_NVP( stop ) );
				ar ( CEREAL_NVP( counts ) );
				return;
			}

	};


	class indexdb_intervals : public indexdb_object {

		public :

			indexdb_object * clone () const;

			size_t size;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::base_class < indexdb_object > ( this ) );
				ar ( CEREAL_NVP( size ) );
				return;
			}

	};


	class indexdb_block : public indexdb_object {

		public :

			indexdb_object * clone () const;

			template < class Archive >
			void serialize ( Archive & ar ) {
				ar ( cereal::base_class < indexdb_object > ( this ) );
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


	// utility for extracting dirname / basename

	void indexdb_path_split ( std::string const & in, std::string & dir, std::string & base );


	// base class for indices

	class indexdb {

		public :

			virtual ~indexdb ();

			virtual void add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) = 0;
			virtual void update_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) = 0;
			virtual void del_dict ( backend_path loc ) = 0;
			virtual bool query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) = 0;

			virtual void add_schema ( backend_path loc, field_list const & fields ) = 0;
			virtual void update_schema ( backend_path loc, field_list const & fields ) = 0;
			virtual void del_schema ( backend_path loc ) = 0;
			virtual bool query_schema ( backend_path loc, field_list & fields ) = 0;

			virtual void add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts ) = 0;
			virtual void update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts ) = 0;
			virtual void del_group ( backend_path loc ) = 0;
			virtual bool query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts ) = 0;

			virtual void add_intervals ( backend_path loc, size_t const & size ) = 0;
			virtual void update_intervals ( backend_path loc, size_t const & size ) = 0;
			virtual void del_intervals ( backend_path loc ) = 0;
			virtual bool query_intervals ( backend_path loc, size_t & size ) = 0;

			virtual void add_block ( backend_path loc ) = 0;
			virtual void update_block ( backend_path loc ) = 0;
			virtual void del_block ( backend_path loc ) = 0;
			virtual bool query_block ( backend_path loc, std::vector < std::string > & child_blocks, std::vector < std::string > & child_groups, std::vector < std::string > & child_intervals ) = 0;

			template < class Archive >
			void save ( Archive & ar ) const {
				return;
			}

			template < class Archive >
			void load ( Archive & ar ) {
				return;
			}

	};


	// class for SQLite object index

	class indexdb_sql : public indexdb {

		public :

			indexdb_sql ();
			~indexdb_sql ();
			indexdb_sql & operator= ( indexdb_sql const & other );
			indexdb_sql ( indexdb_sql const & other );
			indexdb_sql ( std::string const & path, std::string const & volpath, access_mode mode );

			void copy ( indexdb_sql const & other );

			void add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );
			void update_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );
			void del_dict ( backend_path loc );
			bool query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void add_schema ( backend_path loc, field_list const & fields );
			void update_schema ( backend_path loc, field_list const & fields );
			void del_schema ( backend_path loc );
			bool query_schema ( backend_path loc, field_list & fields );

			void add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );
			void update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );
			void del_group ( backend_path loc );
			bool query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts );

			void add_intervals ( backend_path loc, size_t const & size );
			void update_intervals ( backend_path loc, size_t const & size );
			void del_intervals ( backend_path loc );
			bool query_intervals ( backend_path loc, size_t & size );

			void add_block ( backend_path loc );
			void update_block ( backend_path loc );
			void del_block ( backend_path loc );
			bool query_block ( backend_path loc, std::vector < std::string > & child_blocks, std::vector < std::string > & child_groups, std::vector < std::string > & child_intervals );
			
			void commit ( std::deque < indexdb_transaction > const & trans );

			void tree ( backend_path root, std::string const & filter, std::deque < indexdb_transaction > & result );

			template < class Archive >
			void save ( Archive & ar ) const {
				ar ( cereal::base_class < indexdb > ( this ) );
				ar ( CEREAL_NVP( path_ ) );
				ar ( CEREAL_NVP( volpath_ ) );
				ar ( CEREAL_NVP( mode_ ) );
				return;
			}

			template < class Archive >
			void load ( Archive & ar ) {
				ar ( cereal::base_class < indexdb > ( this ) );
				ar ( CEREAL_NVP( path_ ) );
				ar ( CEREAL_NVP( volpath_ ) );
				ar ( CEREAL_NVP( mode_ ) );
				sql_ = NULL;
				open();
				return;
			}

		private :

			void ops_dict ( backend_path loc, indexdb_op op, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

			void ops_schema ( backend_path loc, indexdb_op op, field_list const & fields );

			void ops_group ( backend_path loc, indexdb_op op, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );

			void ops_intervals ( backend_path loc, indexdb_op op, size_t const & size );

			void ops_block ( backend_path loc, indexdb_op op );

			std::string dbpath ( std::string const & fullpath );

			void open ();
			void close ();
			void init ( std::string const & path );
			void sql_err ( bool err, char const * msg, char const * file, int line );

			std::string path_;
			std::string volpath_;
			access_mode mode_;

			sqlite3 * sql_;

	};


	// class for object index in memory

	class indexdb_mem : public indexdb {

		public :

			indexdb_mem ();
			~indexdb_mem ();
			indexdb_mem & operator= ( indexdb_mem const & other );
			indexdb_mem ( indexdb_mem const & other );

			void copy ( indexdb_mem const & other );

			void add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );
			void update_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );
			void del_dict ( backend_path loc );			
			bool query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types );

			void add_schema ( backend_path loc, field_list const & fields );
			void update_schema ( backend_path loc, field_list const & fields );
			void del_schema ( backend_path loc );			
			bool query_schema ( backend_path loc, field_list & fields );

			void add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );
			void update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );
			void del_group ( backend_path loc );			
			bool query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts );

			void add_intervals ( backend_path loc, size_t const & size );
			void update_intervals ( backend_path loc, size_t const & size );
			void del_intervals ( backend_path loc );			
			bool query_intervals ( backend_path loc, size_t & size );

			void add_block ( backend_path loc );
			void update_block ( backend_path loc );
			void del_block ( backend_path loc );
			bool query_block ( backend_path loc, std::vector < std::string > & child_blocks, std::vector < std::string > & child_groups, std::vector < std::string > & child_intervals );

			std::deque < indexdb_transaction > const & history () const;
			
			void history_clear();
			
			void replay ( std::deque < indexdb_transaction > const & trans );

			template < class Archive >
			void save ( Archive & ar ) const {
				ar ( cereal::base_class < indexdb > ( this ) );
				ar ( CEREAL_NVP( history_ ) );
				ar ( CEREAL_NVP( data_dict_ ) );
				ar ( CEREAL_NVP( data_schema_ ) );
				ar ( CEREAL_NVP( data_group_ ) );
				ar ( CEREAL_NVP( data_intervals_ ) );
				ar ( CEREAL_NVP( data_block_ ) );
				return;
			}

			template < class Archive >
			void load ( Archive & ar ) {
				ar ( cereal::base_class < indexdb > ( this ) );
				ar ( CEREAL_NVP( history_ ) );
				ar ( CEREAL_NVP( data_dict_ ) );
				ar ( CEREAL_NVP( data_schema_ ) );
				ar ( CEREAL_NVP( data_group_ ) );
				ar ( CEREAL_NVP( data_intervals_ ) );
				ar ( CEREAL_NVP( data_block_ ) );
				return;
			}

		private :

			void ops_dict ( backend_path loc, indexdb_op op, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types );

			void ops_schema ( backend_path loc, indexdb_op op, field_list const & fields );

			void ops_group ( backend_path loc, indexdb_op op, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts );

			void ops_intervals ( backend_path loc, indexdb_op op, size_t const & size );

			void ops_block ( backend_path loc, indexdb_op op );

			std::deque < indexdb_transaction > history_;

			std::map < std::string, indexdb_dict > data_dict_;
			std::map < std::string, indexdb_schema > data_schema_;
			std::map < std::string, indexdb_group > data_group_;
			std::map < std::string, indexdb_intervals > data_intervals_;
			std::map < std::string, indexdb_block > data_block_;

	};


}


#endif
