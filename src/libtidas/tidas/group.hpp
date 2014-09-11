/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_GROUP_HPP
#define TIDAS_GROUP_HPP


namespace tidas {

	static const std::string time_field = "TIDAS_TIME";

	typedef std::pair < index_type, index_type > span;

	// base class for group backend interface.  Rather than trying to mix inheritance and 
	// virtual methods that can read / write data of supported types, we use a more C-like
	// interface that simply specifies the type, number of samples, and a void* to point
	// to the data.  These classes are only used internally, so we have complete control
	// over calls to this interface.

	class group_backend {

		public :
			
			group_backend ();
			virtual ~group_backend ();

			virtual group_backend * clone () = 0;

			virtual void read ( backend_path const & loc, schema & schm, index_type & nsamp ) = 0;

			virtual void write ( backend_path const & loc, schema const & schm, index_type nsamp ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data ) = 0;

			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data ) = 0;

	};


	// memory backend class

	class group_backend_mem : public group_backend {

		public :
			
			group_backend_mem ( index_type nsamp, schema const & schm );
			~group_backend_mem ();

			group_backend_mem * clone ();

			void read ( backend_path const & loc, schema & schm, index_type & nsamp );

			void write ( backend_path const & loc, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

		private :

			schema schm_;
			index_type nsamp_;
			std::map < std::string, std::vector < double > > data_float_;
			std::map < std::string, std::vector < int64_t > > data_int_;

	};


	// HDF5 backend class

	class group_backend_hdf5 : public group_backend {

		public :
			
			group_backend_hdf5 ();
			~group_backend_hdf5 ();

			group_backend_hdf5 * clone ();

			void read ( backend_path const & loc, schema & schm, index_type & nsamp );

			void write ( backend_path const & loc, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

	};


	// GetData backend class

	class group_backend_getdata : public group_backend {

		public :
			
			group_backend_getdata ();
			~group_backend_getdata ();

			group_backend_getdata * clone ();

			void read ( backend_path const & loc, schema & schm, index_type & nsamp );

			void write ( backend_path const & loc, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data );

	};


	// group of data streams which are sampled synchronously

	class group {

		public :

			group ();
			group ( schema const & schm, index_type nsamp );
			group ( backend_path const & loc );
			group ( group const & orig );
			~group ();

			void read_meta ();

			void write_meta ();

			void relocate ( backend_path const & loc );

			backend_path location ();

			void duplicate ( backend_path const & newloc );

			schema const & schema_get () const;

			index_type nsamp () const;

			void time_cache ();

			void time_flush ();

			std::vector < time_type > const & times();

			intrvl range_time ( span const & spn );

			span range_index_inner ( intrvl const & intv );

			span range_index_outer ( intrvl const & intv );

			template < class T >
			void read_field ( std::string const & field_name, index_type offset, std::vector < T > & data ) {
				index_type n = data.size();
				if ( offset + n > nsamp_ ) {
					std::ostringstream o;
					o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " from group " << loc_.name << " (" << nsamp_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				T testval;
				data_type type = data_type_get ( typeid ( testval ) );
				backend_->read_field ( loc_, field_name, offset, n, type, static_cast < void* > ( &(data[0]) ) );
				return;
			}

			template < class T >
			void write_field ( std::string const & field_name, index_type offset, std::vector < T > const & data ) {
				index_type n = data.size();
				if ( offset + n > nsamp_ ) {
					std::ostringstream o;
					o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " to group " << loc_.name << " (" << nsamp_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				T testval;
				data_type type = data_type_get ( typeid ( testval ) );
				backend_->write_field ( loc_, field_name, offset, n, type, static_cast < void* > ( &(data[0]) ) );
				return;
			}

		private :

			schema schm_;
			index_type nsamp_;
			std::vector < time_type > times_;

			backend_path loc_;
			group_backend * backend_;

	};


}


#endif
