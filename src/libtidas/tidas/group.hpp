/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_GROUP_HPP
#define TIDAS_GROUP_HPP


namespace tidas {

	static const std::string group_time_field = "TIDAS_TIME";

	static const std::string group_meta = "data";

	// base class for group backend interface.  Rather than trying to mix inheritance and 
	// virtual methods that can read / write data of supported types, we use a more C-like
	// interface that simply specifies the type, number of samples, and a void* to point
	// to the data.  These classes are only used internally, so we have complete control
	// over calls to this interface.

	class group_backend {

		public :
			
			group_backend () {}
			virtual ~group_backend () {}

			virtual group_backend * clone () = 0;

			virtual void read_meta ( backend_path const & loc, std::string const & filter, index_type & nsamp ) = 0;

			virtual void write_meta ( backend_path const & loc, std::string const & filter, schema const & schm, index_type nsamp ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data ) = 0;
	};


	// memory backend class

	class group_backend_mem : public group_backend {

		public :
			
			group_backend_mem ( index_type nsamp );
			~group_backend_mem ();
			group_backend_mem ( group_backend_mem const & other );
			group_backend_mem & operator= ( group_backend_mem const & other );
			void copy ( group_backend_mem const & other );

			group_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, index_type & nsamp );

			void write_meta ( backend_path const & loc, std::string const & filter, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data );

		private :

			index_type nsamp_;
			std::map < std::string, std::vector < int8_t > > data_int8_;
			std::map < std::string, std::vector < uint8_t > > data_uint8_;
			std::map < std::string, std::vector < int16_t > > data_int16_;
			std::map < std::string, std::vector < uint16_t > > data_uint16_;
			std::map < std::string, std::vector < int32_t > > data_int32_;
			std::map < std::string, std::vector < uint32_t > > data_uint32_;
			std::map < std::string, std::vector < int64_t > > data_int64_;
			std::map < std::string, std::vector < uint64_t > > data_uint64_;
			std::map < std::string, std::vector < float > > data_float_;
			std::map < std::string, std::vector < double > > data_double_;

	};


	// HDF5 backend class

	class group_backend_hdf5 : public group_backend {

		public :
			
			group_backend_hdf5 ();
			~group_backend_hdf5 ();
			group_backend_hdf5 ( group_backend_hdf5 const & other );
			group_backend_hdf5 & operator= ( group_backend_hdf5 const & other );

			group_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, index_type & nsamp );

			void write_meta ( backend_path const & loc, std::string const & filter, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data );

	};


	// GetData backend class

	class group_backend_getdata : public group_backend {

		public :
			
			group_backend_getdata ();
			~group_backend_getdata ();
			group_backend_getdata ( group_backend_getdata const & other );
			group_backend_getdata & operator= ( group_backend_getdata const & other );

			group_backend * clone ();

			void read_meta ( backend_path const & loc, std::string const & filter, index_type & nsamp );

			void write_meta ( backend_path const & loc, std::string const & filter, schema const & schm, index_type nsamp );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data );

	};


	// group of data streams which are sampled synchronously

	class group {

		public :

			group ();
			~group ();
			group ( group const & other );
			group & operator= ( group const & other );
			void copy ( group const & other );

			group ( backend_path const & loc, std::string const & filter );

			group ( schema const & schm, index_type nsamp );
			group ( schema const & schm, dict const & dictionary, index_type nsamp );

			void read_meta ( std::string const & filter );

			void write_meta ( std::string const & filter );

			void relocate ( backend_path const & loc );

			backend_path location () const;

			group duplicate ( std::string const & filter, backend_path const & newloc, interval_list const & intr );

			//------------

			dict & dictionary () const;

			schema const & schema_get () const;

			index_type nsamp () const;

			time_type start ();

			time_type stop ();

			void read_times ( std::vector < time_type > & data );

			void write_times ( std::vector < time_type > const & data );

			template < class T >
			void read_field ( std::string const & field_name, index_type offset, std::vector < T > & data ) {
				field check = schm_.seek ( field_name );
				if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
					std::ostringstream o;
					o << "cannot read non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
					TIDAS_THROW( o.str().c_str() );
				}
				index_type n = data.size();
				if ( offset + n > nsamp_ ) {
					std::ostringstream o;
					o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " from group " << loc_.name << " (" << nsamp_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				backend_->read_field ( loc_, field_name, offset, data );
				return;
			}

			template < class T >
			void write_field ( std::string const & field_name, index_type offset, std::vector < T > const & data ) {
				field check = schm_.seek ( field_name );
				if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
					std::ostringstream o;
					o << "cannot write non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
					TIDAS_THROW( o.str().c_str() );
				}
				index_type n = data.size();
				if ( offset + n > nsamp_ ) {
					std::ostringstream o;
					o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " to group " << loc_.name << " (" << nsamp_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				backend_->write_field ( loc_, field_name, offset, data );
				return;
			}

		private :

			schema schm_;
			dict dict_;
			index_type nsamp_;

			backend_path loc_;
			std::unique_ptr < group_backend > backend_;

	};


}


#endif
