/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_GROUP_HPP
#define TIDAS_GROUP_HPP


namespace tidas {

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

			virtual void read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts ) = 0;
			virtual void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > const & data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > & data ) = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > const & data ) = 0;

			virtual std::string dict_meta () = 0;
			virtual std::string schema_meta () = 0;
	};


	// memory backend class

	class group_backend_mem : public group_backend {

		public :
			
			group_backend_mem ();
			~group_backend_mem ();
			group_backend_mem ( group_backend_mem const & other );
			group_backend_mem & operator= ( group_backend_mem const & other );
			void copy ( group_backend_mem const & other );

			group_backend * clone ();

			void read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts );
			void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > const & data );

			std::string dict_meta ();
			std::string schema_meta ();

			template < typename T >
			void reset_data ( std::vector < std::vector < T > > data, size_t count, size_t nsamp ) {
				data.resize ( count );
				for ( size_t i = 0; i < count; ++i ) {
					data[i].resize ( nsamp );
					data[i].clear();
				}
				return;
			}

		private :

			index_type nsamp_;
			std::map < data_type, size_t > counts_;

			std::vector < std::vector < int8_t > > data_int8_;
			std::vector < std::vector < uint8_t > > data_uint8_;
			std::vector < std::vector < int16_t > > data_int16_;
			std::vector < std::vector < uint16_t > > data_uint16_;
			std::vector < std::vector < int32_t > > data_int32_;
			std::vector < std::vector < uint32_t > > data_uint32_;
			std::vector < std::vector < int64_t > > data_int64_;
			std::vector < std::vector < uint64_t > > data_uint64_;
			std::vector < std::vector < float > > data_float_;
			std::vector < std::vector < double > > data_double_;
			std::vector < std::vector < std::string > > data_string_;

	};


	// HDF5 backend class

	class group_backend_hdf5 : public group_backend {

		public :
			
			group_backend_hdf5 ();
			~group_backend_hdf5 ();
			group_backend_hdf5 ( group_backend_hdf5 const & other );
			group_backend_hdf5 & operator= ( group_backend_hdf5 const & other );

			group_backend * clone ();

			void read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts );
			void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > const & data );

			std::string dict_meta ();
			std::string schema_meta ();

	};


	// GetData backend class

	class group_backend_getdata : public group_backend {

		public :
			
			group_backend_getdata ();
			~group_backend_getdata ();
			group_backend_getdata ( group_backend_getdata const & other );
			group_backend_getdata & operator= ( group_backend_getdata const & other );

			group_backend * clone ();

			void read ( backend_path const & loc, index_type & nsamp, std::map < data_type, size_t > & counts );
			void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint8_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint16_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint32_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < int64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < uint64_t > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < float > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < double > const & data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > & data );
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, std::vector < std::string > const & data );

			std::string dict_meta ();
			std::string schema_meta ();

	};


	// group of data streams which are sampled synchronously

	class group {

		public :

			group ();
			group ( schema const & schm, dict const & d, size_t const & size );

			~group ();
			group & operator= ( group const & other );

			group ( group const & other );
			group ( backend_path const & loc );
			group ( group const & other, std::string const & filter, backend_path const & loc );

			// metadata ops

			void read ( backend_path const & loc );

			void copy ( group const & other, std::string const & filter, backend_path const & loc );

			void write ( backend_path const & loc );

			backend_path location () const;

			// data ops

			dict const & dictionary () const;

			schema const & schema_get () const;

			index_type size () const;

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
				if ( offset + n > size_ ) {
					std::ostringstream o;
					o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " from group " << loc_.name << " (" << size_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				backend_->read_field ( loc_, field_name, type_indx_.at( field_name ), offset, data );
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
				if ( offset + n > size_ ) {
					std::ostringstream o;
					o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " to group " << loc_.name << " (" << size_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				backend_->write_field ( loc_, field_name, type_indx_.at( field_name ), offset, data );
				return;
			}

		private :

			void compute_counts();

			schema schm_;
			dict dict_;
			index_type size_;

			std::map < data_type, size_t > counts_;
			std::map < std::string, size_t > type_indx_;

			backend_path loc_;
			std::unique_ptr < group_backend > backend_;

	};


}


#endif
