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

			virtual void read ( backend_path const & loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts ) = 0;

			virtual void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) const = 0;

			virtual void resize ( backend_path const & loc, index_type const & nsamp ) = 0;

			virtual void update_range ( backend_path const & loc, time_type const & start, time_type const & stop ) = 0;

			virtual void link ( backend_path const & loc, link_type type, std::string const & path ) const = 0;

			virtual void wipe ( backend_path const & loc ) const = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double * data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double const * data ) = 0;

			virtual void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char ** data ) const = 0;
			virtual void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char * const * data ) = 0;

			virtual std::string dict_meta () const = 0;
			virtual std::string schema_meta () const = 0;
	};


	// HDF5 backend class

	class group_backend_hdf5 : public group_backend {

		public :
			
			group_backend_hdf5 ();
			~group_backend_hdf5 ();
			group_backend_hdf5 ( group_backend_hdf5 const & other );
			group_backend_hdf5 & operator= ( group_backend_hdf5 const & other );

			void read ( backend_path const & loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts );
			void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) const;

			void resize ( backend_path const & loc, index_type const & nsamp );
			void update_range ( backend_path const & loc, time_type const & start, time_type const & stop );

			void link ( backend_path const & loc, link_type type, std::string const & path ) const;
			void wipe ( backend_path const & loc ) const;

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char ** data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char * const * data );

			std::string dict_meta () const;
			std::string schema_meta () const;

	};


	// GetData backend class

	class group_backend_getdata : public group_backend {

		public :
			
			group_backend_getdata ();
			~group_backend_getdata ();
			group_backend_getdata ( group_backend_getdata const & other );
			group_backend_getdata & operator= ( group_backend_getdata const & other );

			void read ( backend_path const & loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts );
			void write ( backend_path const & loc, index_type const & nsamp, std::map < data_type, size_t > const & counts ) const;

			void resize ( backend_path const & loc, index_type const & nsamp );
			void update_range ( backend_path const & loc, time_type const & start, time_type const & stop );

			void link ( backend_path const & loc, link_type type, std::string const & path ) const;
			void wipe ( backend_path const & loc ) const;

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int8_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint8_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int16_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint16_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int32_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint32_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, int64_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, uint64_t const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, float const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double * data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, double const * data );

			void read_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char ** data ) const;
			void write_field ( backend_path const & loc, std::string const & field_name, size_t type_indx, index_type offset, index_type ndata, char * const * data );

			std::string dict_meta () const;
			std::string schema_meta () const;

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

			void relocate ( backend_path const & loc );

			void sync ();

			void flush () const;

			void copy ( group const & other, std::string const & filter, backend_path const & loc );

			/// Create a link at the specified location.
			void link ( link_type const & type, std::string const & path ) const;

			/// Delete the on-disk data and metadata associated with this object.
			/// In-memory metadata is not modified.
			void wipe () const;

			backend_path location () const;

			// data ops

			dict const & dictionary () const;

			schema const & schema_get () const;

			index_type size () const;

			void resize ( index_type const & newsize );

			void range ( time_type & start, time_type & stop ) const;

			template < class T >
			void read_field ( std::string const & field_name, index_type offset, index_type ndata, T * data ) const {
				field check = schm_.field_get ( field_name );
				if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
					std::ostringstream o;
					o << "cannot read non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
					TIDAS_THROW( o.str().c_str() );
				}
				if ( offset + ndata > size_ ) {
					std::ostringstream o;
					o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+ndata-1) << " from group " << loc_.name << " (" << size_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				if ( loc_.type != backend_type::none ) {
					backend_->read_field ( loc_, field_name, type_indx_.at( field_name ), offset, ndata, data );
				} else {
					TIDAS_THROW( "cannot read field- backend not assigned" );
				}
				return;
			}

			template < class T >
			void write_field ( std::string const & field_name, index_type offset, index_type ndata, T const * data ) {
				field check = schm_.field_get ( field_name );
				if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
					std::ostringstream o;
					o << "cannot write non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
					TIDAS_THROW( o.str().c_str() );
				}
				if ( offset + ndata > size_ ) {
					std::ostringstream o;
					o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+ndata-1) << " to group " << loc_.name << " (" << size_ << " samples)";
					TIDAS_THROW( o.str().c_str() );
				}
				if ( loc_.type != backend_type::none ) {
					backend_->write_field ( loc_, field_name, type_indx_.at( field_name ), offset, ndata, data );
				} else {
					TIDAS_THROW( "cannot write field- backend not assigned" );
				}
				return;
			}

			// wrapper for std::vector

			template < class T >
			void read_field ( std::string const & field_name, index_type offset, std::vector < T > & data ) const {
				read_field ( field_name, offset, data.size(), data.data() );
				return;
			}

			template < class T >
			void write_field ( std::string const & field_name, index_type offset, std::vector < T > const & data ) {
				write_field ( field_name, offset, data.size(), data.data() );
				return;
			}

			// specialization for strings

			void read_field ( std::string const & field_name, index_type offset, index_type ndata, char ** data ) const;

			void write_field ( std::string const & field_name, index_type offset, index_type ndata, char * const * data );

			// wrapper for vector of strings

			void read_field ( std::string const & field_name, index_type offset, std::vector < std::string > & data ) const;

			void write_field ( std::string const & field_name, index_type offset, std::vector < std::string > const & data );

			// overload for time_type which updates range

			void write_field ( std::string const & field_name, index_type offset, index_type ndata, time_type const * data );

			void write_field ( std::string const & field_name, index_type offset, std::vector < time_type > const & data );

			// read / write the full time stamp vector

			void read_times ( index_type ndata, time_type * data ) const;

			void read_times ( std::vector < time_type > & data ) const;

			void write_times ( index_type ndata, time_type const * data );

			void write_times ( std::vector < time_type > const & data );



		private :

			void set_backend ();

			void compute_counts();

			void dict_loc ( backend_path & dloc );

			void schema_loc ( backend_path & sloc );

			schema schm_;
			dict dict_;
			index_type size_;

			time_type start_;
			time_type stop_;

			std::map < data_type, size_t > counts_;
			std::map < std::string, size_t > type_indx_;

			backend_path loc_;
			std::unique_ptr < group_backend > backend_;

	};


}


#endif
