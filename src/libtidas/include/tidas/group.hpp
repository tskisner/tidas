/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_GROUP_HPP
#define TIDAS_GROUP_HPP


namespace tidas {

    // Base class for group backend interface.

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

    /// A group represents a collection of timestreams which are sampled
    /// consistently (they have the same number of samples).  The list of
    /// fields and their types (the schema) is set at construction time.
    /// The number of samples in the group may be altered, although this
    /// should be done with care since it may be an expensive filesystem
    /// operation.  A group has an optional dictionary associated with it
    /// at construction time.
    /// Some public methods are only used internally and are not needed for
    /// normal use of the object.  These are labelled "internal".

    class group {

        public :

            /// Default constructor.  The backend is set to be memory based.
            group ();

            /// Constructor with specified schema, dictionary, and number
            /// of samples.
            group ( schema const & schm, dict const & d, size_t const & size );

            /// Destructor
            ~group ();

            /// Assignment operator.
            group & operator= ( group const & other );

            /// Copy constructor.
            group ( group const & other );

            /// (**Internal**) Load the group from the specified location.
            /// All meta data operations will apply to this location.
            group ( backend_path const & loc );

            /// (**Internal**) Copy from an existing group.
            /// Apply an optional filter to elemetns and relocate to a new
            /// location.  If a filter is given, a new location must be
            /// specified.
            group ( group const & other, std::string const & filter,
                backend_path const & loc );

            // metadata ops

            /// (**Internal**) Change the location of the group.
            void relocate ( backend_path const & loc );

            /// (**Internal**) Reload metadata from the current location,
            /// overwriting the current state in memory.
            void sync ();

            /// (**Internal**) Write metadata to the current location,
            /// overwriting the information at that location.
            void flush () const;

            /// (**Internal**) Copy with optional selection and relocation.
            void copy ( group const & other, std::string const & filter, backend_path const & loc );

            /// (**Internal**) Create a link at the specified location.
            void link ( link_type const & type, std::string const & path ) const;

            /// (**Internal**) Delete the on-disk data and metadata associated
            /// with this object.  In-memory metadata is not modified.
            void wipe () const;

            /// (**Internal**) The current location.
            backend_path location () const;

            // data ops

            /// A (const) reference to the dictionary specified at construction.
            dict const & dictionary () const;

            /// The fixed schema specified at construction.
            schema const & schema_get () const;

            /// The number of samples currently in the group.
            index_type size () const;

            /// Change the number of samples in the group.  Depending on the
            /// backend format, this may involve expensive disk operations.
            void resize ( index_type const & newsize );

            /// The extrema values of the timestamp field.
            void range ( time_type & start, time_type & stop ) const;

            /// Explicitly check timestamps and update group metadata.
            void update_range ();

            /// Read a range of data from a field.  Store results to the bare
            /// memory address specified.
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

            /// Read a range of data from a field.  Store results to the bare
            /// memory address specified.  Rather than use templates, this
            /// casts the data pointer based on the tidas type.
            void read_field_astype ( std::string const & field_name,
                index_type offset, index_type ndata, tidas::data_type type,
                void * data ) const;

            /// Write a range of data to a field.  Data is provided at the
            /// bare memory address specified.
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

            /// Write a range of data to a field.  Data is provided at the
            /// bare memory address specified.  Rather than use templates, this
            /// casts the data pointer based on the tidas type.
            void write_field_astype ( std::string const & field_name,
                index_type offset, index_type ndata, tidas::data_type type,
                void const * data );

            // wrapper for std::vector

            /// Read a range of data from a field.  Store results in the
            /// given vector.
            template < class T >
            void read_field ( std::string const & field_name, index_type offset, std::vector < T > & data ) const {
                read_field ( field_name, offset, data.size(), data.data() );
                return;
            }

            /// Write a range of data to a field.  Data is provided as
            /// a vector.
            template < class T >
            void write_field ( std::string const & field_name, index_type offset, std::vector < T > const & data ) {
                write_field ( field_name, offset, data.size(), data.data() );
                return;
            }

            // specialization for strings

            // /// Read a range of string data from a field.  Store results in
            // /// in an array of pointers.
            // void read_field ( std::string const & field_name, index_type offset, index_type ndata, char ** data ) const;
            //
            // /// Write a range of string data to a field.  Data is provided at
            // /// the bare memory address specified.
            // void write_field ( std::string const & field_name, index_type offset, index_type ndata, char * const * data );

            // wrapper for vector of strings

            /// Read a range of string data from a field.  Store results in
            /// in a vector of strings.
            void read_field ( std::string const & field_name, index_type offset, std::vector < std::string > & data ) const;

            /// Write a range of string data to a field.  Data is provided as
            /// a vector of strings.
            void write_field ( std::string const & field_name, index_type offset, std::vector < std::string > const & data );

            // read / write the full time stamp vector

            /// Read the timestamps for this group.  Data is stored in the
            /// bare memory address provided.
            void read_times ( index_type ndata, time_type * data,
                index_type offset = 0 ) const;

            /// Read the timestamps for this group.  Data is stored in the
            /// vector provided.
            void read_times ( std::vector < time_type > & data,
                index_type offset = 0 ) const;

            /// Write the timestamps for this group.  Data is provided at
            /// the bare memory address specified.
            void write_times ( index_type ndata, time_type const * data,
                index_type offset = 0 );

            /// Write the timestamps for this group.  Data is provided as
            /// a vector.
            void write_times ( std::vector < time_type > const & data,
                index_type offset = 0 );

            /// Print information to a stream.
            void info ( std::ostream & out, size_t indent ) const;

            template < class Archive >
            void save ( Archive & ar ) const {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( start_ ) );
                ar ( CEREAL_NVP( stop_ ) );
                ar ( CEREAL_NVP( size_ ) );
                ar ( CEREAL_NVP( dict_ ) );
                ar ( CEREAL_NVP( schm_ ) );
                ar ( CEREAL_NVP( counts_ ) );
                ar ( CEREAL_NVP( type_indx_ ) );
            }

            template < class Archive >
            void load ( Archive & ar ) {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( start_ ) );
                ar ( CEREAL_NVP( stop_ ) );
                ar ( CEREAL_NVP( size_ ) );
                ar ( CEREAL_NVP( dict_ ) );
                ar ( CEREAL_NVP( schm_ ) );
                ar ( CEREAL_NVP( counts_ ) );
                ar ( CEREAL_NVP( type_indx_ ) );
                set_backend();
            }

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
