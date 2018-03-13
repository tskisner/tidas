/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_INTERVALS_HPP
#define TIDAS_INTERVALS_HPP


namespace tidas {

    /// intrvl class.
    /// This represents a single span of time and their sample indices for a
    /// particular group.

    class intrvl {

        public :

            /// Default constructor.
            intrvl ();

            /// Constructor specifying the start and stop times and the
            /// first and last indices.
            intrvl ( time_type new_start, time_type new_stop, index_type new_first, index_type new_last );

            bool operator== ( const intrvl & other ) const;
            bool operator!= ( const intrvl & other ) const;

            /// Start time of the interval.
            time_type start;

            /// Stop time of the interval.
            time_type stop;

            /// First sample index in the interval.
            index_type first;

            /// Last sample index in the interval.
            index_type last;

    };

    /// Convenience typedef for a vector of intervals.
    typedef std::vector < intrvl > interval_list;


    // base class for intervals backend interface

    class intervals_backend {

        public :

            intervals_backend () {}
            virtual ~intervals_backend () {}

            virtual void read ( backend_path const & loc, size_t & size ) = 0;

            virtual void write ( backend_path const & loc, size_t const & size ) const = 0;

            virtual void link ( backend_path const & loc, link_type type, std::string const & path ) const = 0;

            virtual void wipe ( backend_path const & loc ) const = 0;

            virtual void read_data ( backend_path const & loc, interval_list & intr ) const = 0;

            virtual void write_data ( backend_path const & loc, interval_list const & intr ) = 0;

            virtual std::string dict_meta () const = 0;

    };


    // HDF5 backend class

    class intervals_backend_hdf5 : public intervals_backend {

        public :

            intervals_backend_hdf5 ();
            ~intervals_backend_hdf5 ();
            intervals_backend_hdf5 ( intervals_backend_hdf5 const & other );
            intervals_backend_hdf5 & operator= ( intervals_backend_hdf5 const & other );

            void read ( backend_path const & loc, size_t & size );

            void write ( backend_path const & loc, size_t const & size ) const;

            void link ( backend_path const & loc, link_type type, std::string const & path ) const;

            void wipe ( backend_path const & loc ) const;

            void read_data ( backend_path const & loc, interval_list & intr ) const;

            void write_data ( backend_path const & loc, interval_list const & intr );

            std::string dict_meta () const;

    };


    // GetData backend class

    class intervals_backend_getdata : public intervals_backend {

        public :

            intervals_backend_getdata ();
            ~intervals_backend_getdata ();
            intervals_backend_getdata ( intervals_backend_getdata const & other );
            intervals_backend_getdata & operator= ( intervals_backend_getdata const & other );

            void read ( backend_path const & loc, size_t & size );

            void write ( backend_path const & loc, size_t const & size ) const;

            void link ( backend_path const & loc, link_type type, std::string const & path ) const;

            void wipe ( backend_path const & loc ) const;

            void read_data ( backend_path const & loc, interval_list & intr ) const;

            void write_data ( backend_path const & loc, interval_list const & intr );

            std::string dict_meta () const;

    };


    /// An intervals object represents a list of disjoint spans of time.
    /// Although each interval has both a time range and a sample range,
    /// those sample indices are only meaningful in the context of a particular
    /// group.  An intervals object has an optional dictionary associated
    /// with it at construction time.
    /// Some public methods are only used internally and are not needed for
    /// normal use of the object.  These are labelled "internal".

    class intervals {

        public :

            /// Default constructor.  The backend is set to be memory based.
            intervals ();

            /// Constructor with specified size and dictionary.
            intervals ( dict const & d, size_t const & size );

            /// Destructor
            ~intervals ();

            /// Assignment operator.
            intervals & operator= ( intervals const & other );

            /// Copy constructor.
            intervals ( intervals const & other );

            /// (**Internal**) Load the intervals from the specified
            /// location.
            /// All meta data operations will apply to this location.
            intervals ( backend_path const & loc );

            /// (**Internal**) Copy from an existing intervals instance.
            /// Apply an optional filter to elements and relocate to a new
            /// location.  If a filter is given, a new location must be
            /// specified.
            intervals ( intervals const & other, std::string const & filter,
                backend_path const & loc );

            // metadata ops

            /// (**Internal**) Change the location of the intervals.
            void relocate ( backend_path const & loc );

            /// (**Internal**) Reload metadata from the current location,
            /// overwriting the current state in memory.
            void sync ();

            /// (**Internal**) Write metadata to the current location,
            /// overwriting the information at that location.
            void flush () const;

            /// (**Internal**) Copy with optional selection and relocation.
            void copy ( intervals const & other, std::string const & filter, backend_path const & loc );

            /// (**Internal**) Create a link at the specified location.
            void link ( link_type const & type, std::string const & path ) const;

            /// (**Internal**) Delete the on-disk data and metadata associated
            /// with this object.  In-memory metadata is not modified.
            void wipe () const;

            /// (**Internal**) The current location.
            backend_path location () const;

            // data ops

            /// A (const) reference to the dictionary specified at
            /// construction.
            dict const & dictionary () const;

            /// The number of intervals contained in this object.
            size_t size () const;

            /// Read the list of intervals from the current location.
            void read_data ( interval_list & intr ) const;

            /// Read the list of intervals from the current location.
            interval_list read_data () const;

            /// Write the list of intervals to the current location.
            void write_data ( interval_list const & intr );

            /// The total number of samples included in all intervals.
            static index_type total_samples ( interval_list const & intr );

            /// The total time span included in all intervals.
            static time_type total_time ( interval_list const & intr );

            /// Return the single interval that contains a specified time.
            /// If no interval contains the time, an empty interval is
            /// returned.
            static intrvl seek ( interval_list const & intr, time_type time );

            /// Return the first interval whose stop time is after the
            /// specified time.
            static intrvl seek_ceil ( interval_list const & intr, time_type time );

            /// Return the last interval whose start time is before the
            /// specified time.
            static intrvl seek_floor ( interval_list const & intr, time_type time );

            /// Print information to a stream.
            void info ( std::ostream & out, size_t indent ) const;

            template < class Archive >
            void save ( Archive & ar ) const {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( size_ ) );
                ar ( CEREAL_NVP( dict_ ) );
            }

            template < class Archive >
            void load ( Archive & ar ) {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( size_ ) );
                ar ( CEREAL_NVP( dict_ ) );
                set_backend();
            }

        private :

            void set_backend ();

            void dict_loc ( backend_path & dloc );

            dict dict_;

            size_t size_;

            backend_path loc_;
            std::unique_ptr < intervals_backend > backend_;

    };

}


#endif
