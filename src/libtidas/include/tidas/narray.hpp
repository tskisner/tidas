
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TIDAS_NARRAY_HPP
#define TIDAS_NARRAY_HPP


namespace tidas {

/// narray_data class.
/// This represents a N-dimensional array data as a wrapper around a buffer.

class narray_data {
    public:

        /// Default constructor.
        narray_data();

        /// Constructor specifying the dimensions and data type.
        narray_data(std::vector <index_type> const & shp, data_type dtyp);

        bool operator==(const narray_data & other) const;
        bool operator!=(const narray_data & other) const;

        /// Query the shape.
        std::vector <index_type> shape() const;

        /// Query the type..
        data_type dtype() const;

        template <class Archive>
        void save(Archive & ar) const {
            ar(CEREAL_NVP(shape_));
            ar(CEREAL_NVP(dtype_));
        }

        template <class Archive>
        void load(Archive & ar) {
            ar(CEREAL_NVP(shape_));
            ar(CEREAL_NVP(dtype_));
        }

    private:

        std::vector <index_type> shape_;
        data_type dtype_;
};


// base class for narray backend interface

class narray_backend {
    public:

        narray_backend() {}

        virtual ~narray_backend() {}

        virtual void read(backend_path const & loc,
                          std::vector <index_type> & shp, data_type & dtyp) = 0;

        virtual void write(backend_path const & loc,
                           std::vector <index_type> const & shp,
                           data_type dtyp) const = 0;

        virtual void link(backend_path const & loc, link_type type,
                          std::string const & path) const = 0;

        virtual void wipe(backend_path const & loc) const = 0;

        virtual void read_data(backend_path const & loc,
                               std::vector <index_type> const & offset,
                               std::vector <index_type> const & size,
                               double * data) const = 0;

        virtual void write_data(backend_path const & loc,
                                std::vector <index_type> const & offset,
                                std::vector <index_type> const & size,
                                double const * data) = 0;

        virtual std::string dict_meta() const = 0;
};


// HDF5 backend class

class narray_backend_hdf5 : public narray_backend {
    public:

        narray_backend_hdf5();
        ~narray_backend_hdf5();
        narray_backend_hdf5(narray_backend_hdf5 const & other);
        narray_backend_hdf5 & operator=(narray_backend_hdf5 const & other);

        void read(backend_path const & loc, size_t & size);

        void write(backend_path const & loc, size_t const & size) const;

        void link(backend_path const & loc, link_type type,
                  std::string const & path) const;

        void wipe(backend_path const & loc) const;

        void read_data(backend_path const & loc, interval_list & intr) const;

        void write_data(backend_path const & loc, interval_list const & intr);

        std::string dict_meta() const;
};


// GetData backend class

class narray_backend_getdata : public narray_backend {
    public:

        narray_backend_getdata();
        ~narray_backend_getdata();
        narray_backend_getdata(narray_backend_getdata const & other);
        narray_backend_getdata & operator=(narray_backend_getdata const & other);

        void read(backend_path const & loc, size_t & size);

        void write(backend_path const & loc, size_t const & size) const;

        void link(backend_path const & loc, link_type type,
                  std::string const & path) const;

        void wipe(backend_path const & loc) const;

        void read_data(backend_path const & loc, interval_list & intr) const;

        void write_data(backend_path const & loc, interval_list const & intr);

        std::string dict_meta() const;
};


/// An narray object represents a N-dimensional array of data.
/// An narray object has a shape and a datatype.
/// An narray object has an optional dictionary associated
/// with it at construction time.
/// Some public methods are only used internally and are not needed for
/// normal use of the object.  These are labelled "internal".

class narray {
    public:

        /// Default constructor.  The backend is set to be memory based.
        narray();

        /// Constructor with specified shape, type, and dictionary.
        narray(dict const & d, std::vector <index_type> const & shp, data_type dtyp);

        /// Destructor
        ~narray();

        /// Assignment operator.
        narray & operator=(narray const & other);

        /// Copy constructor.
        narray(narray const & other);

        /// (**Internal**) Load the narray from the specified
        /// location.
        /// All meta data operations will apply to this location.
        narray(backend_path const & loc);

        /// (**Internal**) Copy from an existing narray instance.
        /// Apply an optional filter to elements and relocate to a new
        /// location.  If a filter is given, a new location must be
        /// specified.
        narray(narray const & other, std::string const & filter,
               backend_path const & loc);

        // metadata ops

        /// (**Internal**) Change the location of the narray.
        void relocate(backend_path const & loc);

        /// (**Internal**) Reload metadata from the current location,
        /// overwriting the current state in memory.
        void sync();

        /// (**Internal**) Write metadata to the current location,
        /// overwriting the information at that location.
        void flush() const;

        /// (**Internal**) Copy with optional selection and relocation.
        void copy(narray const & other, std::string const & filter,
                  backend_path const & loc);

        /// (**Internal**) Create a link at the specified location.
        void link(link_type const & type, std::string const & path) const;

        /// (**Internal**) Delete the on-disk data and metadata associated
        /// with this object.  In-memory metadata is not modified.
        void wipe() const;

        /// (**Internal**) The current location.
        backend_path location() const;

        // data ops

        /// A (const) reference to the dictionary specified at
        /// construction.
        dict const & dictionary() const;

        /// Query the shape.
        std::vector <index_type> shape() const;

        /// Query the type..
        data_type dtype() const;

        /// Read the narray data from the current location.
        void read_data(narray_data & narr,
                       std::vector <index_type> const & offset,
                       std::vector <index_type> const & size) const;

        /// Read the narray data from the current location.
        narray_data read_data(std::vector <index_type> const & offset,
                              std::vector <index_type> const & size) const;

        /// Write the narray data to the current location.
        void write_data(narray_data const & narr,
                        std::vector <index_type> const & offset);

        /// Print information to a stream.
        void info(std::ostream & out, size_t indent) const;

        template <class Archive>
        void save(Archive & ar) const {
            ar(CEREAL_NVP(loc_));
            ar(CEREAL_NVP(shape_));
            ar(CEREAL_NVP(dtype_));
            ar(CEREAL_NVP(dict_));
        }

        template <class Archive>
        void load(Archive & ar) {
            ar(CEREAL_NVP(loc_));
            ar(CEREAL_NVP(shape_));
            ar(CEREAL_NVP(dtype_));
            ar(CEREAL_NVP(dict_));
            set_backend();
        }

    private:

        void set_backend();

        void dict_loc(backend_path & dloc);

        dict dict_;

        std::vector <index_type> shape_;
        data_type dtype_;

        backend_path loc_;
        std::unique_ptr <narray_backend> backend_;
};

}


#endif // ifndef TIDAS_NARRAY_HPP
