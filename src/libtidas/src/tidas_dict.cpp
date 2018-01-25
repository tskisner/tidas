/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <regex>


using namespace std;
using namespace tidas;



tidas::dict::dict () {
    relocate ( loc_ );
}


tidas::dict::~dict () {
}


dict & tidas::dict::operator= ( dict const & other ) {
    if ( this != &other ) {
        copy ( other, "", other.loc_ );
    }
    return *this;
}


tidas::dict::dict ( dict const & other ) {
    copy ( other, "", other.loc_ );
}


tidas::dict::dict ( backend_path const & loc ) {
    relocate ( loc );
    sync();
}


tidas::dict::dict ( dict const & other, string const & filter, backend_path const & loc ) {
    copy ( other, filter, loc );
}


void tidas::dict::set_backend () {

    switch ( loc_.type ) {
        case backend_type::none:
            backend_.reset();
            break;
        case backend_type::hdf5:
            backend_.reset( new dict_backend_hdf5 () );
            break;
        case backend_type::getdata:
            TIDAS_THROW( "GetData backend not yet implemented" );
            break;
        default:
            TIDAS_THROW( "backend not recognized" );
            break;
    }

    return;
}


void tidas::dict::relocate ( backend_path const & loc ) {

    loc_ = loc;
    set_backend ();

    return;
}


void tidas::dict::sync () {

    // read our own metadata

    if ( loc_.type != backend_type::none ) {

        bool found = false;

        if ( loc_.idx ) {
            found = loc_.idx->query_dict ( loc_, data_, types_ );
        }

        if ( ! found ) {
            backend_->read ( loc_, data_, types_ );
            if ( loc_.idx && ( loc_.mode == access_mode::write ) ) {
                loc_.idx->add_dict ( loc_, data_, types_ );
            }
        }

    }

    return;
}


void tidas::dict::flush () const {

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {
            // write our own metadata

            backend_->write ( loc_, data_, types_ );

            // update index

            if ( loc_.idx ) {
                loc_.idx->update_dict ( loc_, data_, types_ );
            }
        }

    }

    return;
}


void tidas::dict::copy ( dict const & other, string const & filter, backend_path const & loc ) {

    string filt = filter_default ( filter );

    if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
        TIDAS_THROW( "copy of non-memory dict with a filter requires a new location" );
    }

    // set backend

    loc_ = loc;
    set_backend ();

    // filtered copy of our metadata

    data_.clear();
    types_.clear();

    regex re ( filt, std::regex::extended );

    for ( auto dat : other.data() ) {
        if ( regex_match ( dat.first, re ) ) {
            data_[ dat.first ] = dat.second;
            types_[ dat.first ] = other.types().at( dat.first );
        }
    }

    // update index

    if ( loc_.idx && ( loc_.mode == access_mode::write ) && ( loc != other.loc_ ) ) {
        loc_.idx->update_dict ( loc_, data_, types_ );
    }

    return;
}


backend_path tidas::dict::location () const {
    return loc_;
}


template <>
void tidas::dict::put < float > ( std::string const & key, float const & val ) {
    std::ostringstream o;
    o.precision(6);
    o.str("");
    if ( ! ( o << val ) ) {
        TIDAS_THROW( "cannot convert type to string \
            for dict storage" );
    }
    data_[ key ] = o.str();
    types_[ key ] = data_type_get ( typeid ( val ) );
    return;
}


void tidas::dict::clear () {
    data_.clear();
    types_.clear();
    return;
}


std::map < std::string, std::string > const & tidas::dict::data() const {
    return data_;
}


std::map < std::string, data_type > const & tidas::dict::types() const {
    return types_;
}
