/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <cmath>
#include <limits>


using namespace std;
using namespace tidas;


tidas::intrvl::intrvl () {
    start = -1.0;
    stop = -1.0;
    first = -1;
    last = -1;
}


tidas::intrvl::intrvl ( time_type new_start, time_type new_stop, index_type new_first, index_type new_last ) {
    start = new_start;
    stop = new_stop;
    first = new_first;
    last = new_last;
}


bool tidas::intrvl::operator== ( const intrvl & other ) const {
    if ( first != other.first ) {
        return false;
    }
    if ( last != other.last ) {
        return false;
    }
    if ( fabs ( start - other.start ) > numeric_limits < time_type > :: epsilon() ) {
        return false;
    }
    if ( fabs ( stop - other.stop ) > numeric_limits < time_type > :: epsilon() ) {
        return false;
    }
    return true;
}


bool tidas::intrvl::operator!= ( const intrvl & other ) const {
    return ! ( *this == other );
}


tidas::intervals::intervals () {
    size_ = 0;
    relocate ( loc_ );
}


tidas::intervals::intervals ( dict const & d, size_t const & size ) {
    size_ = size;
    dict_ = d;
    relocate ( loc_ );
}


tidas::intervals::~intervals () {
}


intervals & tidas::intervals::operator= ( intervals const & other ) {
    if ( this != &other ) {
        copy ( other, "", other.loc_ );
    }
    return *this;
}


tidas::intervals::intervals ( intervals const & other ) {
    copy ( other, "", other.loc_ );
}


tidas::intervals::intervals ( backend_path const & loc ) {
    relocate ( loc );
    sync();
}


tidas::intervals::intervals ( intervals const & other, std::string const & filter, backend_path const & loc ) {
    copy ( other, filter, loc );
}


void tidas::intervals::set_backend () {

    switch ( loc_.type ) {
        case backend_type::none:
            backend_.reset();
            break;
        case backend_type::hdf5:
            backend_.reset( new intervals_backend_hdf5 () );
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


void tidas::intervals::relocate ( backend_path const & loc ) {

    loc_ = loc;
    set_backend();

    // relocate dict

    backend_path dloc;
    dict_loc ( dloc );
    dict_.relocate ( dloc );

    return;
}


void tidas::intervals::sync () {

    // read dict

    dict_.sync();

    // read our own metadata

    if ( loc_.type != backend_type::none ) {

        // if we have an index, use it!

        bool found = false;

        if ( loc_.idx ) {
            found = loc_.idx->query_intervals ( loc_, size_ );
        }

        if ( ! found ) {
            backend_->read ( loc_, size_ );
            if ( loc_.idx && ( loc_.mode == access_mode::write ) ) {
                loc_.idx->add_intervals ( loc_, size_ );
            }
        }

    }

    return;
}


void tidas::intervals::flush () const {

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {    
            // write our own metadata

            backend_->write ( loc_, size_ );

            // update index

            if ( loc_.idx ) {
                loc_.idx->update_intervals ( loc_, size_ );
            }
        }

    }

    // write dict

    dict_.flush();

    return;
}


void tidas::intervals::copy ( intervals const & other, string const & filter, backend_path const & loc ) {

    string filt = filter_default ( filter );

    if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
        TIDAS_THROW( "copy of non-memory group with a filter requires a new location" );
    }

    // extract filters

    map < string, string > filts = filter_split ( filter );

    // set backend

    loc_ = loc;
    set_backend();

    // copy our metadata

    size_ = other.size_;

    // copy dict

    backend_path dloc;
    dict_loc ( dloc );

    dict_.copy ( other.dict_, filts[ dict_submatch_key ], dloc );

    // update index

    if ( loc_.idx && ( loc_.mode == access_mode::write ) && ( loc != other.loc_ ) ) {
        loc_.idx->update_intervals ( loc_, size_ );
    }

    return;
}


void tidas::intervals::link ( link_type const & type, string const & path ) const {

    if ( type != link_type::none ) {

        if ( loc_.type != backend_type::none ) {
            backend_->link ( loc_, type, path );
        }

    }

    return;
}


void tidas::intervals::wipe () const {

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {
            backend_->wipe ( loc_ );
        } else {
            TIDAS_THROW( "cannot wipe intervals in read-only mode" );
        }
    }

    return;
}


void tidas::intervals::dict_loc ( backend_path & dloc ) {
    dloc = loc_;
    if ( loc_.type != backend_type::none ) {
        dloc.meta = backend_->dict_meta();
    }
    return;
}


backend_path tidas::intervals::location () const {
    return loc_;
}


size_t tidas::intervals::size () const {
    return size_;
}


void tidas::intervals::read_data ( interval_list & intr ) const {
    if ( ! backend_ ) {
        TIDAS_THROW( "intervals read_data:  backend not assigned" );
    }

    backend_->read_data ( loc_, intr );
    return;
}


void tidas::intervals::write_data ( interval_list const & intr ) {
    if ( ! backend_ ) {
        TIDAS_THROW( "intervals read_data:  backend not assigned" );
    }

    backend_->write_data ( loc_, intr );
    return;
}


dict const & tidas::intervals::dictionary () const {
    return dict_;
}


index_type tidas::intervals::total_samples ( interval_list const & intr ) {
    index_type tot = 0;
    for ( auto inv : intr ) {
        tot += inv.last - inv.first + 1;
    }
    return tot;
}


time_type tidas::intervals::total_time ( interval_list const & intr ) {
    time_type tot = 0.0;
    for ( auto inv : intr ) {
        tot += inv.stop - inv.start;
    }
    return tot;
}


intrvl tidas::intervals::seek ( interval_list const & intr, time_type time ) {

    intrvl ret;

    // just do a linear seek, since the number of intervals in a single
    // block should never be too large...

    for ( auto inv : intr ) {
        if ( ( time >= inv.start ) && ( time <= inv.stop ) ) {
            ret = inv;
        }
    }

    return ret;
}


intrvl tidas::intervals::seek_ceil ( interval_list const & intr, time_type time ) {

    intrvl ret;

    // just do a linear seek, since the number of intervals in a single
    // block should never be too large...

    for ( auto inv : intr ) {
        if ( time < inv.stop ) {
            // we are inside the interval, or after the stop of the previous one
            ret = inv;
        }
    }

    return ret;
}


intrvl tidas::intervals::seek_floor ( interval_list const & intr, time_type time ) {

    intrvl ret;

    // just do a linear seek, since the number of intervals in a single
    // block should never be too large...

    for ( auto inv : intr ) {
        if ( time > inv.start ) {
            // we are inside the interval, or before the start of the next one
            ret = inv;
        }
    }

    return ret;
}


