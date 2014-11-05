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
}


tidas::intervals::intervals ( dict const & d, size_t const & size ) {
	size_ = size;
	dict_ = d;
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
	read ( loc );
}


tidas::intervals::intervals ( intervals const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::intervals::read ( backend_path const & loc ) {

	// set backend

	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new intervals_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend_.reset( new intervals_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	// read dict

	backend_path dictloc = loc_;
	dictloc.meta = backend_->dict_meta();

	dict_.read ( dictloc );

	// read our own metadata

	backend_->read ( loc_, size_ );

	return;
}


void tidas::intervals::copy ( intervals const & other, string const & filter, backend_path const & loc ) {	

	// extract filters

	map < string, string > filts = filter_split ( filter );

	string fil = filter_default ( filts[ "root" ] );

	if ( ( loc == other.loc_ ) && ( fil != ".*" ) ) {
		TIDAS_THROW( "copy of intervals with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new intervals_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend_.reset( new intervals_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	// filtered copy of our metadata

	if ( fil != ".*" ) {
		TIDAS_THROW( "intervals class does not accept filters" );
	}
	size_ = other.size_;

	if ( loc_ != other.loc_ ) {
		if ( loc_.mode == MODE_RW ) {

			// write our metadata

			backend_->write ( loc_, size_ );

			// copy dict

			backend_path dictloc = loc_;
			dictloc.meta = backend_->dict_meta();

			dict_.copy ( other.dict_, filts[ dict_submatch_key ], dictloc );

		} else {
			TIDAS_THROW( "cannot copy to new read-only location" );
		}
	} else {
		// just copy in memory
		dict_ = other.dict_;
	}

	return;
}


void tidas::intervals::write ( backend_path const & loc ) {

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot write to read-only location" );
	}

	// write our metadata

	unique_ptr < intervals_backend > backend;

	switch ( loc.type ) {
		case BACKEND_MEM:
			backend.reset( new intervals_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend.reset( new intervals_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	backend->write ( loc, size_ );

	// write dict

	backend_path dictloc = loc;
	dictloc.meta = backend->dict_meta();

	dict_.write ( dictloc );

	return;
}


backend_path tidas::intervals::location () const {
	return loc_;
}


size_t tidas::intervals::size () const {
	return size_;
}


void tidas::intervals::read_data ( interval_list & intr ) {
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
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		tot += it->last - it->first + 1;
	}
	return tot;
}


time_type tidas::intervals::total_time ( interval_list const & intr ) {
	time_type tot = 0.0;
	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		tot += it->stop - it->start;
	}
	return tot;
}


intrvl tidas::intervals::seek ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( ( time >= it->start ) && ( time <= it->stop ) ) {
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_ceil ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( time < it->stop ) {
			// we are inside the interval, or after the stop of the previous one
			ret = (*it);
		}
	}

	return ret;
}


intrvl tidas::intervals::seek_floor ( interval_list const & intr, time_type time ) {

	intrvl ret;

	// just do a linear seek, since the number of intervals in a single
	// block should never be too large...

	for ( interval_list::const_iterator it = intr.begin(); it != intr.end(); ++it ) {
		if ( time > it->start ) {
			// we are inside the interval, or before the start of the next one
			ret = (*it);
		}
	}

	return ret;
}





tidas::intervals_backend_mem::intervals_backend_mem () : intervals_backend () {

}


tidas::intervals_backend_mem::~intervals_backend_mem () {

}


tidas::intervals_backend_mem::intervals_backend_mem ( intervals_backend_mem const & other ) {
	store_ = other.store_;
	size_ = other.size_;
}


intervals_backend_mem & tidas::intervals_backend_mem::operator= ( intervals_backend_mem const & other ) {
	if ( this != &other ) {
		store_ = other.store_;
		size_ = other.size_;
	}
	return *this;
}


intervals_backend * tidas::intervals_backend_mem::clone () {
	intervals_backend_mem * ret = new intervals_backend_mem ( *this );
	ret->store_ = store_;
	ret->size_ = size_;
	return ret;
}


string tidas::intervals_backend_mem::dict_meta () {
	return string("");
}


void tidas::intervals_backend_mem::read ( backend_path const & loc, size_t & size ) {
	size = size_;
	return;
}


void tidas::intervals_backend_mem::write ( backend_path const & loc, size_t const & size ) {
	size_ = size;
	return;
}


void tidas::intervals_backend_mem::read_data ( backend_path const & loc, interval_list & intr ) {
	intr = store_;
	return;
}


void tidas::intervals_backend_mem::write_data ( backend_path const & loc, interval_list const & intr ) {
	store_ = intr;
	return;
}




