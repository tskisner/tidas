/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


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


tidas::intervals::intervals () {

}


tidas::intervals::~intervals () {

}


tidas::intervals::intervals ( intervals const & other ) {
	copy ( other );
}


intervals & tidas::intervals::operator= ( intervals const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


void tidas::intervals::copy ( intervals const & other ) {
	dict_ = other.dict_;
	loc_ = other.loc_;
	if ( other.backend_ ) {
		backend_.reset( other.backend_->clone() );
	}
}


tidas::intervals::intervals ( backend_path const & loc, string const & filter ) {
	relocate ( loc );
	read_meta ( filter );
}


void tidas::intervals::read_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	// extract dictionary filter and process
	string dict_filter;
	dict_.read_meta ( dict_filter );
	
	if ( backend_ ) {
		backend_->read_meta ( loc_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::intervals::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	if ( backend_ ) {
		backend_->write_meta ( loc_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	// extract dictionary filter and process
	string dict_filter;
	dict_.write_meta ( dict_filter );

	return;
}


void tidas::intervals::relocate ( backend_path const & loc ) {
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

	backend_path dict_loc = loc_;
	dict_loc.meta = intervals_meta_time;

	dict_.relocate ( dict_loc );

	return;
}


backend_path tidas::intervals::location () const {
	return loc_;
}


intervals tidas::intervals::duplicate ( string const & filter, backend_path const & newloc ) {
	intervals newintervals;
	newintervals.dict_ = dict_;
	newintervals.relocate ( newloc );
	newintervals.write_meta ( filter );

	// reload to pick up filtered metadata
	newintervals.read_meta ( "" );

	return newintervals;
}


void tidas::intervals::read_data ( interval_list & intr ) {
	backend_->read_data ( loc_, intr );
	return;
}


void tidas::intervals::write_data ( interval_list const & intr ) {
	backend_->write_data ( loc_, intr );
	return;
}


dict & tidas::intervals::dictionary () const {
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
}


intervals_backend_mem & tidas::intervals_backend_mem::operator= ( intervals_backend_mem const & other ) {
	if ( this != &other ) {
		store_ = other.store_;
	}
	return *this;
}


intervals_backend * tidas::intervals_backend_mem::clone () {
	intervals_backend_mem * ret = new intervals_backend_mem ( *this );
	ret->store_ = store_;
	return ret;
}


void tidas::intervals_backend_mem::read_meta ( backend_path const & loc ) {

	return;
}


void tidas::intervals_backend_mem::write_meta ( backend_path const & loc ) {

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




