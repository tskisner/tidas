/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


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
		case BACKEND_NONE:
			backend_.reset();
			break;
		case BACKEND_HDF5:
			backend_.reset( new dict_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
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

	if ( loc_.type != BACKEND_NONE ) {
		backend_->read ( loc_, data_, types_ );
	}

	return;
}


void tidas::dict::flush () const {

	if ( loc_.type != BACKEND_NONE ) {

		if ( loc_.mode == MODE_RW ) {
			// write our own metadata

			backend_->write ( loc_, data_, types_ );
		}

	}

	return;
}


void tidas::dict::copy ( dict const & other, string const & filter, backend_path const & loc ) {

	string filt = filter_default ( filter );

	if ( ( filt != ".*" ) && ( loc.type != BACKEND_NONE ) && ( loc == other.loc_ ) ) {
		TIDAS_THROW( "copy of non-memory dict with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;
	set_backend ();

	// filtered copy of our metadata

	data_.clear();
	types_.clear();

	regex re ( filt, std::regex::extended );

	for ( map < string, string > :: const_iterator it = other.data().begin(); it != other.data().end(); ++it ) {
		if ( regex_match ( it->first, re ) ) {
			data_[ it->first ] = it->second;
			types_[ it->first ] = other.types().at( it->first );
		}
	}

	return;
}


backend_path tidas::dict::location () const {
	return loc_;
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

