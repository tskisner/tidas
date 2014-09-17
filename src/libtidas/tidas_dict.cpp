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
	backend_ = NULL;
}


tidas::dict::~dict () {
	if ( backend_ ) {
		delete backend_;
	}
}


tidas::dict::dict ( dict const & orig ) {
	data_ = orig.data_;
	types_ = orig.types_;
	loc_ = orig.loc_;
	backend_ = NULL;
	if ( orig.backend_ ) {
		backend_ = orig.backend_->clone();
	}
}


tidas::dict::dict ( backend_path const & loc, string const & filter ) {
	backend_ = NULL;
	relocate ( loc );
	read_meta ( filter );
}


void tidas::dict::read_meta ( string const & filter ) {

	string filt = filter_default ( filter );
	
	if ( backend_ ) {
		backend_->read_meta ( loc_, filt, data_, types_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::dict::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	if ( backend_ ) {
		backend_->write_meta ( loc_, filt, data_, types_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::dict::relocate ( backend_path const & loc ) {
	loc_ = loc;

	if ( backend_ ) {
		delete backend_;
	}
	backend_ = NULL;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_ = new dict_backend_mem ();
			break;
		case BACKEND_HDF5:
			backend_ = new dict_backend_hdf5 ();
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


backend_path tidas::dict::location () const {
	return loc_;
}


dict tidas::dict::duplicate ( std::string const & filter, backend_path const & newloc ) {
	dict newdict;
	newdict.data_ = data_;
	newdict.types_ = types_;
	newdict.relocate ( newloc );
	newdict.write_meta ( filter );
	return newdict;
}


string tidas::dict::get_string ( string const & key ) const {
	return data_.at( key );
}


double tidas::dict::get_double ( string const & key ) const {
	return stod ( data_.at( key ) );
}


int tidas::dict::get_int ( string const & key ) const {
	return stoi ( data_.at( key ) );
}


long long tidas::dict::get_ll ( string const & key ) const {
	return stoll ( data_.at( key ) );
}


void tidas::dict::clear () {
	data_.clear();
	return;
}




tidas::dict_backend_mem::dict_backend_mem () {

}


tidas::dict_backend_mem::~dict_backend_mem () {

}


dict_backend_mem * tidas::dict_backend_mem::clone () {
	dict_backend_mem * ret = new dict_backend_mem ( *this );
	ret->data_ = data_;
	ret->types_ = types_;
	return ret;
}


void tidas::dict_backend_mem::read_meta ( backend_path const & loc, string const & filter, map < string, string > & data, map < string, data_type > & types ) {

	data.clear();
	types.clear();

	RE2 re ( filter );

	for ( map < string, string > :: const_iterator it = data_.begin(); it != data_.end(); ++it ) {
		if ( RE2::FullMatch ( it->first, re ) ) {
			data[ it->first ] = it->second;
			types[ it->first ] = types_[ it->first ];
		}
	}

	return;
}


void tidas::dict_backend_mem::write_meta ( backend_path const & loc, string const & filter, map < string, string > const & data, map < string, data_type > const & types ) {

	data_.clear();
	types_.clear();

	RE2 re ( filter );

	for ( map < string, string > :: const_iterator it = data.begin(); it != data.end(); ++it ) {
		if ( RE2::FullMatch ( it->first, re ) ) {
			data_[ it->first ] = it->second;
			types_[ it->first ] = types[ it->first ];
		}
	}

	return;
}


