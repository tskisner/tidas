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
	flush();
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


void tidas::dict::set_backend ( backend_path const & loc, std::unique_ptr < dict_backend > & backend ) {

	switch ( loc.type ) {
		case BACKEND_MEM:
			backend.reset( new dict_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend.reset( new dict_backend_hdf5 () );
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

	// set backend

	set_backend ( loc_, backend_ );

	return;
}


void tidas::dict::sync () {

	// read our own metadata

	backend_->read ( loc_, data_, types_ );

	return;
}


void tidas::dict::flush () {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot flush to read-only location" );
	}

	// write our own metadata

	backend_->write ( loc_, data_, types_ );

	return;
}


void tidas::dict::copy ( dict const & other, string const & filter, backend_path const & loc ) {

	string filt = filter_default ( filter );

	if ( ( filt != ".*" ) && ( loc.type != BACKEND_MEM ) && ( loc == other.loc_ ) ) {
		TIDAS_THROW( "copy of non-memory dict with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;
	set_backend ( loc_, backend_ );

	// filtered copy of our metadata

	data_.clear();
	types_.clear();

	RE2 re ( filt );

	for ( map < string, string > :: const_iterator it = other.data().begin(); it != other.data().end(); ++it ) {
		if ( RE2::FullMatch ( it->first, re ) ) {
			data_[ it->first ] = it->second;
			types_[ it->first ] = other.types().at( it->first );
		}
	}

	if ( ( loc_ != other.loc_ ) || ( loc.type == BACKEND_MEM ) ) {
		if ( loc_.mode == MODE_RW ) {

			// write our metadata
			
			backend_->write ( loc_, data_, types_ );

		} else {
			TIDAS_THROW( "cannot copy to new read-only location" );
		}
	}

	return;
}


void tidas::dict::duplicate ( backend_path const & loc ) {

	if ( loc.type == BACKEND_MEM ) {
		TIDAS_THROW( "calling duplicate() with memory backend makes no sense" );
	}

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot duplicate to read-only location" );
	}

	// write our metadata

	unique_ptr < dict_backend > backend;
	set_backend ( loc, backend );

	backend->write ( loc, data_, types_ );

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


tidas::dict_backend_mem::dict_backend_mem () {

}


tidas::dict_backend_mem::~dict_backend_mem () {

}


tidas::dict_backend_mem::dict_backend_mem ( dict_backend_mem const & other ) {
	data_ = other.data_;
	types_ = other.types_;
}


dict_backend_mem & tidas::dict_backend_mem::operator= ( dict_backend_mem const & other ) {
	if ( this != &other ) {
		data_ = other.data_;
		types_ = other.types_;
	}
	return *this;
}


dict_backend * tidas::dict_backend_mem::clone () {
	dict_backend_mem * ret = new dict_backend_mem ( *this );
	ret->data_ = data_;
	ret->types_ = types_;
	return ret;
}


void tidas::dict_backend_mem::read ( backend_path const & loc, map < string, string > & data, map < string, data_type > & types ) {
	data = data_;
	types = types_;
	return;
}


void tidas::dict_backend_mem::write ( backend_path const & loc, map < string, string > const & data, map < string, data_type > const & types ) {
	data_ = data;
	types_ = types;
	return;
}


