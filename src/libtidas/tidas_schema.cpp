/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas/re2/re2.h>


using namespace std;
using namespace tidas;


tidas::field::field () {
	type = TYPE_NONE;
	name = "";
	units = "";
}


bool tidas::field::operator== ( const field & other ) const {
	if ( type != other.type ) {
		return false;
	}
	if ( name != other.name ) {
		return false;
	}
	if ( units != other.units ) {
		return false;
	}
	return true;
}


bool tidas::field::operator!= ( const field & other ) const {
	return ! ( *this == other );
}


tidas::schema::schema () {
	init();
}


tidas::schema::~schema () {
	clear();
}


tidas::schema::schema ( schema const & other ) {
	init();
	clear();
	copy ( other );
}


schema & tidas::schema::operator= ( schema const & other ) {
	if ( this != &other ) {
		clear();
		copy ( other );
	}
	return *this;
}


void tidas::schema::init () {
	backend_ = NULL;
	return;
}


void tidas::schema::clear () {
	if ( backend_ ) {
		delete backend_;
		backend_ = NULL;
	}
	return;
}


void tidas::schema::copy ( schema const & other ) {
	fields_ = other.fields_;
	loc_ = other.loc_;
	if ( other.backend_ ) {
		backend_ = other.backend_->clone();
	}
}


tidas::schema::schema ( backend_path const & loc, string const & filter ) {
	init();
	relocate ( loc );
	read_meta ( filter );
}


tidas::schema::schema ( field_list const & fields ) {
	init();
	fields_ = fields;
}


void tidas::schema::read_meta ( string const & filter ) {

	string filt = filter_default ( filter );
	
	if ( backend_ ) {
		backend_->read_meta ( loc_, filt, fields_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::schema::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	if ( backend_ ) {
		backend_->write_meta ( loc_, filt, fields_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::schema::relocate ( backend_path const & loc ) {
	loc_ = loc;

	if ( backend_ ) {
		delete backend_;
	}
	backend_ = NULL;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_ = new schema_backend_mem ();
			break;
		case BACKEND_HDF5:
			backend_ = new schema_backend_hdf5 ();
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


backend_path tidas::schema::location () const {
	return loc_;
}


schema tidas::schema::duplicate ( std::string const & filter, backend_path const & newloc ) {
	schema newschema;
	newschema.fields_ = fields_;
	newschema.relocate ( newloc );
	newschema.write_meta ( filter );

	// reload to pick up filtered metadata
	newschema.read_meta ( "" );

	return newschema;
}


void tidas::schema::append ( field const & fld ) {
	fields_.push_back ( fld );
	return;
}


void tidas::schema::remove ( std::string const & name ) {
	field_list::iterator it;

	for ( it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			fields_.erase ( it );
			break;
		}
	}

	return;
}


field tidas::schema::seek ( std::string const & name ) const {

	for ( field_list::const_iterator it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			return (*it);
		}
	}

	return field();
}


field_list tidas::schema::fields () const {
	return fields_;
}



tidas::schema_backend_mem::schema_backend_mem () {

}


tidas::schema_backend_mem::~schema_backend_mem () {

}


schema_backend_mem * tidas::schema_backend_mem::clone () {
	schema_backend_mem * ret = new schema_backend_mem ( *this );
	ret->fields_ = fields_;
	return ret;
}


void tidas::schema_backend_mem::read_meta ( backend_path const & loc, string const & filter, field_list & fields ) {

	fields.clear();

	RE2 re ( filter );

	for ( field_list :: const_iterator it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( RE2::FullMatch ( it->name, re ) ) {
			fields.push_back( *it );
		}
	}

	return;
}


void tidas::schema_backend_mem::write_meta ( backend_path const & loc, string const & filter, field_list const & fields ) {

	fields_.clear();

	RE2 re ( filter );

	for ( field_list :: const_iterator it = fields.begin(); it != fields.end(); ++it ) {
		if ( RE2::FullMatch ( it->name, re ) ) {
			fields_.push_back( *it );
		}
	}

	return;
}







