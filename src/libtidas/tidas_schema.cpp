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


tidas::field::field ( std::string const & fname, data_type ftype, std::string const & funits ) {
	type = ftype;
	name = fname;
	units = funits;
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


field_list tidas::field_filter_type ( field_list const & fields, data_type type ) {
	field_list ret;
	for ( field_list::const_iterator it = fields.begin(); it != fields.end(); ++it ) {
		if ( it->type == type ) {
			ret.push_back ( *it );
		}
	}
	return ret;
}


tidas::schema::schema () {
	relocate ( loc_ );
	flush();
}


tidas::schema::schema ( field_list const & fields ) {
	fields_ = fields;
	relocate ( loc_ );
	flush();
}


tidas::schema::~schema () {

}


schema & tidas::schema::operator= ( schema const & other ) {
	if ( this != &other ) {
		copy ( other, "", other.loc_ );
	}
	return *this;
}


tidas::schema::schema ( schema const & other ) {
	copy ( other, "", other.loc_ );
}


tidas::schema::schema ( backend_path const & loc ) {
	relocate ( loc );
	sync ();
}


tidas::schema::schema ( schema const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::schema::set_backend ( backend_path const & loc, std::unique_ptr < schema_backend > & backend ) {

	switch ( loc.type ) {
		case BACKEND_MEM:
			backend.reset( new schema_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend.reset( new schema_backend_hdf5 () );
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


void tidas::schema::relocate ( backend_path const & loc ) {

	loc_ = loc;

	// set backend

	set_backend ( loc_, backend_ );

	return;
}


void tidas::schema::sync () {

	// read our own metadata

	backend_->read ( loc_, fields_ );

	return;
}


void tidas::schema::flush () {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot flush to read-only location" );
	}

	// write our own metadata

	backend_->write ( loc_, fields_ );

	return;
}


void tidas::schema::copy ( schema const & other, string const & filter, backend_path const & loc ) {	

	// extract filters

	string filt = filter_default ( filter );

	if ( ( loc == other.loc_ ) && ( filt != ".*" ) ) {
		TIDAS_THROW( "copy of schema with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;
	set_backend ( loc_, backend_ );

	// filtered copy of our metadata

	fields_.clear();

	RE2 re ( filt );

	for ( field_list :: const_iterator it = other.fields().begin(); it != other.fields().end(); ++it ) {
		if ( RE2::FullMatch ( it->name, re ) ) {
			fields_.push_back ( field( it->name, it->type, it->units ) );
		}
	}

	if ( loc_ != other.loc_ ) {
		if ( loc_.mode == MODE_RW ) {

			// write our metadata

			backend_->write ( loc_, fields_ );

		} else {
			TIDAS_THROW( "cannot copy to new read-only location" );
		}
	}

	return;
}


void tidas::schema::duplicate ( backend_path const & loc ) {

	if ( loc.type == BACKEND_MEM ) {
		TIDAS_THROW( "calling duplicate() with memory backend makes no sense" );
	}

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot duplicate to read-only location" );
	}

	// write our metadata

	unique_ptr < schema_backend > backend;
	set_backend ( loc, backend );

	backend->write ( loc, fields_ );

	return;
}


backend_path tidas::schema::location () const {
	return loc_;
}


void tidas::schema::clear () {
	fields_.clear();
	return;
}


void tidas::schema::append ( field const & fld ) {
	fields_.push_back ( fld );
	return;
}


void tidas::schema::remove ( string const & name ) {
	field_list::iterator it;

	for ( it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			fields_.erase ( it );
			break;
		}
	}

	return;
}


field tidas::schema::seek ( string const & name ) const {

	for ( field_list::const_iterator it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			return (*it);
		}
	}

	return field();
}


field_list const & tidas::schema::fields () const {
	return fields_;
}


tidas::schema_backend_mem::schema_backend_mem () {

}


tidas::schema_backend_mem::~schema_backend_mem () {

}


tidas::schema_backend_mem::schema_backend_mem ( schema_backend_mem const & other ) {
	fields_ = other.fields_;
}


schema_backend_mem & tidas::schema_backend_mem::operator= ( schema_backend_mem const & other ) {
	if ( this != &other ) {
		fields_ = other.fields_;
	}
	return *this;
}


schema_backend * tidas::schema_backend_mem::clone () {
	schema_backend_mem * ret = new schema_backend_mem ( *this );
	ret->fields_ = fields_;
	return ret;
}


void tidas::schema_backend_mem::read ( backend_path const & loc, field_list & fields ) {
	fields = fields_;
	return;
}


void tidas::schema_backend_mem::write ( backend_path const & loc, field_list const & fields ) {
	fields_ = fields;
	return;
}








