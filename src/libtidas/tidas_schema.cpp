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


tidas::field::~field () {

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

}


tidas::schema::~schema () {

}

tidas::schema::schema ( field_list const & fields ) {
	fields_ = fields;
}


tidas::schema::schema ( schema const & orig ) {
	fields_ = orig.fields_;
}


tidas::schema::schema ( schema const & orig, std::string const & match ) {
	RE2 re ( match );

	for ( field_list::const_iterator it = orig.fields_.begin(); it != orig.fields_.end(); ++it ) {
		if ( RE2::FullMatch ( it->name, re ) ) {
			fields_.push_back ( *it );
		}
	}
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


