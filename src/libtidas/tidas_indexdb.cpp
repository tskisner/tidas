/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;



indexdb_object * tidas::indexdb_dict::clone () {
	return new indexdb_dict ( *this );
}


indexdb_object * tidas::indexdb_schema::clone () {
	return new indexdb_schema ( *this );
}


indexdb_object * tidas::indexdb_group::clone () {
	return new indexdb_group ( *this );
}


indexdb_object * tidas::indexdb_intervals::clone () {
	return new indexdb_intervals ( *this );
}


indexdb_object * tidas::indexdb_block::clone () {
	return new indexdb_block ( *this );
}



tidas::indexdb_transaction::indexdb_transaction () {

}


tidas::indexdb_transaction::~indexdb_transaction () {
}


indexdb_transaction & tidas::indexdb_transaction::operator= ( indexdb_transaction const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb_transaction::indexdb_transaction ( indexdb_transaction const & other ) {
	copy ( other );
}


void tidas::indexdb_transaction::copy ( indexdb_transaction const & other ) {
	op = other.op;
	obj.reset ( other.obj->clone() );
	return;
}



tidas::indexdb::indexdb () {

}


tidas::indexdb::~indexdb () {
}


indexdb & tidas::indexdb::operator= ( indexdb const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb::indexdb ( indexdb const & other ) {
	copy ( other );
}


void tidas::indexdb::copy ( indexdb const & other ) {
	path_ = other.path_;
	history_ = other.history_;
	return;
}


tidas::indexdb::indexdb ( string const & path ) {
	path_ = path;

	// load from disk


}



CEREAL_REGISTER_TYPE( tidas::indexdb_dict );
CEREAL_REGISTER_TYPE( tidas::indexdb_schema );
CEREAL_REGISTER_TYPE( tidas::indexdb_group );
CEREAL_REGISTER_TYPE( tidas::indexdb_intervals );
CEREAL_REGISTER_TYPE( tidas::indexdb_block );




