/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <regex>


using namespace std;
using namespace tidas;


tidas::field::field () {
	type = data_type::none;
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
	for ( auto fld : fields ) {
		if ( fld.type == type ) {
			ret.push_back ( fld );
		}
	}
	return ret;
}


tidas::schema::schema () {
	relocate ( loc_ );
}


tidas::schema::schema ( field_list const & fields ) {
	fields_ = fields;
	relocate ( loc_ );
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


void tidas::schema::set_backend () {

	switch ( loc_.type ) {
		case backend_type::none:
			backend_.reset();
			break;
		case backend_type::hdf5:
			backend_.reset( new schema_backend_hdf5 () );
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


void tidas::schema::relocate ( backend_path const & loc ) {

	loc_ = loc;
	set_backend();

	return;
}


void tidas::schema::sync () {

	// read our own metadata

	if ( loc_.type != backend_type::none ) {

		// if we have an index, use it!

		bool found = false;

		if ( loc_.idx ) {
			found = loc_.idx->query_schema ( loc_, fields_ );
		}

		if ( ! found ) {
			backend_->read ( loc_, fields_ );
			if ( loc_.idx && ( loc_.mode == access_mode::write ) ) {
				loc_.idx->add_schema ( loc_, fields_ );
			}
		}
	}

	return;
}


void tidas::schema::flush () const {

	if ( loc_.type != backend_type::none ) {

		if ( loc_.mode == access_mode::write ) {
			// write our own metadata

			backend_->write ( loc_, fields_ );

			// update index

			if ( loc_.idx ) {
				loc_.idx->update_schema ( loc_, fields_ );
			}
		}
	}

	return;
}


void tidas::schema::copy ( schema const & other, string const & filter, backend_path const & loc ) {	

	// extract filters

	string filt = filter_default ( filter );

	if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
		TIDAS_THROW( "copy of non-memory schema with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;
	set_backend();

	// filtered copy of our metadata.  Note that we *always* keep the time field.

	fields_.clear();

	regex re ( filt, std::regex::extended );

	for ( auto fld : other.fields_ ) {
		if ( regex_match ( fld.name, re ) || ( fld.name == group_time_field ) ) {
			fields_.push_back ( field( fld.name, fld.type, fld.units ) );
		}
	}

	// update index

	if ( loc_.idx && ( loc_.mode == access_mode::write ) && ( loc != other.loc_ ) ) {
		loc_.idx->update_schema ( loc_, fields_ );
	}

	return;
}


backend_path tidas::schema::location () const {
	return loc_;
}


void tidas::schema::clear () {
	fields_.clear();
	return;
}


void tidas::schema::field_add ( field const & fld ) {
	fields_.push_back ( fld );
	return;
}


void tidas::schema::field_del ( string const & name ) {
	field_list::iterator it;

	for ( it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			fields_.erase ( it );
			break;
		}
	}

	return;
}


field const & tidas::schema::field_get ( string const & name ) const {

	for ( field_list::const_iterator it = fields_.begin(); it != fields_.end(); ++it ) {
		if ( name == it->name ) {
			return (*it);
		}
	}

	ostringstream o;
	o << "schema does not contain field named \"" << name << "\"";
	TIDAS_THROW( o.str().c_str() );

	return fields_[0];
}


field_list const & tidas::schema::fields () const {
	return fields_;
}


