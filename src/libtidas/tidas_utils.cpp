/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <cstdlib>

extern "C" {
	#include <unistd.h>
	#include <sys/stat.h>
	#include <sys/types.h>
}

using namespace std;
using namespace tidas;


tidas::exception::exception ( const char * msg, const char * file, int line ) : std::exception () {
	snprintf ( msg_, msg_len_, "Exeption at line %d of file %s:  %s", line, file, msg );
	return;
}


tidas::exception::~exception ( ) throw () {
	return;
}


const char * tidas::exception::what() const throw() { 
	return msg_;
}


int64_t tidas::fs_stat ( char const * path ) {
	int64_t size = -1;
	struct stat filestat;
	int ret;

	ret = ::stat ( path, &filestat );

	if ( ret == 0 ) {
		/* we found the file, get props */
		size = static_cast < int64_t > ( filestat.st_size );
	}
	return size;
}


void tidas::fs_rm ( char const * path ) {
	int ret;
	int64_t size;

	size = fs_stat ( path );
	if ( size > 0 ) {
		ret = ::unlink ( path );
	}

	return;
}


void tidas::fs_mkdir ( char const * path ) {
	int ret;
	int64_t size;

	size = fs_stat ( path );

	if ( size <= 0 ) {
		ret = ::mkdir ( path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH );
	}

	return;
}


data_type tidas::data_type_get ( type_info const & test ) {
	data_type ret = TYPE_NONE;

	int8_t type_int8;
	uint8_t type_uint8;
	int16_t type_int16;
	uint16_t type_uint16;
	int32_t type_int32;
	uint32_t type_uint32;
	int64_t type_int64;
	uint64_t type_uint64;
	float type_float;
	double type_double;
	string type_string;

	if ( test == typeid ( type_int8 ) ) {
		ret = TYPE_INT8;
	} else if ( test == typeid ( type_uint8 ) ) {
		ret = TYPE_UINT8;
	} else if ( test == typeid ( type_int16 ) ) {
		ret = TYPE_INT16;
	} else if ( test == typeid ( type_uint16 ) ) {
		ret = TYPE_UINT16;
	} else if ( test == typeid ( type_int32 ) ) {
		ret = TYPE_INT32;
	} else if ( test == typeid ( type_uint32 ) ) {
		ret = TYPE_UINT32;
	} else if ( test == typeid ( type_int64 ) ) {
		ret = TYPE_INT64;
	} else if ( test == typeid ( type_uint64 ) ) {
		ret = TYPE_UINT64;
	} else if ( test == typeid ( type_float ) ) {
		ret = TYPE_FLOAT32;
	} else if ( test == typeid ( type_double ) ) {
		ret = TYPE_FLOAT64;
	} else if ( test == typeid ( type_string ) ) {
		ret = TYPE_STRING;
	} else {
		ret = TYPE_NONE;
	}

	return ret;
}


string tidas::data_type_to_string ( data_type type ) {
	string ret;
	switch ( type ) {
		case TYPE_INT8:
			ret = "TYPE_INT8";
			break;
		case TYPE_UINT8:
			ret = "TYPE_UINT8";
			break;
		case TYPE_INT16:
			ret = "TYPE_INT16";
			break;
		case TYPE_UINT16:
			ret = "TYPE_UINT16";
			break;
		case TYPE_INT32:
			ret = "TYPE_INT32";
			break;
		case TYPE_UINT32:
			ret = "TYPE_UINT32";
			break;
		case TYPE_INT64:
			ret = "TYPE_INT64";
			break;
		case TYPE_UINT64:
			ret = "TYPE_UINT64";
			break;
		case TYPE_FLOAT32:
			ret = "TYPE_FLOAT32";
			break;
		case TYPE_FLOAT64:
			ret = "TYPE_FLOAT64";
			break;
		case TYPE_STRING:
			ret = "TYPE_STRING";
			break;
		case TYPE_NONE:
			ret = "TYPE_NONE";
			break;
		default:
			TIDAS_THROW( "data type not recognized" );
			break;
	}
	return ret;
}


data_type tidas::data_type_from_string ( string const & name ) {

	data_type ret;
	
	if ( name.compare( "TYPE_INT8" ) == 0 ) {
		ret = TYPE_INT8;
	} else if ( name.compare( "TYPE_UINT8" ) == 0 ) {
		ret = TYPE_UINT8;
	} else if ( name.compare( "TYPE_INT16" ) == 0 ) {
		ret = TYPE_INT16;
	} else if ( name.compare( "TYPE_UINT16" ) == 0 ) {
		ret = TYPE_UINT16;
	} else if ( name.compare( "TYPE_INT32" ) == 0 ) {
		ret = TYPE_INT32;
	} else if ( name.compare( "TYPE_UINT32" ) == 0 ) {
		ret = TYPE_UINT32;
	} else if ( name.compare( "TYPE_INT64" ) == 0 ) {
		ret = TYPE_INT64;
	} else if ( name.compare( "TYPE_UINT64" ) == 0 ) {
		ret = TYPE_UINT64;
	} else if ( name.compare( "TYPE_FLOAT32" ) == 0 ) {
		ret = TYPE_FLOAT32;
	} else if ( name.compare( "TYPE_FLOAT64" ) == 0 ) {
		ret = TYPE_FLOAT64;
	} else if ( name.compare( "TYPE_STRING" ) == 0 ) {
		ret = TYPE_STRING;
	} else if ( name.compare( "TYPE_NONE" ) == 0 ) {
		ret = TYPE_NONE;
	} else {
		ostringstream o;
		o << "data type \"" << name << "\" not recognized";
		TIDAS_THROW( o.str().c_str() );
	}
	return ret;
}


template <>
std::string tidas::data_convert < std::string > ( std::string const & str ) {
	return str;
}


string tidas::filter_default ( string const & filter ) {
	string ret = filter;
	if ( ret == "" ) {
		ret = ".*";
	}
	return ret;
}


//[grp=.*[schm=field.*,dict=prop.*],intr=base.*[dict=blah*]]/2012.* [grp=.*[schm=field.*,dict=prop.*],intr=base.*]/13.* [grp=.*[schm=field.*,dict=prop.*],intr=base.*]/.*


// This splits filter of the form XXXX[XXXXXXX]

void tidas::filter_sub ( string const & filter, string & root, string & subfilter ) {

	root = "";
	subfilter = "";

	size_t off = 0;

	size_t pos = filter.find ( submatch_begin, off );

	if ( pos != string::npos ) {

		root = filter.substr ( off, (pos-off) );

		// we include the beginning character here
		subfilter = filter.substr ( pos, (filter.size()-pos) );

	} else {
		// we just have the root filter
		root = filter;
	}

	return;
}


// This splits filter of the form XXXX/XXXX

void tidas::filter_block ( string const & filter, string & local, string & subfilter ) {

	local = "";
	subfilter = "";

	size_t off = 0;

	size_t pos = filter.find ( path_sep, off );

	if ( pos != string::npos ) {

		local = filter.substr ( off, (pos-off) );

		off = pos + 1;

		subfilter = filter.substr ( off, (pos-off) );

	} else {
		// we just have a local filter
		local = filter;
	}

	return;
}


// This splits a filter of form [XXX=XXX,XXX=XXX]

map < string, string > tidas::filter_split ( string const & filter ) {
	map < string, string > ret;

	if ( filter != "" ) {

		// check enclosing chars

		if ( filter.compare ( 0, 1, submatch_begin ) != 0 ) {
			TIDAS_THROW( "filter does not start with correct character" );
		}

		if ( filter.compare ( (filter.size() - 1), 1, submatch_end ) != 0 ) {
			TIDAS_THROW( "filter does not end with correct character" );
		}

		// search through string and build up sub filters

		string inside = filter.substr ( 1, (filter.size() - 2) );

		size_t off = 0;

		size_t pos;

		string keyval;
		string key;
		string val;

		size_t assign;

		while ( ( pos = inside.find ( submatch_sep, off ) ) != string::npos ) {

			keyval = inside.substr ( off, (pos-off) );

			assign = keyval.find ( submatch_assign );

			if ( assign == string::npos ) {
				ostringstream o;
				o << "filter string sub-match must include the \"" << submatch_assign << "\" character";
				TIDAS_THROW( o.str().c_str() );
			}

			key = keyval.substr ( 0, assign );
			val = keyval.substr ( (assign+1), (keyval.size() - assign) );

			ret[ key ] = val;

			off = pos + 1;

		}

		// last entry

		keyval = inside.substr ( off, (inside.size() - off) );

		assign = keyval.find ( submatch_assign );

		if ( assign == string::npos ) {
			ostringstream o;
			o << "filter string sub-match must include the \"" << submatch_assign << "\" character";
			TIDAS_THROW( o.str().c_str() );
		}

		key = keyval.substr ( 0, assign );
		val = keyval.substr ( (assign+1), (keyval.size() - assign) );

		ret[ key ] = val;

	}
	return ret;
}



