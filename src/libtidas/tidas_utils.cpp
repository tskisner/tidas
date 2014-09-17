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


string tidas::filter_default ( string const & filter ) {
	string ret = filter;
	if ( ret == "" ) {
		ret = ".*";
	}
	return ret;
}



// format = "blahblah@one=foo:two=bar:three=blat@"
// returned as:
//   root == "blahblah"
//   one == "foo"
//   two == "bar"
//   three == "blat"

map < string, string > tidas::filter_split ( string const & filter ) {
	map < string, string > ret;

	if ( filter == "" ) {
		ret[ "root" ] = ".*";
	} else {

		// split root and sub object filters

		string match = "^(.+?)@(.+)@$";

		RE2 re ( match );

		string root;
		string subselect;

		if ( RE2::FullMatch ( filter, re, &root, &subselect ) ) {
			ret[ "root" ] = root;

			// search through string and build up sub filters

			size_t pos = 0;
			size_t next = 0;
			size_t len;

			while ( next != string::npos ) {

				size_t eq = subselect.find ( '=', pos );
				if ( eq == string::npos ) {
					TIDAS_THROW( "filter string sub-match must include an \"=\" character" );
				}

				len = eq - pos;
				string key = subselect.substr ( pos, len );

				pos = eq + 1;
				next = subselect.find ( ':', pos );

				string val;

				if ( next == string::npos ) {
					len = subselect.size() - pos;
					val = subselect.substr ( pos, len );
				} else {
					len = next - pos;
					val = subselect.substr ( pos, len );
					pos = next + 1;
				}

				ret[ key ] = val;

			}

		} else {
			ret[ "root" ] = filter;
		}

	}
	return ret;
}



