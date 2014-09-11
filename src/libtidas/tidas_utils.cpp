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



