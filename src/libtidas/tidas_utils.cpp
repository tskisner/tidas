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


double * tidas::double_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(double) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate double mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (double*)temp;
}


float * tidas::float_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(float) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate float mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (float*)temp;
}


int * tidas::int_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(int) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate int mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (int*)temp;
}


long * tidas::long_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(long) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate long mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (long*)temp;
}


size_t * tidas::sizet_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(size_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate size_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (size_t*)temp;
}


int8_t * tidas::int8_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(int8_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate int8_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (int8_t*)temp;
}


uint8_t * tidas::uint8_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(uint8_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate uint8_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (uint8_t*)temp;
}


int16_t * tidas::int16_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(int16_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate int16_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (int16_t*)temp;
}


uint16_t * tidas::uint16_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(uint16_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate uint16_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (uint16_t*)temp;
}


int32_t * tidas::int32_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(int32_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate int32_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (int32_t*)temp;
}


uint32_t * tidas::uint32_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(uint32_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate uint32_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (uint32_t*)temp;
}


int64_t * tidas::int64_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(int64_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate int64_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (int64_t*)temp;
}


uint64_t * tidas::uint64_alloc ( size_t n ) {
	void * temp = ::malloc ( n * sizeof(uint64_t) );
	if ( ! temp ) {
		std::ostringstream o;
		o << "cannot allocate uint64_t mem buffer of size " << n;
		TIDAS_THROW( o.str().c_str() );
	}
	return (uint64_t*)temp;
}


int64_t tidas::fs_stat ( char const * path ) {
	int64_t size = -1;
	struct stat filestat;
	int ret;

	ret = ::stat ( path, &filestat );

	if ( ret == 0 ) {
		/* we found the file, get props */
		size = (int64_t)filestat.st_size;
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



