/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.h>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>


/* global error handling function */

static tidas_error_handler_t * tidas_error_handler = NULL;


/* set default error handler */

tidas_error_handler_t * tidas_set_error_handler ( tidas_error_handler_t * newhandler ) {
	tidas_error_handler_t * oldhandler;
	oldhandler = tidas_error_handler;
	tidas_error_handler = newhandler;
	return oldhandler;
}


/* default error handler - print message and abort */

void tidas_error_default ( tidas_error_code errcode, char const * file, int line, char const * msg ) {
	char errorstr [ TIDAS_ERR_LEN ];

	switch ( errcode ) {
	case TIDAS_ERR_NONE:
		return;
		break;
	case TIDAS_ERR_ALLOC:
		strcpy ( errorstr, "Memory allocation error" );
		break;
	case TIDAS_ERR_FREE:
		strcpy ( errorstr, "Memory free error" );
		break;
	case TIDAS_ERR_PTR:
		strcpy ( errorstr, "Pointer is NULL" );
		break;
	case TIDAS_ERR_OPTION:
		strcpy ( errorstr, "Unsupported option" );
		break;
	case TIDAS_ERR_HDF5:
		strcpy ( errorstr, "HDF5 error" );
		break;
	case TIDAS_ERR_GETDATA:
		strcpy ( errorstr, "getdata error" );
		break;
	default:
		fprintf ( stderr, "Unknown error code" );
		return;
		break;
	}
	fprintf ( stderr, "TIDAS ERROR %d:  %s in file %s, line %d -- %s\n", (int)errcode, errorstr, file, line, msg );
	return;
}


void tidas_error ( tidas_error_code errcode, char const * file, int line, char const * msg ) {
	if ( errcode == 0 ) {
		return;
	}
	/* call user-defined error handler if it exists */
	if ( tidas_error_handler ) {
		(*tidas_error_handler) ( errcode, file, line, msg );
		return;
	}
	tidas_error_default ( errcode, file, line, msg );
	abort();
	return;
}


double * tidas_double_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(double) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate double mem buffer", NULL );
	}
	return (double*)temp;
}


float * tidas_float_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(float) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate float mem buffer", NULL );
	}
	return (float*)temp;
}


int * tidas_int_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(int) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate int mem buffer", NULL );
	}
	return (int*)temp;
}


long * tidas_long_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(long) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate long mem buffer", NULL );
	}
	return (long*)temp;
}


size_t * tidas_sizet_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(size_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate size_t mem buffer", NULL );
	}
	return (size_t*)temp;
}


int8_t * tidas_int8_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(int8_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate int8_t mem buffer", NULL );
	}
	return (int8_t*)temp;
}


uint8_t * tidas_uint8_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(uint8_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate uint8_t mem buffer", NULL );
	}
	return (uint8_t*)temp;
}


int16_t * tidas_int16_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(int16_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate int16_t mem buffer", NULL );
	}
	return (int16_t*)temp;
}


uint16_t * tidas_uint16_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(uint16_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate uint16_t mem buffer", NULL );
	}
	return (uint16_t*)temp;
}


int32_t * tidas_int32_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(int32_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate int32_t mem buffer", NULL );
	}
	return (int32_t*)temp;
}


uint32_t * tidas_uint32_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(uint32_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate uint32_t mem buffer", NULL );
	}
	return (uint32_t*)temp;
}


int64_t * tidas_int64_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(int64_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate int64_t mem buffer", NULL );
	}
	return (int64_t*)temp;
}


uint64_t * tidas_uint64_alloc ( size_t n ) {
	void * temp = malloc ( n * sizeof(uint64_t) );
	if ( ! temp ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate uint64_t mem buffer", NULL );
	}
	return (uint64_t*)temp;
}


int64_t tidas_fs_stat ( char const * path ) {
	int64_t size = -1;
	struct stat filestat;
	int ret;

	ret = stat ( path, &filestat );

	if ( ret == 0 ) {
		/* we found the file, get props */
		size = (int64_t)filestat.st_size;
	}
	return size;
}


void tidas_fs_rm ( char const * path ) {
	int ret;
	int64_t size;

	size = tidas_fs_stat ( path );
	if ( size > 0 ) {
		ret = unlink ( path );
	}

	return;
}


void tidas_fs_mkdir ( char const * path ) {
	int ret;
	int64_t size;

	size = tidas_fs_stat ( path );

	if ( size <= 0 ) {
		ret = mkdir ( path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH );
	}

	return;
}


tidas_vector * tidas_vector_alloc ( tidas_vector_ops ops ) {
	tidas_vector * vec = (tidas_vector *) malloc ( sizeof ( tidas_vector ) );
	if ( ! vec ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate tidas vector", NULL );
	}
	vec->n = 0;
	vec->ops = ops;
	vec->data = NULL;
	return vec;
}


tidas_vector * tidas_vector_copy ( tidas_vector const * orig ) {
	size_t i;
	void const * src;
	void * dst;

	TIDAS_PTR_CHECK( orig );

	tidas_vector * vec = tidas_vector_alloc ( orig->ops );	

	tidas_vector_resize ( vec, orig->n );

	for ( i = 0; i < vec->n; ++i ) {
		/* deep copy each element */
		src = tidas_vector_get ( orig, i );
		dst = tidas_vector_set ( vec, i );
		(*(vec->ops.copy))( dst, src );
	}

	return vec;
}


void tidas_vector_clear ( tidas_vector * vec ) {
	size_t i;
	void * cursor;

	TIDAS_PTR_CHECK( vec );

	if ( vec->n > 0 ) {
		if ( vec->data ) {

			for ( i = 0; i < vec->n; ++i ) {
				/* clear each element */
				cursor = tidas_vector_set ( vec, i );
				(*(vec->ops.clear))( cursor );
			}

			free ( vec->data );
			vec->data = NULL;
			vec->n = 0;

		} else {
			TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on free, tidas vector corrupted (data == null, but n > 0)" );
		}
	} else {
		if ( vec->data ) {
			TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on clear, tidas vector size == 0, but data not null" );
		}
	}

	return;
}


void tidas_vector_free ( tidas_vector * vec ) {
	size_t i;
	void * cursor;

	if ( vec ) {
		tidas_vector_clear ( vec );
		free ( vec );
	}
	return;
}


void tidas_vector_resize ( tidas_vector * vec, size_t newsize ) {
	size_t i;
	void * cursor;

	TIDAS_PTR_CHECK( vec );

	if ( newsize == vec->n ) {
		return;
	}

	if ( newsize == 0 ) {
		/* we are clearing all elements */

		tidas_vector_clear ( vec );

	} else {

		if ( newsize > vec->n ) {
			/* we need to realloc and initialize the new elements */

			vec->data = (void *) realloc ( vec->data, newsize * vec->ops.size );
			if ( ! vec->data ) {
				TIDAS_ERROR_VOID( TIDAS_ERR_ALLOC, "cannot resize tidas vector" );
			}

			for ( i = vec->n; i < newsize; ++i ) {
				/* initialize each new element */
				cursor = tidas_vector_set ( vec, i );
				(*(vec->ops.init))( cursor );
			}

			vec->n = newsize;

		} else {
			/* we are truncating the vector and need to clear elements that are cut */

			for ( i = newsize; i < vec->n; ++i ) {
				/* clear each element before realloc */
				cursor = tidas_vector_set ( vec, i );
				(*(vec->ops.clear))( cursor );
			}

			vec->data = (void *) realloc ( vec->data, newsize * vec->ops.size );
			if ( ! vec->data ) {
				TIDAS_ERROR_VOID( TIDAS_ERR_ALLOC, "cannot resize tidas vector" );
			}

			vec->n = newsize;

		}

	}

	return;
}


void * tidas_vector_set ( tidas_vector * vec, size_t elem ) {
	size_t cursor;

	TIDAS_PTR_CHECK( vec );

	cursor = elem * vec->ops.size;
	return (void*) &( ((char*)vec->data)[ cursor ] );
}


void const * tidas_vector_get ( tidas_vector const * vec, size_t elem ) {
	size_t cursor;

	TIDAS_PTR_CHECK( vec );

	cursor = elem * vec->ops.size;
	return (void const *) &( ((char const *)vec->data)[ cursor ] );
}


void * tidas_vector_seek_set ( tidas_vector * vec, void const * target ) {
	size_t i;
	int result;
	void * cursor = NULL;

	TIDAS_PTR_CHECK( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cursor = tidas_vector_set ( vec, i );
		result = (*(vec->ops.comp))( target, cursor );
		if ( result == 0 ) {
			break;
		}
	}

	return cursor;
}


void const * tidas_vector_seek_get ( tidas_vector const * vec, void const * target ) {
	size_t i;
	int result;
	void const * cursor = NULL;

	TIDAS_PTR_CHECK( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cursor = tidas_vector_get ( vec, i );
		result = (*(vec->ops.comp))( target, cursor );
		if ( result == 0 ) {
			break;
		}
	}

	return cursor;
}


void tidas_vector_view ( tidas_vector const * vec, TIDAS_VECTOR_VIEW viewer, void * props ) {
	size_t i;
	void const * cursor;

	TIDAS_PTR_CHECK( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cursor = tidas_vector_get ( vec, i );
		(*viewer)( cursor, i, props );
	}
	return;
}


void tidas_vector_process ( tidas_vector * vec, TIDAS_VECTOR_PROC processor, void * props ) {
	size_t i;
	void * cursor;

	TIDAS_PTR_CHECK( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cursor = tidas_vector_set ( vec, i );
		(*processor)( cursor, i, props );
	}
	return;
}



