/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.h>


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


tidas_vector * tidas_vector_alloc ( size_t elem_size ) {
	tidas_vector * vec = (tidas_vector *) malloc ( sizeof ( tidas_vector ) );
	if ( ! vec ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate tidas vector", NULL );
	}
	vec->n = 0;
	vec->elem_size = elem_size;
	vec->data = NULL;
	return vec;
}


tidas_vector * tidas_vector_copy ( tidas_vector const * orig ) {
	size_t i;
	void * dst;

	TIDAS_PTR_CHECK(orig);

	tidas_vector * vec = tidas_vector_alloc ( orig->elem_size );
	tidas_vector_resize ( vec, orig->n );
	dst = memcpy ( vec->data, orig->data, vec->elem_size * vec->n );

	return vec;
}


void tidas_vector_free ( tidas_vector * vec ) {
	if ( vec ) {
		if ( vec->n > 0 ) {
			if ( vec->data ) {
				free ( vec->data );
			} else {
				TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on free, tidas vector corrupted (data == null, but n > 0)" );
			}
		} else {
			if ( vec->data ) {
				TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on free, tidas vector size == 0, but data not null" );
			}
		}
		free ( vec );
	}
	return;
}


void tidas_vector_resize ( tidas_vector * vec, size_t newsize ) {

	TIDAS_PTR_CHECK(vec);

	if ( newsize > 0 ) {

		vec->data = (void *) realloc ( vec->data, newsize * vec->elem_size );
		if ( ! vec->data ) {
			TIDAS_ERROR_VOID( TIDAS_ERR_ALLOC, "cannot resize tidas vector" );
		}

	} else {
		/* just clear memory */

		if ( vec->n > 0 ) {
			if ( vec->data ) {
				free ( vec->data );
			} else {
				TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on resize, tidas vector corrupted (data == null, but n > 0)" );
			}
		} else {
			if ( vec->data ) {
				TIDAS_ERROR_VOID( TIDAS_ERR_FREE, "on resize, tidas vector size == 0, but data not null" );
			}
		}
		vec->n = 0;
		vec->data = NULL;
	}

	return;
}


void * tidas_vector_at ( tidas_vector * vec, size_t elem ) {
	size_t cursor;

	TIDAS_PTR_CHECK(vec);

	cursor = elem * vec->elem_size;
	return (void*) &( ((char*)vec->data)[ cursor ] );
}


