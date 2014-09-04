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
	case TIDAS_ERR_FREE:
		strcpy ( errorstr, "Memory free error" );
		break;
	case TIDAS_ERR_PTR:
		strcpy ( errorstr, "Pointer is NULL" );
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

	TIDAS_PTR_CHECK(orig);

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

	TIDAS_PTR_CHECK(vec);

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

	TIDAS_PTR_CHECK(vec);

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

	TIDAS_PTR_CHECK(vec);

	cursor = elem * vec->ops.size;
	return (void*) &( ((char*)vec->data)[ cursor ] );
}


void const * tidas_vector_get ( tidas_vector const * vec, size_t elem ) {
	size_t cursor;

	TIDAS_PTR_CHECK(vec);

	cursor = elem * vec->ops.size;
	return (void const *) &( ((char const *)vec->data)[ cursor ] );
}


