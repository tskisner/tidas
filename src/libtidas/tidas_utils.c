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
	fprintf ( stderr, "TIDAS ERROR %d:  %s in file %s, line %d -- %s\n", errcode, file, line, errorstr, msg );
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


char ** tidas_string_list ( size_t n, size_t len ) {
	size_t i, j;
	char ** array = NULL;
	if ( n == 0 ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate string list of size zero", NULL );
	}
	array = (char **) calloc (n, sizeof(char *) );
	if ( ! array ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate string list", NULL );
	}
	for (i = 0; i < n; i++) {
		array[i] = NULL;
		array[i] = (char *) calloc ( len, sizeof(char) );
		if ( ! array[i] ) {
			for ( j = 0; j < i; ++j ) {
				free( array[j] );
			}
			free( array );
			TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate string list element", NULL );
		}
	}
	return array;
}


int tidas_string_list_free ( char ** array, size_t n ) {
	size_t i;
	if ( ( array == NULL ) || ( n == 0 ) ) {
		return 0;
	}
	for ( i = 0; i < n; ++i ) {
		free( array[i] );
	}
	free( array );
	return 0;
}



