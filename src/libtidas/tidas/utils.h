/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_UTILS_H
#define TIDAS_UTILS_H


/* Error Handling */

typedef enum {
	TIDAS_ERR_NONE,
	TIDAS_ERR_ALLOC
} tidas_error_code;

typedef void tidas_error_handler_t ( tidas_error_code errcode, char const * file, int line, char const * msg );

void tidas_error ( tidas_error_code errcode, char const * file, int line, char const * msg );

void tidas_error_default ( tidas_error_code errcode, char const * file, int line, char const * msg );

tidas_error_handler_t * tidas_set_error_handler ( tidas_error_handler_t * newhandler );
  
#define TIDAS_ERROR(errcode, msg) \
	do { \
		tidas_error (errcode, __FILE__, __LINE__, msg); \
		return errcode; \
	} while (0)

#define TIDAS_ERROR_VAL(errcode, msg, value) \
	do { \
		tidas_error (errcode, __FILE__, __LINE__, msg); \
		return value; \
	} while (0)

#define TIDAS_ERROR_VOID(errcode, msg) \
	do { \
		tidas_error (errcode, __FILE__, __LINE__, msg); \
		return; \
	} while (0)


/* list of strings */

char ** tidas_string_list ( size_t n, size_t len );

int tidas_string_list_free ( char ** array, size_t n );


#endif
