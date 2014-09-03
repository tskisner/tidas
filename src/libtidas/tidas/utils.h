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
	TIDAS_ERR_ALLOC,
	TIDAS_ERR_FREE,
	TIDAS_ERR_PTR
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

#define TIDAS_PTR_CHECK(handle) \
	if ( ! handle ) { \
		tidas_error (TIDAS_ERR_PTR, __FILE__, __LINE__, "pointer is NULL"); \
	}


/* a "vector" of memory blocks.  too bad we can't use libstdc+++ ... */

typedef struct {
	size_t n;
	size_t elem_size;
	void * data;
} tidas_vector;

tidas_vector * tidas_vector_alloc ( size_t elem_size );

tidas_vector * tidas_vector_copy ( tidas_vector const * orig );

void tidas_vector_free ( tidas_vector * vec );

void tidas_vector_resize ( tidas_vector * vec, size_t newsize );

void * tidas_vector_at ( tidas_vector * vec, size_t elem );




#endif
