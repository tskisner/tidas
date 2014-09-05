/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INTERVALS_H
#define TIDAS_INTERVALS_H


/* a single time interval */

typedef struct {
	TIDAS_DTYPE_TIME start;
	TIDAS_DTYPE_TIME stop;
} tidas_intrvl;

void tidas_intrvl_init ( void * addr );

void tidas_intrvl_clear ( void * addr );

void tidas_intrvl_copy ( void * dest, void const * src );

int tidas_intrvl_comp ( void const * addr1, void const * addr2 );


/* a collection of intervals */

typedef struct {
	tidas_vector * data;
} tidas_intervals;

tidas_intervals * tidas_intervals_alloc ();

void tidas_intervals_clear ( tidas_intervals * intervals );

void tidas_intervals_free ( tidas_intervals * intervals );

tidas_intervals * tidas_intervals_copy ( tidas_intervals const * orig );

void tidas_intervals_append ( tidas_intervals * intervals, tidas_intrvl const * intrvl );

tidas_intrvl const * tidas_intervals_get ( tidas_intervals const * intervals, size_t indx );

tidas_intrvl const * tidas_intervals_seek ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );

tidas_intrvl const * tidas_intervals_seek_ceil ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );

tidas_intrvl const * tidas_intervals_seek_floor ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );

void tidas_intervals_read ( tidas_intervals * intervals, char const * path, tidas_backend backend );

void tidas_intervals_write ( tidas_intervals const * intervals, char const * path, tidas_backend backend );

void tidas_intervals_read_hdf5 ( tidas_intervals * intervals, char const * path );

void tidas_intervals_write_hdf5 ( tidas_intervals const * intervals, char const * path );



#endif
