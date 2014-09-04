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

tidas_intrvl * tidas_intrvl_alloc ();

tidas_intrvl * tidas_intrvl_copy ( tidas_intrvl const * intrvl );

void tidas_intrvl_free ( tidas_intrvl * intrvl );


/* a collection of intervals */

typedef struct {
	size_t n;
	void * data;
} tidas_intervals;


tidas_intervals * tidas_intervals_alloc ();


void tidas_intervals_free ( tidas_intervals * intervals );


void tidas_intervals_append ( tidas_intervals * intervals );


tidas_intrvl const * tidas_interval_seek ( TIDAS_DTYPE_TIME time );


tidas_intrvl const * tidas_interval_seek_ceil ( TIDAS_DTYPE_TIME time );


tidas_intrvl const * tidas_interval_seek_floor ( TIDAS_DTYPE_TIME time );


#endif
