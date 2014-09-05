/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.h>

#include <float.h>
#include <math.h>


#define NINT 10
#define SPAN 123.4
#define GAP 1.0


void tidas_test_intervals ( ) {
	size_t i;
	tidas_intervals * intervals;
	tidas_intervals * copy;
	tidas_intrvl const * cursor;
	tidas_intrvl const * cur1;
	tidas_intrvl const * cur2;
	tidas_intrvl intrvl;
	double start;
	double stop;
	double time;
	char testfile[ TIDAS_PATH_LEN ];

	/* test vector ops */

	fprintf ( stderr, "Testing tidas_intervals operations\n" );

	intervals = tidas_intervals_alloc ();

	for ( i = 0; i < NINT; ++i ) {
		start = GAP + (double)i * ( SPAN + GAP );
		stop = (double)(i + 1) * ( SPAN + GAP );
		intrvl.start = start;
		intrvl.stop = stop;
		tidas_intervals_append ( intervals, &intrvl );
	}

	for ( i = 0; i < NINT; ++i ) {
		start = GAP + (double)i * ( SPAN + GAP );
		stop = (double)(i + 1) * ( SPAN + GAP );
		cursor = tidas_intervals_get ( intervals, i );
		if ( ( fabs ( cursor->start - start ) > DBL_EPSILON ) || ( fabs ( cursor->stop - stop ) > DBL_EPSILON ) ) {
			fprintf( stderr, "  failed consistency on interval append / get\n" );
			exit(0);
		}
	}

	/* test time before first interval */

	cursor = tidas_intervals_seek ( intervals, 0.0 );
	if ( cursor ) {
		fprintf( stderr, "  failed interval seek before first interval\n" );
		exit(0);
	}

	for ( i = 0; i < NINT; ++i ) {
		start = GAP + (double)i * ( SPAN + GAP );
		stop = (double)(i + 1) * ( SPAN + GAP );
		/* test time in the middle of the interval */
		time = 0.5 * ( start + stop );
		cursor = tidas_intervals_seek ( intervals, time );
		if ( ( fabs ( cursor->start - start ) > DBL_EPSILON ) || ( fabs ( cursor->stop - stop ) > DBL_EPSILON ) ) {
			fprintf( stderr, "  failed consistency on interval seek\n" );
			exit(0);
		}
	}

	/* test time after last interval */

	stop = (double)(NINT) * ( SPAN + GAP ) + GAP;
	cursor = tidas_intervals_seek ( intervals, stop );
	if ( cursor ) {
		fprintf( stderr, "  failed interval seek after last interval\n" );
		exit(0);
	}

	/*
	FIXME: implement tests

	tidas_intrvl const * tidas_intervals_seek_ceil ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );

	tidas_intrvl const * tidas_intervals_seek_floor ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );
	*/

	strcpy ( testfile, "test_intervals.hdf5.out" );

	copy = tidas_intervals_copy ( intervals );

	tidas_intervals_write ( intervals, testfile, TIDAS_BACKEND_HDF5 );

	tidas_intervals_read ( intervals, testfile, TIDAS_BACKEND_HDF5 );

	for ( i = 0; i < NINT; ++i ) {
		cur1 = tidas_intervals_get ( intervals, i );
		cur2 = tidas_intervals_get ( copy, i );
		if ( ( fabs ( cur1->start - cur2->start ) > DBL_EPSILON ) || ( fabs ( cur1->stop - cur2->stop ) > DBL_EPSILON ) ) {
			fprintf( stderr, "  failed consistency on interval HDF5 write / read\n" );
			exit(0);
		}
	}

	tidas_intervals_free ( intervals );
	tidas_intervals_free ( copy );

	fprintf ( stderr, "  (PASS)\n" );


	return;
}


