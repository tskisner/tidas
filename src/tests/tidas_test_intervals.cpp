/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.h>


#define NINT 10
#define SPAN 123.4
#define GAP 1.0

#ifdef HAVE_HDF5
#include <hdf5.h>
#endif


void intervals_manip_setup () {
	return;
}

void intervals_manip_teardown () {
	return;
}

START_TEST(intervals_manip)
{
	size_t i;
	tidas_intervals * intervals;
	tidas_intrvl const * cursor;
	tidas_intrvl intrvl;
	double start;
	double stop;
	double time;

	fprintf ( stderr, "  Testing tidas_intervals manipulation\n" );

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
		ck_assert_msg ( ( check_dbl_eq( cursor->start, start ) ) && ( check_dbl_eq( cursor->stop, stop ) ), "    failed consistency on interval %d append / get", (int)i );
	}

	/* test time before first interval */

	cursor = tidas_intervals_seek ( intervals, 0.0 );
	ck_assert_msg ( ! cursor, "    failed interval seek before first interval" );

	for ( i = 0; i < NINT; ++i ) {
		start = GAP + (double)i * ( SPAN + GAP );
		stop = (double)(i + 1) * ( SPAN + GAP );
		/* test time in the middle of the interval */
		time = 0.5 * ( start + stop );
		cursor = tidas_intervals_seek ( intervals, time );
		ck_assert_msg ( ( check_dbl_eq( cursor->start, start ) ) && ( check_dbl_eq( cursor->stop, stop ) ), "    failed consistency on interval %d seek", (int)i );
	}

	/* test time after last interval */

	stop = (double)(NINT) * ( SPAN + GAP ) + GAP;
	cursor = tidas_intervals_seek ( intervals, stop );
	ck_assert_msg ( ! cursor, "    failed interval seek after last interval" );

	/*
	FIXME: implement tests

	tidas_intrvl const * tidas_intervals_seek_ceil ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );

	tidas_intrvl const * tidas_intervals_seek_floor ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time );
	*/

	tidas_intervals_free ( intervals );

}
END_TEST


void intervals_hdf5_setup () {
	return;
}

void intervals_hdf5_teardown () {
	return;
}

START_TEST(intervals_hdf5)
{

#ifdef HAVE_HDF5

	size_t i;
	tidas_intervals * intervals;
	tidas_intervals * copy;
	tidas_intrvl const * cur1;
	tidas_intrvl const * cur2;
	tidas_intrvl intrvl;
	double start;
	double stop;
	hid_t file;
	herr_t status;

	char testfile[ TIDAS_PATH_LEN ];

	fprintf ( stderr, "  Testing tidas_intervals HDF5 I/O\n" );

	intervals = tidas_intervals_alloc ();

	for ( i = 0; i < NINT; ++i ) {
		start = GAP + (double)i * ( SPAN + GAP );
		stop = (double)(i + 1) * ( SPAN + GAP );
		intrvl.start = start;
		intrvl.stop = stop;
		tidas_intervals_append ( intervals, &intrvl );
	}

	strcpy ( testfile, "test_intervals.hdf5.out" );

	copy = tidas_intervals_copy ( intervals );

	/* delete file if it exists */

	tidas_fs_rm ( testfile );

	file = H5Fcreate ( testfile, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );
	status = H5Fclose ( file );

	tidas_intervals_write ( intervals, TIDAS_BACKEND_HDF5, testfile, "", "intervals1" );
	tidas_intervals_write ( intervals, TIDAS_BACKEND_HDF5, testfile, "", "intervals2" );
	tidas_intervals_write ( intervals, TIDAS_BACKEND_HDF5, testfile, "", "intervals3" );

	tidas_intervals_read ( intervals, TIDAS_BACKEND_HDF5, testfile, "", "intervals2" );

	for ( i = 0; i < NINT; ++i ) {
		cur1 = tidas_intervals_get ( intervals, i );
		cur2 = tidas_intervals_get ( copy, i );
		ck_assert_msg ( ( check_dbl_eq( cur1->start, cur2->start ) ) && ( check_dbl_eq( cur1->stop, cur2->stop ) ), "    failed consistency on interval %d HDF5 write / read", (int)i );
	}

	tidas_intervals_free ( intervals );
	tidas_intervals_free ( copy );

#else

	fprintf ( stderr, "  Skipping tidas_intervals HDF5 I/O (backend disabled)\n" );

#endif

}
END_TEST


Suite * make_suite_intervals() {
	Suite * s;
	TCase * tc_manip;
	TCase * tc_hdf5;
	TCase * tc_getdata;

	s = suite_create ( "Intervals" );

	tc_manip = tcase_create ( "Manipulation" );
	tcase_add_checked_fixture ( tc_manip, intervals_manip_setup, intervals_manip_teardown );
	tcase_add_test ( tc_manip, intervals_manip );
	suite_add_tcase ( s, tc_manip );

	tc_hdf5 = tcase_create ( "HDF5" );
	tcase_add_checked_fixture ( tc_hdf5, intervals_hdf5_setup, intervals_hdf5_teardown );
	tcase_add_test ( tc_hdf5, intervals_hdf5 );
	suite_add_tcase ( s, tc_hdf5 );

	return s;
}











