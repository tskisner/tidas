/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


/*

#define TEST_LEN 100
#define VEC_N 3
#define VEC_N_DOUBLE 3


typedef struct {
	double * dval;
	int16_t i16val;
	float fval;
	int32_t i32val;
	char sval[ TEST_LEN ];
	size_t indx;
} test_elem;


typedef struct {
	float factor;
} test_elem_assign_props;


void test_elem_init ( void * addr ) {
	size_t i;

	test_elem * elem = (test_elem *)addr;

	elem->dval = tidas_double_alloc ( VEC_N_DOUBLE );

	for ( i = 0; i < VEC_N_DOUBLE; ++i ) {
		elem->dval[i] = 0.0;
	}

	return;
}


void test_elem_clear ( void * addr ) {
	size_t i;

	test_elem * elem = (test_elem *)addr;

	for ( i = 0; i < VEC_N_DOUBLE; ++i ) {
		elem->dval[i] = 0.0;
	}

	return;
}


void test_elem_copy ( void * dest, void const * src ) {
	size_t i;

	test_elem * elem_dest = (test_elem *)dest;

	test_elem const * elem_src = (test_elem const *)src;

	for ( i = 0; i < VEC_N_DOUBLE; ++i ) {
		elem_dest->dval[i] = elem_src->dval[i];
	}

	elem_dest->i16val = elem_src->i16val;
	elem_dest->fval = elem_src->fval;
	elem_dest->i32val = elem_src->i32val;

	strncpy ( elem_dest->sval, elem_src->sval, TEST_LEN );

	return;
}


int test_elem_comp ( void const * addr1, void const * addr2 ) {
	size_t j;
	int ret = 0;
	test_elem const * cursor1;
	test_elem const * cursor2;

	cursor1 = (test_elem const *)addr1;
	cursor2 = (test_elem const *)addr2;

	if ( cursor1->i16val != cursor2->i16val ) {
		ret = -1;
	}
	if ( cursor1->i32val != cursor2->i32val ) {
		ret = -1;
	}
	if ( cursor1->fval != cursor2->fval ) {
		ret = -1;
	}
	for ( j = 0; j < VEC_N_DOUBLE; ++j ) {
		if ( cursor1->dval[j] != cursor2->dval[j] ) {
			ret = -1;
		}
	}
	return ret;
}


void test_elem_assign ( void * addr, size_t i, void * props ) {
	size_t j;
	float factor;
	int ret;
	test_elem * cursor;
	test_elem_assign_props * wrapper = (test_elem_assign_props *)props;
	
	factor = wrapper->factor;
	cursor = (test_elem *)addr;

	for ( j = 0; j < VEC_N_DOUBLE; ++j ) {
		cursor->dval[j] = (double)i * (double)factor;
	}
	cursor->i16val = (int16_t)i;
	cursor->fval = (float)i;
	cursor->i32val = (int32_t)i;
	cursor->indx = i;
	ret = snprintf ( cursor->sval, TEST_LEN, "%d", (int)i );

	return;
}


void test_elem_print ( void const * addr, size_t i, void * props ) {
	size_t j;
	test_elem const * cursor;

	cursor = (test_elem const *)addr;

	fprintf ( stderr, "    test elem %d:\n", (int)i );
	fprintf ( stderr, "     " );
	for ( j = 0; j < VEC_N_DOUBLE; ++j ) {
		fprintf ( stderr, " %f", cursor->dval[j] );
	}
	fprintf ( stderr, "\n" );
	fprintf ( stderr, "      (%d) %d %f %d %s\n", (int)cursor->indx, (int)cursor->i16val, cursor->fval, (int)cursor->i32val, cursor->sval );

	return;
}


void utils_mem_setup () {
	return;
}

void utils_mem_teardown () {
	return;
}

START_TEST(utils_mem)
{
	size_t i;
	size_t ntest = 10;
	double * test_double;
	float * test_float;
	int * test_int;
	long * test_long;
	size_t * test_sizet;
	int8_t * test_int8;
	uint8_t * test_uint8;
	int16_t * test_int16;
	uint16_t * test_uint16;
	int32_t * test_int32;
	uint32_t * test_uint32;
	int64_t * test_int64;
	uint64_t * test_uint64;

	fprintf ( stderr, "  Testing memory helper functions\n" );

	test_double = tidas_double_alloc ( ntest );
	test_float = tidas_float_alloc ( ntest );
	test_int = tidas_int_alloc ( ntest );
	test_long = tidas_long_alloc ( ntest );
	test_sizet = tidas_sizet_alloc ( ntest );
	test_int8 = tidas_int8_alloc ( ntest );
	test_uint8 = tidas_uint8_alloc ( ntest );
	test_int16 = tidas_int16_alloc ( ntest );
	test_uint16 = tidas_uint16_alloc ( ntest );
	test_int32 = tidas_int32_alloc ( ntest );
	test_uint32 = tidas_uint32_alloc ( ntest );
	test_int64 = tidas_int64_alloc ( ntest );
	test_uint64 = tidas_uint64_alloc ( ntest );

	for ( i = 0; i < ntest; ++i ) {
		test_double[i] = 1.234567890123456;
		test_float[i] = 1.234567;
		test_int8[i] = -123;
		test_uint8[i] = 123;
		test_int16[i] = -12345;
		test_uint16[i] = 12345;
		test_int32[i] = -1234567890;
		test_uint32[i] = 1234567890;
		test_int[i] = -1234567890;
		test_long[i] = -1234567890;
		test_sizet[i] = 1234567890;
		test_int64[i] = -123456789012345;
		test_uint64[i] = 123456789012345;
	}

	free ( test_double );
	free ( test_float );
	free ( test_int );
	free ( test_long );
	free ( test_sizet );
	free ( test_int8 );
	free ( test_uint8 );
	free ( test_int16 );
	free ( test_uint16 );
	free ( test_int32 );
	free ( test_uint32 );
	free ( test_int64 );
	free ( test_uint64 );

}
END_TEST


void utils_fs_setup () {
	return;
}

void utils_fs_teardown () {
	return;
}

START_TEST(utils_fs)
{
	int ret;
	int64_t size;
	char outdir[ TIDAS_PATH_LEN ];
	char outfile[ TIDAS_PATH_LEN ];
	char com[ TIDAS_PATH_LEN ];

	fprintf ( stderr, "  Testing filesystem operations\n" );

	strcpy ( outdir, "test.out" );

	tidas_fs_mkdir ( outdir );
	size = tidas_fs_stat ( outdir );
	ck_assert_msg ( size > 0, "mkdir failed" );

	strcpy ( outfile, "test_fs.out" );

	snprintf ( com, TEST_LEN, "echo test > %s/%s", outdir, outfile );
	ret = system( com );

	snprintf ( com, TEST_LEN, "%s/%s", outdir, outfile );

	size = tidas_fs_stat ( com );
	ck_assert_msg ( size > 0, "stat failed" );

	tidas_fs_rm ( com );
	size = tidas_fs_stat ( com );
	ck_assert_msg ( size != 0, "rm failed" );

	snprintf ( com, TEST_LEN, "rmdir %s", outdir );
	ret = system( com );
	
}
END_TEST


void utils_vector_setup () {
	return;
}

void utils_vector_teardown () {
	return;
}

START_TEST(utils_vector)
{

	size_t i, j;
	int ret;
	test_elem * cursor;
	test_elem const * ccursor;
	char compare[ TEST_LEN ];
	tidas_vector_ops ops;
	tidas_vector * vec;
	test_elem_assign_props props;
	test_elem const * cur1;
	test_elem const * cur2;

	// test vector ops

	fprintf ( stderr, "  Testing tidas_vector operations\n" );

	ops.size = sizeof( test_elem );
	ops.init = test_elem_init;
	ops.clear = test_elem_clear;
	ops.copy = test_elem_copy;
	ops.comp = test_elem_comp;

	vec = tidas_vector_alloc ( ops );

	tidas_vector_resize ( vec, VEC_N );

	props.factor = 1.5;

	tidas_vector_process ( vec, test_elem_assign, (void*)(&props) );

	tidas_vector_view ( vec, test_elem_print, NULL );

	tidas_vector * copy = tidas_vector_copy ( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cur1 = (test_elem const *) tidas_vector_get ( vec, i );
		cur2 = (test_elem const *) tidas_vector_seek_get ( vec, cur1 );
		ck_assert_msg ( cur2->indx == i, "    failed consistency on copy element %d", (int)i );
	}

	tidas_vector_view ( copy, test_elem_print, NULL );

	tidas_vector_free ( vec );

	tidas_vector_free ( copy );

}
END_TEST


Suite * make_suite_utils() {
	Suite * s;
	TCase * tc_mem;
	TCase * tc_fs;
	TCase * tc_vec;

	s = suite_create ( "Utilities" );

	tc_mem = tcase_create ( "Memory" );
	tcase_add_checked_fixture ( tc_mem, utils_mem_setup, utils_mem_teardown );
	tcase_add_test ( tc_mem, utils_mem );
	suite_add_tcase ( s, tc_mem );

	tc_fs = tcase_create ( "Filesystem" );
	tcase_add_checked_fixture ( tc_fs, utils_fs_setup, utils_fs_teardown );
	tcase_add_test ( tc_fs, utils_fs );
	suite_add_tcase ( s, tc_fs );

	tc_vec = tcase_create ( "Vector" );
	tcase_add_checked_fixture ( tc_vec, utils_vector_setup, utils_vector_teardown );
	tcase_add_test ( tc_vec, utils_vector );
	suite_add_tcase ( s, tc_vec );

	return s;
}

*/


