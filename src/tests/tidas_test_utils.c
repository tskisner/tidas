/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.h>


#define TEST_LEN 100
#define VEC_N 10

typedef struct {
	double * dval;
	int16_t i16val;
	float fval;
	int32_t i32val;
	char sval[ TEST_LEN ];
} test_elem;

void test_elem_init ( void * addr ) {
	size_t i;

	test_elem * elem = (test_elem *)addr;

	elem->dval = (double *) malloc ( VEC_N * sizeof( double ) );
	if ( ! elem->dval ) {
		fprintf ( stderr, "  failed to allocate test elem doubles\n" );
		exit(0);
	}

	for ( i = 0; i < VEC_N; ++i ) {
		elem->dval[i] = 0.0;
	}

	return;
}

void test_elem_clear ( void * addr ) {
	size_t i;

	test_elem * elem = (test_elem *)addr;

	for ( i = 0; i < VEC_N; ++i ) {
		elem->dval[i] = 0.0;
	}

	return;
}

void test_elem_copy ( void * dest, void const * src ) {
	size_t i;

	test_elem * elem_dest = (test_elem *)dest;

	test_elem const * elem_src = (test_elem const *)src;

	for ( i = 0; i < VEC_N; ++i ) {
		elem_dest->dval[i] = elem_src->dval[i];
	}

	elem_dest->i16val = elem_src->i16val;
	elem_dest->fval = elem_src->fval;
	elem_dest->i32val = elem_src->i32val;

	strncpy ( elem_dest->sval, elem_src->sval, TEST_LEN );

	return;
}


void tidas_test_utils ( ) {
	size_t i, j;
	int ret;
	test_elem * cursor;
	test_elem const * ccursor;
	char compare[ TEST_LEN ];

	/* test vector ops */

	fprintf ( stderr, "Testing tidas_vector operations\n" );

	tidas_vector_ops ops;
	ops.size = sizeof( test_elem );
	ops.init = test_elem_init;
	ops.clear = test_elem_clear;
	ops.copy = test_elem_copy;

	tidas_vector * vec = tidas_vector_alloc ( ops );

	tidas_vector_resize ( vec, 10 );

	for ( i = 0; i < vec->n; ++i ) {
		cursor = (test_elem *) tidas_vector_set ( vec, i );
		for ( j = 0; j < VEC_N; ++j ) {
			cursor->dval[j] = (double)i * (double)j;
		}
		cursor->i16val = (int16_t)i;
		cursor->fval = (float)i;
		cursor->i32val = (int32_t)i;
		ret = snprintf ( cursor->sval, TEST_LEN, "%d", (int)i );
	}

	tidas_vector * copy = tidas_vector_copy ( vec );

	tidas_vector_free ( vec );

	for ( i = 0; i < vec->n; ++i ) {
		ccursor = (test_elem const *) tidas_vector_get ( copy, i );
		for ( j = 0; j < VEC_N; ++j ) {
			if ( ccursor->dval[j] != (double)i * (double)j ) {
				fprintf( stderr, "  failed consistency on vector elem %d, double value %d (%f != %f)\n", (int)i, (int)j, ccursor->dval[j], (double)i*(double)j );
				exit(0);
			}
		}
		if ( ccursor->i16val != (int16_t)i ) {
			fprintf( stderr, "  failed consistency on vector elem %d, int16 value (%d != %d)\n", (int)i, (int)ccursor->i16val, (int)i );
			exit(0);
		}
		if ( ccursor->fval != (float)i ) {
			fprintf( stderr, "  failed consistency on vector elem %d, float value (%f != %f)\n", (int)i, ccursor->fval, (float)i );
			exit(0);
		}
		if ( ccursor->i32val != (int32_t)i ) {
			fprintf( stderr, "  failed consistency on vector elem %d, int32 value (%d != %d)\n", (int)i, ccursor->i32val, (int)i );
			exit(0);
		}
		ret = snprintf ( compare, TEST_LEN, "%d", (int)i );
		if ( strncmp ( ccursor->sval, compare, TEST_LEN ) != 0 ) {
			fprintf( stderr, "  failed consistency on vector elem %d, string value (%s != %s)\n", (int)i, ccursor->sval, compare );
			exit(0);
		}
	}

	tidas_vector_free ( copy );

	fprintf ( stderr, "  (PASS)\n" );


	return;
}


