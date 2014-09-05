/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.h>


#define TEST_LEN 100
#define VEC_N 3

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
	for ( j = 0; j < VEC_N; ++j ) {
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

	for ( j = 0; j < VEC_N; ++j ) {
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

	fprintf ( stderr, "  test elem %d:\n", (int)i );
	fprintf ( stderr, "   " );
	for ( j = 0; j < VEC_N; ++j ) {
		fprintf ( stderr, " %f", cursor->dval[j] );
	}
	fprintf ( stderr, "\n" );
	fprintf ( stderr, "    (%d) %d %f %d %s\n", (int)cursor->indx, (int)cursor->i16val, cursor->fval, (int)cursor->i32val, cursor->sval );

	return;
}



void tidas_test_utils ( ) {
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

	/* test vector ops */

	fprintf ( stderr, "Testing tidas_vector operations\n" );

	ops.size = sizeof( test_elem );
	ops.init = test_elem_init;
	ops.clear = test_elem_clear;
	ops.copy = test_elem_copy;
	ops.comp = test_elem_comp;

	vec = tidas_vector_alloc ( ops );

	tidas_vector_resize ( vec, 3 );

	props.factor = 1.5;

	tidas_vector_process ( vec, test_elem_assign, (void*)(&props) );

	tidas_vector_view ( vec, test_elem_print, NULL );

	tidas_vector * copy = tidas_vector_copy ( vec );

	for ( i = 0; i < vec->n; ++i ) {
		cur1 = (test_elem const *) tidas_vector_get ( vec, i );
		cur2 = (test_elem const *) tidas_vector_seek_get ( vec, cur1 );
		if ( cur2->indx != i ) {
			fprintf( stderr, "  failed consistency on copy\n" );
			exit(0);
		}
	}

	tidas_vector_view ( copy, test_elem_print, NULL );

	tidas_vector_free ( vec );

	tidas_vector_free ( copy );

	fprintf ( stderr, "  (PASS)\n" );


	return;
}


