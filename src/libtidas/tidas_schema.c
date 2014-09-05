/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.h>



void tidas_field_init ( void * addr ) {

	tidas_field * elem = (tidas_field *)addr;

	elem->type = TIDAS_TYPE_NONE;

	strcpy ( elem->name, "" );
	strcpy ( elem->units, "" );

	return;
}


void tidas_field_clear ( void * addr ) {

	tidas_field * elem = (tidas_field *)addr;

	elem->type = TIDAS_TYPE_NONE;

	strcpy ( elem->name, "" );
	strcpy ( elem->units, "" );

	return;
}


void tidas_field_copy ( void * dest, void const * src ) {

	tidas_field * elem_dest = (tidas_field *)dest;

	tidas_field const * elem_src = (tidas_field *)src;

	elem_dest->type = elem_src->type;
	strncpy ( elem_dest->name, elem_src->name, TIDAS_NAME_LEN );
	strncpy ( elem_dest->units, elem_src->units, TIDAS_NAME_LEN );

	return;
}


int tidas_field_comp ( void const * addr1, void const * addr2 ) {
	int ret;
	tidas_field const * elem1 = (tidas_field *)addr1;
	tidas_field const * elem2 = (tidas_field *)addr2;

	ret = strncmp ( elem1->name, elem2->name, TIDAS_NAME_LEN );

	if ( ret == 0 ) {
		if ( elem1->type != elem2->type ) {
			ret = -1;
		} else {
			ret = strncmp ( elem1->name, elem2->name, TIDAS_NAME_LEN );
		}
	}
	return ret;
}


tidas_schema * tidas_schema_alloc () {
	tidas_schema * schema = (tidas_schema *) malloc ( sizeof( tidas_schema ) );
	if ( ! schema ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate tidas schema", NULL );
	}

	tidas_vector_ops ops;
	ops.size = sizeof( tidas_field );
	ops.init = tidas_field_init;
	ops.clear = tidas_field_clear;
	ops.copy = tidas_field_copy;

	schema->data = tidas_vector_alloc ( ops );

	return schema;
}


void tidas_schema_free ( tidas_schema * schema ) {
	if ( schema ) {
		tidas_vector_free ( schema->data );
		free ( schema );
	}
	return;
}


void tidas_schema_append ( tidas_schema * schema, tidas_field const * field ) {
	size_t cursize;
	size_t newsize;
	void * cursor;

	TIDAS_PTR_CHECK( schema );

	cursize = schema->data->n;
	newsize = cursize + 1;

	tidas_vector_resize ( schema->data, newsize );

	cursor = tidas_vector_set ( schema->data, cursize );

	tidas_intrvl_copy ( cursor, (void const *) field );

	return;
}


tidas_field const * tidas_schema_lookup ( tidas_schema const * schema, char const * name ) {
	size_t i;
	tidas_field const * cursor = NULL;

	TIDAS_PTR_CHECK( schema );

	for ( i = 0; i < schema->data->n; ++i ) {
		cursor = (tidas_field const *) tidas_vector_get ( schema->data, i );
		if ( strncmp ( cursor->name, name, TIDAS_NAME_LEN ) == 0 ) {
			break;
		}
	}

	return cursor;
}



