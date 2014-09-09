/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_SCHEMA_H
#define TIDAS_SCHEMA_H


/* one field of the schema */

typedef struct {
	tidas_type type;
	char name[ TIDAS_NAME_LEN ];
	char units[ TIDAS_NAME_LEN ];
} tidas_field;

void tidas_field_init ( void * addr );

void tidas_field_clear ( void * addr );

void tidas_field_copy ( void * dest, void const * src );

int tidas_field_comp ( void const * addr1, void const * addr2 );


/* a schema used for a group */

typedef struct {
	tidas_vector * data;
} tidas_schema;

tidas_schema * tidas_schema_alloc ();

void tidas_schema_clear ( tidas_schema * schema );

tidas_schema * tidas_schema_copy ( tidas_schema const * orig );

void tidas_schema_free ( tidas_schema * schema );

void tidas_schema_append ( tidas_schema * schema, tidas_field const * field );

tidas_field const * tidas_schema_lookup ( tidas_schema const * schema, char const * name );


#endif
