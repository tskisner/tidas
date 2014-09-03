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

tidas_field * tidas_field_alloc ( tidas_type type, char const * name, char const * units );

tidas_field * tidas_field_copy ( tidas_field const * orig );

void tidas_field_free ( tidas_field * field );


/* a schema used for a group */

typedef struct {
	size_t n;
	tidas_field * fields;
} tidas_schema;

tidas_schema * tidas_schema_alloc ();

tidas_schema * tidas_schema_copy ( tidas_field const * orig );

void tidas_schema_free ( tidas_schema * schema );

void tidas_schema_append ( tidas_field const * field );


#endif
