/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_GROUP_H
#define TIDAS_GROUP_H


typedef struct {
	char name[ TIDAS_NAME_LEN ];
	tidas_backend backend;
	tidas_schema * schema;
	void * backdat;
} tidas_group;

void tidas_group_init ( void * addr );

void tidas_group_clear ( void * addr );

void tidas_group_copy ( void * dest, void const * src );

int tidas_group_comp ( void const * addr1, void const * addr2 );


tidas_group * tidas_group_alloc ( char const * name, tidas_schema * schema, tidas_backend backend );

void tidas_group_free ( tidas_group * group );

tidas_group * tidas_group_copy ( tidas_group const * orig );


void tidas_group_mem_free ( tidas_group * group );

void tidas_group_hdf5_free ( tidas_group * group );

void tidas_group_getdata_free ( tidas_group * group );


#endif
