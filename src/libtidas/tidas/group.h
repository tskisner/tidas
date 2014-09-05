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


tidas_group * tidas_group_alloc ( char const * name, tidas_backend backend );


tidas_volume * tidas_volume_open ( char const * path );


void tidas_volume_close ( tidas_volume * vol );



#endif
