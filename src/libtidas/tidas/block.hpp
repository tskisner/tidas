/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_BLOCK_H
#define TIDAS_BLOCK_H


typedef struct {
	char name[ TIDAS_NAME_LEN ];
	tidas_vector * blocks;
	tidas_vector * groups;
	tidas_vector * intervals;
} tidas_block;


tidas_block * tidas_block_create ( char const * path );

tidas_block * tidas_block_open ( char const * path );

void tidas_block_close ( tidas_block * vol );



#endif
