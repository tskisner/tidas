/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_VOLUME_H
#define TIDAS_VOLUME_H


typedef struct {
	char path[ TIDAS_PATH_LEN ];
	tidas_block * root;
	tidas_index * index;
} tidas_volume;


tidas_volume * tidas_volume_create ( char const * path );

tidas_volume * tidas_volume_open ( char const * path );

void tidas_volume_close ( tidas_volume * vol );



#endif
