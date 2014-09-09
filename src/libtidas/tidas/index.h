/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INDEX_H
#define TIDAS_INDEX_H


/* selection */

typedef struct {
	tidas_vector * blocks;
} tidas_selection;


/* metadata index */

typedef struct {
	sqlite3 * db;
} tidas_index;




#endif
