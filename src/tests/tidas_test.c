/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.h>


int main ( int argc, char *argv[] ) {

	fprintf ( stderr, "\n" );

	tidas_test_utils();

	tidas_test_intervals();

	fprintf ( stderr, "\n" );

	return 0;
}

