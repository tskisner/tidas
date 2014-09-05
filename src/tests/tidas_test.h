/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_TEST_H
#define TIDAS_TEST_H

#include <tidas_internal.h>

#include <float.h>
#include <math.h>

#include <check.h>


int check_dbl_eq ( double d1, double d2 );


Suite * make_suite_utils();

Suite * make_suite_intervals();


#endif
