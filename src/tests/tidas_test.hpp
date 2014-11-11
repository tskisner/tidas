/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_TEST_HPP
#define TIDAS_TEST_HPP

#include <tidas_internal.hpp>

#include <gtest/gtest.h>


class dictTest : public ::testing::Test {

	public :

		dictTest ();
		~dictTest () { }
		virtual void SetUp() { }
		virtual void TearDown() { }

		tidas::dict dct;

};


void dict_setup ( tidas::dict & dct );

void dict_verify ( tidas::dict const & dct );



#endif
