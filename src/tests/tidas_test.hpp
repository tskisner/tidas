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

#include <ctidas.h>


class dictTest : public ::testing::Test {

	public :

		dictTest () {}
		~dictTest () {}
		virtual void SetUp();
		virtual void TearDown();

		tidas::dict dct;
		ctidas_dict * cdct;

};

void dict_setup ( tidas::dict & dct, ctidas_dict * cdct );

void dict_verify ( tidas::dict const & dct, ctidas_dict const * cdct );


class intervalsTest : public ::testing::Test {

	public :

		intervalsTest () { }
		~intervalsTest () { }
		virtual void SetUp();
		virtual void TearDown();

		tidas::interval_list intrvls;
		ctidas_intrvl ** cintrvls;

};

void intervals_setup ( tidas::interval_list & inv, ctidas_intrvl ** cinv );

void intervals_verify ( tidas::interval_list const & inv, ctidas_intrvl * const * cinv );


class schemaTest : public ::testing::Test {

	public :

		schemaTest ();
		~schemaTest () { }
		virtual void SetUp() { }
		virtual void TearDown() { }


};

void schema_setup ( tidas::field_list & flist );

void schema_verify ( tidas::field_list const & flist );


class groupTest : public ::testing::Test {

	public :

		groupTest ();
		~groupTest () { }
		virtual void SetUp();
		virtual void TearDown() { }

		size_t gnsamp;
};

void group_setup ( tidas::group & grp, size_t offset, size_t full_nsamp );

void group_verify ( tidas::group & grp, size_t offset, size_t full_nsamp );

void group_verify_int ( tidas::group & grp, size_t offset, size_t full_nsamp );


class blockTest : public ::testing::Test {

	public :

		blockTest ();
		~blockTest () { }
		virtual void SetUp();
		virtual void TearDown() { }

		size_t n_samp;
		size_t n_intr;

};

void block_setup ( tidas::block & blk, size_t n_samp, size_t n_intr );

void block_verify ( tidas::block & blk );



void indexdb_setup ( tidas::indexdb & idx );

void indexdb_verify ( tidas::indexdb & idx );


class volumeTest : public ::testing::Test {

	public :

		volumeTest ();
		~volumeTest () { }
		virtual void SetUp();
		virtual void TearDown() { }

		size_t n_samp;
		size_t n_intr;
		size_t n_block;

};

void volume_setup ( tidas::volume & vol, size_t n_samp, size_t n_intr, size_t n_block );

void volume_verify ( tidas::volume & vol );


#endif
