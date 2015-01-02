/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



void block_setup ( block & blk, size_t n_samp, size_t n_intr ) {

	blk.clear();

	dict dt;
	dict_setup ( dt );

	intervals intr ( dt, n_intr );

	interval_list inv;
	intervals_setup ( inv );

	field_list flist;
	schema_setup ( flist );
	schema schm ( flist );

	group grp ( schm, dt, n_samp );

	group & grefa = blk.group_add ( "group_A", grp );
	group_setup ( grefa );

	group & grefb = blk.group_add ( "group_B", grp );
	group_setup ( grefb );

	intervals & irefa = blk.intervals_add ( "intr_A", intr );
	irefa.write_data ( inv );

	intervals & irefb = blk.intervals_add ( "intr_B", intr );
	irefb.write_data ( inv );

	return;
}


void block_verify ( block & blk ) {

	group grp = blk.group_get ( "group_A" );
	group_verify ( grp );

	grp = blk.group_get ( "group_B" );
	group_verify ( grp );

	interval_list inv;
	
	intervals intr = blk.intervals_get ( "intr_A" );
	intr.read_data ( inv );
	intervals_verify ( inv );

	intr = blk.intervals_get ( "intr_B" );
	intr.read_data ( inv );
	intervals_verify ( inv );

	vector < string > blks = blk.all_blocks();

	for ( size_t i = 0; i < blks.size(); ++i ) {
		block subblk = blk.block_get ( blks[i] );
		block_verify ( subblk );
	}

	return;
}


blockTest::blockTest () {

}


void blockTest::SetUp () {
	n_samp = 10;
	n_intr = 10;
}


TEST_F( blockTest, MetaOps ) {

	block blk;

}


TEST_F( blockTest, HDF5Backend ) {

	// HDF5 backend

#ifdef HAVE_HDF5

	backend_path loc;
	loc.type = backend_type::hdf5;
	loc.path = ".";
	loc.name = "test_block.out";
	loc.mode = access_mode::readwrite;

	block blk;
	blk.relocate ( loc );
	blk.flush();

	block_setup ( blk, n_samp, n_intr );

	block_verify ( blk );

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}