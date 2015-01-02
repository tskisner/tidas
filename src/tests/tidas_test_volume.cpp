/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



void volume_setup ( volume & vol, size_t n_samp, size_t n_intr, size_t n_block ) {

	vol.root().clear();

	for ( size_t b = 0; b < n_block; ++b ) {

		ostringstream blkname;
		blkname << "block_" << b;

		block & blk = vol.root().block_add ( blkname.str(), block() );

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

	}

	return;
}


void volume_verify ( volume & vol ) {

	block const & rt = vol.root();

	vector < string > names = rt.all_blocks();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {

		block const & blk = rt.block_get ( *it );

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

	}

	return;
}


volumeTest::volumeTest () {

}


void volumeTest::SetUp () {
	n_samp = 10;
	n_intr = 10;
	n_block = 3;
}


TEST_F( volumeTest, MetaOps ) {

	volume vol;

}


TEST_F( volumeTest, HDF5Backend ) {

	// HDF5 backend

#ifdef HAVE_HDF5

	volume vol ( "test_volume.out", backend_type::hdf5, compression_type::none );

	volume_setup ( vol, n_samp, n_intr, n_block );

	volume_verify ( vol );

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}
