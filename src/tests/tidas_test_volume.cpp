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

		block_setup ( blk, n_samp, n_intr );

	}

	return;
}


void volume_verify ( volume & vol ) {

	block & rt = vol.root();

	vector < string > names = rt.all_blocks();

	for ( vector < string > :: iterator it = names.begin(); it != names.end(); ++it ) {

		block & blk = rt.block_get ( *it );

		block_verify ( blk );

	}

	return;
}


volumeTest::volumeTest () {

}


void volumeTest::SetUp () {
	n_samp = 10 + env_hdf5_chunk_default;
	n_intr = 10;
	n_block = 3;
}


TEST_F( volumeTest, MetaOps ) {

	volume vol;

}


TEST_F( volumeTest, HDF5Backend ) {

	// HDF5 backend

#ifdef HAVE_HDF5

	// test deep copy

	fs_rm_r ( "test_volume.out" );
	fs_rm_r ( "test_volume_dup.out" );

	{
		volume vol ( "test_volume.out", backend_type::hdf5, compression_type::gzip );

		volume_setup ( vol, n_samp, n_intr, n_block );
		volume_verify ( vol );

		volume vol2 ( "test_volume_dup.out", backend_type::hdf5, compression_type::gzip );

		data_copy ( vol, vol2 );

		volume_verify ( vol2 );
	}

	// test deep copy from a read-only volume

	fs_rm_r ( "test_volume_dup_dup.out" );

	{
		volume vol3 ( "test_volume_dup.out", access_mode::read );

		volume vol4 ( "test_volume_dup_dup.out", backend_type::hdf5, compression_type::gzip );

		data_copy ( vol3, vol4 );

		volume_verify ( vol4 );

	}

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}
