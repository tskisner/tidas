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

	// test deep copy from mem

	fs_rm_r ( "test_volume.out" );
	fs_rm_r ( "test_volume_dup_mem.out" );

	{
		volume vol ( "test_volume.out", backend_type::hdf5, compression_type::gzip );
		volume_setup ( vol, n_samp, n_intr, n_block );
		volume_verify ( vol );

		vol.duplicate ( "test_volume_dup_mem.out", backend_type::hdf5, compression_type::gzip );
	}

	{
		volume vol ( "test_volume_dup_mem.out", access_mode::write );
		volume_verify ( vol );
	}

	// test deep copy from a read-write volume

	fs_rm_r ( "test_volume_dup_rw.out" );

	{
		volume vol ( "test_volume_dup_mem.out", access_mode::write );
		vol.duplicate ( "test_volume_dup_rw.out", backend_type::hdf5, compression_type::gzip );
	}

	{
		volume vol ( "test_volume_dup_rw.out", access_mode::read );
		volume_verify ( vol );
	}

	// test deep copy from a read-only volume

	fs_rm_r ( "test_volume_dup_ro.out" );

	{
		volume vol ( "test_volume_dup_mem.out", access_mode::read );
		vol.duplicate ( "test_volume_dup_ro.out", backend_type::hdf5, compression_type::gzip );
	}

	{
		volume vol ( "test_volume_dup_ro.out", access_mode::read );
		volume_verify ( vol );
	}

	

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}