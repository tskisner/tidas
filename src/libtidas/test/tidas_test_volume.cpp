/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
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

    string dir = tidas::test::output_dir();

    // test deep copy from mem

    string volpath = dir + "/test_volume.out";
    string volpathmem = dir + "/test_volume_dup_mem.out";

    fs_rm_r ( volpath.c_str() );
    fs_rm_r ( volpathmem.c_str() );

    {
        volume vol ( volpath, backend_type::hdf5, compression_type::gzip );
        volume_setup ( vol, n_samp, n_intr, n_block );
        volume_verify ( vol );

        vol.duplicate ( volpathmem, backend_type::hdf5, compression_type::gzip );
    }

    {
        volume vol ( volpathmem, access_mode::write );
        volume_verify ( vol );
    }

    // test deep copy from a read-write volume

    string volpathrw = dir + "/test_volume_dup_rw.out";

    fs_rm_r ( volpathrw.c_str() );

    {
        volume vol ( volpathmem, access_mode::write );
        vol.duplicate ( volpathrw, backend_type::hdf5, compression_type::gzip );
    }

    {
        volume vol ( volpathrw, access_mode::read );
        volume_verify ( vol );
    }

    // test deep copy from a read-only volume

    string volpathro = dir + "/test_volume_dup_ro.out";

    fs_rm_r ( volpathro.c_str() );

    {
        volume vol ( volpathmem, access_mode::read );
        vol.duplicate ( volpathro, backend_type::hdf5, compression_type::gzip );
    }

    {
        volume vol ( volpathro, access_mode::read );
        volume_verify ( vol );
    }

    

#else

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}
