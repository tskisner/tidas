/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_mpi_internal.hpp>

#include <fstream>

extern "C" {
    #include <dirent.h>
}

using namespace std;
using namespace tidas;


void mpi_volume_setup ( mpi_volume & vol, size_t n_samp, size_t n_intr, size_t n_block ) {

    vol.root().clear();

    MPI_Comm comm = vol.comm();
    int rank = vol.comm_rank();
    int nproc = vol.comm_size();

    size_t offset;
    size_t nlocal;

    mpi_dist_uniform ( comm, n_block, &offset, &nlocal );

    for ( size_t b = offset; b < nlocal; ++b ) {

        ostringstream blkname;
        blkname << "block_" << b;

        block & blk = vol.root().block_add ( blkname.str(), block() );

        block_setup ( blk, n_samp, n_intr );

    }

    vol.meta_sync();

    return;
}


void mpi_volume_verify ( mpi_volume & vol ) {

    block & rt = vol.root();

    vector < string > names = rt.all_blocks();

    for ( vector < string > :: iterator it = names.begin(); it != names.end(); ++it ) {

        block & blk = rt.block_get ( *it );

        block_verify ( blk );

    }

    return;
}


mpivolumeTest::mpivolumeTest () {

}


void mpivolumeTest::SetUp () {
    chunk = 100;
    n_samp = 10 + 2 * chunk;
    n_intr = 10;
    n_block = 3;

    // this will be used for a large data test
    n_big = 3000000;
}


TEST_F( mpivolumeTest, MetaOps ) {

    volume vol;

}


TEST_F( mpivolumeTest, HDF5Backend ) {

    // HDF5 backend

#ifdef HAVE_HDF5

    string dir = tidas::test::output_dir();

    // We use the backend-specific options to override the default
    // chunk size.

    ostringstream chkstr;
    chkstr << chunk;
    map < string, string > hdf_extra;
    hdf_extra[key_hdf5_chunk] = chkstr.str();

    // test deep copy from mem

    string volpath = dir + "/test_volume.out";
    string volpathmem = dir + "/test_volume_dup_mem.out";

    fs_rm_r ( volpath.c_str() );
    fs_rm_r ( volpathmem.c_str() );

    {
        volume vol ( volpath, backend_type::hdf5, compression_type::gzip, hdf_extra );
        //std::cerr << "vol created" << std::endl;
        volume_setup ( vol, n_samp, n_intr, n_block );
        volume_verify ( vol );

        vol.duplicate ( volpathmem, backend_type::hdf5, compression_type::gzip, "", hdf_extra );
        //std::cerr << "vol duplicated" << std::endl;
    }

    {
        volume vol ( volpathmem, access_mode::write );
        //std::cerr << "vol dup opened in write mode" << std::endl;
        volume_verify ( vol );
    }

    // test deep copy from a read-write volume

    string volpathrw = dir + "/test_volume_dup_rw.out";

    fs_rm_r ( volpathrw.c_str() );

    {
        volume vol ( volpathmem, access_mode::write );
        //std::cerr << "vol dup opened in write mode" << std::endl;
        vol.duplicate ( volpathrw, backend_type::hdf5, compression_type::gzip, "", hdf_extra );
        //std::cerr << "vol duplicated to rw path" << std::endl;
    }

    {
        volume vol ( volpathrw, access_mode::read );
        //std::cerr << "vol rw opened readonly" << std::endl;
        volume_verify ( vol );
    }

    // test deep copy from a read-only volume

    string volpathro = dir + "/test_volume_dup_ro.out";

    fs_rm_r ( volpathro.c_str() );

    {
        volume vol ( volpathmem, access_mode::read );
        vol.duplicate ( volpathro, backend_type::hdf5, compression_type::gzip, "", hdf_extra );
    }

    {
        volume vol ( volpathro, access_mode::read );
        volume_verify ( vol );
    }

    // If the special environment variable is set, then run a "big"
    // test.

    bool do_big = false;
    char * envval = getenv ( "TIDAS_TEST_BIG" );
    if ( envval ) {
        long tmp = atol ( envval );
        if ( tmp > 0 ) {
            do_big = true;
        }
    }

    if ( do_big ) {
        string volbig = dir + "/test_volume_big.out";

        auto start = chrono::steady_clock::now();
        
        volume vol ( volbig, backend_type::hdf5, compression_type::none );
        
        auto stop = chrono::steady_clock::now();
        auto diff = stop - start;

        cout << "Creating large volume: " << chrono::duration <double, milli> (diff).count() << " ms" << endl;

        start = chrono::steady_clock::now();
        volume_setup ( vol, n_big, n_intr, n_block );
        stop = chrono::steady_clock::now();
        diff = stop - start;

        cout << "Writing large volume: " << chrono::duration <double, milli> (diff).count() << " ms" << endl;
        
        start = chrono::steady_clock::now();
        volume_verify ( vol );
        stop = chrono::steady_clock::now();
        diff = stop - start;

        cout << "Read and verify large volume: " << chrono::duration <double, milli> (diff).count() << " ms" << endl;
    }
    

#else

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}


