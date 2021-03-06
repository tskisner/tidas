
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_test.hpp>

#include <chrono>


using namespace std;
using namespace tidas;


void tidas::test::volume_setup(volume & vol, size_t n_samp, size_t n_intr,
                               size_t n_block) {
    vol.root().clear();

    for (size_t b = 0; b < n_block; ++b) {
        ostringstream blkname;
        blkname << "block_" << b;

        block & blk = vol.root().block_add(blkname.str(), block());

        tidas::test::block_setup(blk, n_samp, n_intr);
    }

    return;
}

void tidas::test::volume_verify(volume & vol) {
    block & rt = vol.root();

    vector <string> names = rt.block_names();

    for (vector <string>::iterator it = names.begin(); it != names.end(); ++it) {
        block & blk = rt.block_get(*it);

        tidas::test::block_verify(blk);
    }

    return;
}

volumeTest::volumeTest() {}

void volumeTest::SetUp() {
    chunk = 100;
    n_samp = 10 + 2 * chunk;
    n_intr = 10;
    n_block = 3;

    // this will be used for a large data test
    n_big = 3000000;
}

TEST_F(volumeTest, MetaOps) {
    volume vol;
}


TEST_F(volumeTest, HDF5Backend) {
    // HDF5 backend

#ifdef HAVE_HDF5

    string dir = tidas::test::output_dir();
    fs_mkdir(dir.c_str());

    // We use the backend-specific options to override the default
    // chunk size.

    ostringstream chkstr;
    chkstr << chunk;
    map <string, string> hdf_extra;
    hdf_extra[key_hdf5_chunk] = chkstr.str();

    // test deep copy from mem

    string volpath = dir + "/test_volume.out";
    string volpathmem = dir + "/test_volume_dup_mem.out";

    fs_rm_r(volpath.c_str());
    fs_rm_r(volpathmem.c_str());

    {
        volume vol(volpath, backend_type::hdf5, compression_type::gzip, hdf_extra);

        // std::cerr << "vol created" << std::endl;
        tidas::test::volume_setup(vol, n_samp, n_intr, n_block);
        tidas::test::volume_verify(vol);

        vol.duplicate(volpathmem, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);

        // std::cerr << "vol duplicated" << std::endl;
    }

    {
        volume vol(volpathmem, access_mode::write);

        // std::cerr << "vol dup opened in write mode" << std::endl;
        tidas::test::volume_verify(vol);
    }

    // test deep copy from a read-write volume

    string volpathrw = dir + "/test_volume_dup_rw.out";

    fs_rm_r(volpathrw.c_str());

    {
        volume vol(volpathmem, access_mode::write);

        // std::cerr << "vol dup opened in write mode" << std::endl;
        vol.duplicate(volpathrw, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);

        // std::cerr << "vol duplicated to rw path" << std::endl;
    }

    {
        volume vol(volpathrw, access_mode::read);

        // std::cerr << "vol rw opened readonly" << std::endl;
        tidas::test::volume_verify(vol);
    }

    // test deep copy from a read-only volume

    string volpathro = dir + "/test_volume_dup_ro.out";

    fs_rm_r(volpathro.c_str());

    {
        volume vol(volpathmem, access_mode::read);
        vol.duplicate(volpathro, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);
    }

    {
        volume vol(volpathro, access_mode::read);
        tidas::test::volume_verify(vol);
    }

    // If the special environment variable is set, then run a "big"
    // test.

    bool do_big = false;
    char * envval = getenv("TIDAS_TEST_BIG");
    if (envval) {
        long tmp = atol(envval);
        if (tmp > 0) {
            do_big = true;
        }
    }

    if (do_big) {
        string volbig = dir + "/test_volume_big.out";

        auto start = chrono::steady_clock::now();

        volume vol(volbig, backend_type::hdf5, compression_type::none);

        auto stop = chrono::steady_clock::now();
        auto diff = stop - start;

        cout << "Creating large volume: " <<
            chrono::duration <double, milli> (diff).count() << " ms" << endl;

        start = chrono::steady_clock::now();
        tidas::test::volume_setup(vol, n_big, n_intr, n_block);
        stop = chrono::steady_clock::now();
        diff = stop - start;

        cout << "Writing large volume: " <<
            chrono::duration <double, milli> (diff).count() << " ms" << endl;

        start = chrono::steady_clock::now();
        tidas::test::volume_verify(vol);
        stop = chrono::steady_clock::now();
        diff = stop - start;

        cout << "Read and verify large volume: " << chrono::duration <double, milli> (
            diff).count() << " ms" << endl;
    }

    // EXPECT_TRUE(false);

#else // ifdef HAVE_HDF5

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif // ifdef HAVE_HDF5
}
