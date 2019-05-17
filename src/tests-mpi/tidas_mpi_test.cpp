/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#include <tidas_mpi_internal.hpp>

#include <tidas_mpi_test.hpp>

#include <chrono>

#include <fstream>

extern "C" {
    #include <dirent.h>
}

using namespace std;
using namespace tidas;


int tidas::test::mpi_runner(int argc, char * argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "MPI*";
    return RUN_ALL_TESTS();
}

void tidas::test::mpi_volume_setup(mpi_volume & vol, size_t n_samp, size_t n_intr,
                                   size_t n_block) {
    vol.root().clear();

    MPI_Comm comm = vol.comm();
    int rank = vol.comm_rank();
    int nproc = vol.comm_size();

    size_t offset;
    size_t nlocal;

    mpi_dist_uniform(comm, n_block, &offset, &nlocal);

    // std::cout << "Process " << rank << " assigned blocks " << offset << " - " <<
    // offset+nlocal-1 << std::endl;

    for (size_t b = offset; b < offset + nlocal; ++b) {
        // std::cout << "Process " << rank << " setting up block " << b << std::endl;

        ostringstream blkname;
        blkname << "block_" << b;

        block & blk = vol.root().block_add(blkname.str(), block());

        tidas::test::block_setup(blk, n_samp, n_intr);
    }

    vol.meta_sync();

    return;
}

void tidas::test::mpi_volume_root_setup(mpi_volume & vol, size_t n_samp, size_t n_intr,
                                        size_t n_block) {
    vol.root().clear();

    MPI_Comm comm = vol.comm();
    int rank = vol.comm_rank();
    int nproc = vol.comm_size();

    size_t offset;
    size_t nlocal;

    // Test case where a single process adds all blocks and then sync
    // metadata with some processes having no transactions.

    if (rank == 0) {
        for (size_t b = offset; b < offset + nlocal; ++b) {
            // std::cout << "Process " << rank << " setting up block " << b <<
            // std::endl;

            ostringstream blkname;
            blkname << "block_" << b;

            block & blk = vol.root().block_add(blkname.str(), block());

            tidas::test::block_setup(blk, n_samp, n_intr);
        }
    }

    vol.meta_sync();

    return;
}

void tidas::test::mpi_volume_verify(mpi_volume & vol) {
    block & rt = vol.root();

    vector <string> names = rt.block_names();

    for (vector <string>::iterator it = names.begin(); it != names.end(); ++it) {
        block & blk = rt.block_get(*it);

        tidas::test::block_verify(blk);
    }

    return;
}

MPIvolumeTest::MPIvolumeTest() {}

void MPIvolumeTest::SetUp() {
    chunk = 100;
    n_samp = 10 + 2 * chunk;
    n_intr = 10;
    n_block = 10;

    // this will be used for a large data test
    n_big = 3000000;
}

TEST_F(MPIvolumeTest, MetaOps) {
    mpi_volume vol;
}


TEST_F(MPIvolumeTest, HDF5Backend) {
    // HDF5 backend

#ifdef HAVE_HDF5

    int ret;
    MPI_Comm comm = MPI_COMM_WORLD;
    int rank;
    int nproc;
    ret = MPI_Comm_rank(comm, &rank);
    ret = MPI_Comm_size(comm, &nproc);

    string dir = tidas::test::output_dir();
    if (rank == 0) {
        fs_mkdir(dir.c_str());
    }

    // We use the backend-specific options to override the default
    // chunk size.

    ostringstream chkstr;
    chkstr << chunk;
    map <string, string> hdf_extra;
    hdf_extra[key_hdf5_chunk] = chkstr.str();

    // test deep copy from mem

    string volpath = dir + "/test_mpi_volume.out";
    string volpathmem = dir + "/test_mpi_volume_dup_mem.out";

    if (rank == 0) {
        fs_rm_r(volpath.c_str());
        fs_rm_r(volpathmem.c_str());
    }
    ret = MPI_Barrier(comm);

    {
        mpi_volume vol(comm, volpath, backend_type::hdf5, compression_type::gzip,
                       hdf_extra);

        tidas::test::mpi_volume_setup(vol, n_samp, n_intr, n_block);

        tidas::test::mpi_volume_verify(vol);

        vol.duplicate(volpathmem, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);
    }


    {
        mpi_volume vol(comm, volpathmem, access_mode::write);
        tidas::test::mpi_volume_verify(vol);
    }

    // test deep copy from a read-write volume

    string volpathrw = dir + "/test_mpi_volume_dup_rw.out";

    if (rank == 0) {
        fs_rm_r(volpathrw.c_str());
    }
    ret = MPI_Barrier(comm);

    {
        mpi_volume vol(comm, volpathmem, access_mode::write);
        vol.duplicate(volpathrw, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);
    }

    {
        mpi_volume vol(comm, volpathrw, access_mode::read);
        tidas::test::mpi_volume_verify(vol);
    }

    // test deep copy from a read-only volume

    string volpathro = dir + "/test_mpi_volume_dup_ro.out";

    if (rank == 0) {
        fs_rm_r(volpathro.c_str());
    }
    ret = MPI_Barrier(comm);

    {
        mpi_volume vol(comm, volpathmem, access_mode::read);
        vol.duplicate(volpathro, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);
    }

    ret = MPI_Barrier(comm);

    {
        mpi_volume vol(comm, volpathro, access_mode::read);
        tidas::test::mpi_volume_verify(vol);
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
        string volbig = dir + "/test_mpi_volume_big.out";

        if (rank == 0) {
            fs_rm_r(volbig.c_str());
        }
        ret = MPI_Barrier(comm);

        auto start = chrono::steady_clock::now();

        mpi_volume vol(comm, volbig, backend_type::hdf5, compression_type::none);

        ret = MPI_Barrier(comm);
        auto stop = chrono::steady_clock::now();
        auto diff = stop - start;

        cout << "Creating large volume: " <<
            chrono::duration <double, milli> (diff).count() << " ms" << endl;

        start = chrono::steady_clock::now();

        tidas::test::mpi_volume_setup(vol, n_big, n_intr, n_block);

        ret = MPI_Barrier(comm);
        stop = chrono::steady_clock::now();
        diff = stop - start;

        cout << "Writing large volume: " <<
            chrono::duration <double, milli> (diff).count() << " ms" << endl;

        start = chrono::steady_clock::now();

        tidas::test::mpi_volume_verify(vol);

        ret = MPI_Barrier(comm);
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


TEST_F(MPIvolumeTest, HDF5BackendRoot) {
    // HDF5 backend with root process initialization

#ifdef HAVE_HDF5

    int ret;
    MPI_Comm comm = MPI_COMM_WORLD;
    int rank;
    int nproc;
    ret = MPI_Comm_rank(comm, &rank);
    ret = MPI_Comm_size(comm, &nproc);

    string dir = tidas::test::output_dir();
    if (rank == 0) {
        fs_mkdir(dir.c_str());
    }

    // We use the backend-specific options to override the default
    // chunk size.

    ostringstream chkstr;
    chkstr << chunk;
    map <string, string> hdf_extra;
    hdf_extra[key_hdf5_chunk] = chkstr.str();

    // test deep copy from mem

    string volpath = dir + "/test_mpi_volume_root.out";
    string volpathmem = dir + "/test_mpi_volume_root_dup_mem.out";

    if (rank == 0) {
        fs_rm_r(volpath.c_str());
        fs_rm_r(volpathmem.c_str());
    }
    ret = MPI_Barrier(comm);

    {
        mpi_volume vol(comm, volpath, backend_type::hdf5, compression_type::gzip,
                       hdf_extra);

        tidas::test::mpi_volume_root_setup(vol, n_samp, n_intr, n_block);

        tidas::test::mpi_volume_verify(vol);

        vol.duplicate(volpathmem, backend_type::hdf5, compression_type::gzip, "",
                      hdf_extra);
    }

    {
        mpi_volume vol(comm, volpathmem, access_mode::write);
        tidas::test::mpi_volume_verify(vol);
    }

#else // ifdef HAVE_HDF5

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif // ifdef HAVE_HDF5
}
