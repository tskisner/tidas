/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



void tidas::test::block_setup ( block & blk, size_t n_samp, size_t n_intr ) {

    blk.clear();

    dict dt;
    tidas::test::dict_setup ( dt, NULL );

    intervals intr ( dt, n_intr );

    interval_list inv;
    tidas::test::intervals_setup ( inv, NULL );

    field_list flist;
    tidas::test::schema_setup ( flist );
    schema schm ( flist );

    group grp ( schm, dt, n_samp );

    group & grefa = blk.group_add ( "group_A", grp );
    tidas::test::group_setup ( grefa, 0, grefa.size() );

    group & grefb = blk.group_add ( "group_B", grp );
    tidas::test::group_setup ( grefb, 0, grefb.size() );

    intervals & irefa = blk.intervals_add ( "intr_A", intr );
    irefa.write_data ( inv );

    intervals & irefb = blk.intervals_add ( "intr_B", intr );
    irefb.write_data ( inv );

    return;
}


void tidas::test::block_verify ( block & blk ) {

    group grp = blk.group_get ( "group_A" );
    tidas::test::group_verify ( grp, 0, grp.size() );

    grp = blk.group_get ( "group_B" );
    tidas::test::group_verify ( grp, 0, grp.size() );

    interval_list inv;
    
    intervals intr = blk.intervals_get ( "intr_A" );
    intr.read_data ( inv );
    tidas::test::intervals_verify ( inv, NULL );

    intr = blk.intervals_get ( "intr_B" );
    intr.read_data ( inv );
    tidas::test::intervals_verify ( inv, NULL );

    vector < string > blks = blk.all_blocks();

    for ( size_t i = 0; i < blks.size(); ++i ) {
        block subblk = blk.block_get ( blks[i] );
        tidas::test::block_verify ( subblk );
    }

    return;
}


blockTest::blockTest () {

}


void blockTest::SetUp () {
    n_samp = 10 + hdf5_chunk_default;
    n_intr = 10;
}


TEST_F( blockTest, MetaOps ) {

    block blk;

}


TEST_F( blockTest, Select ) {

    block top;

    backend_path loc;
    loc.type = backend_type::none;
    loc.path = "";
    loc.name = "top";
    loc.mode = access_mode::write;
    loc.comp = compression_type::none;

    top.relocate ( loc );

    for ( size_t i = 0; i < 5; ++i ) {
        ostringstream name;
        name << "block_" << i;
        block & blk = top.block_add ( name.str(), tidas::block() );
        for ( size_t j = 0; j < 5; ++j ) {
            ostringstream subname;
            subname << "sub_" << j;
            block & sub = blk.block_add ( subname.str(), tidas::block() );
        }
    }

    for ( size_t i = 0; i < 5; ++i ) {
        ostringstream name;
        name << "block_" << i;
        for ( size_t j = 0; j < 5; ++j ) {
            ostringstream subname;
            subname << "sub_" << j;
            // string filter = path_sep + name.str() + path_sep + subname.str() + path_sep;
            string filter = path_sep + name.str() + path_sep + subname.str();
            block blk = top.select ( filter );

            vector < string > children = blk.all_blocks();
            EXPECT_EQ ( children.size(), 1 );
            EXPECT_EQ ( children[0], name.str() );

            block sub = blk.block_get ( children[0] );
            children = sub.all_blocks();
            EXPECT_EQ ( children.size(), 1 );
            EXPECT_EQ ( children[0], subname.str() );
        }
    }

}


TEST_F( blockTest, HDF5Backend ) {

    // HDF5 backend

#ifdef HAVE_HDF5

    backend_path loc;
    loc.type = backend_type::hdf5;
    loc.path = tidas::test::output_dir();
    loc.name = "test_block.out";
    loc.mode = access_mode::write;
    loc.comp = compression_type::gzip;

    block blk;
    blk.relocate ( loc );
    blk.flush();

    tidas::test::block_setup ( blk, n_samp, n_intr );

    tidas::test::block_verify ( blk );

#else

    cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}
