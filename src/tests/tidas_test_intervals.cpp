/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;


void intervals_setup ( interval_list & inv ) {

	inv.clear();

	size_t nint = 10;
	time_type gap = 1.0;
	time_type span = 123.4;
	index_type gap_samp = 5;
	index_type span_samp = 617;

	intrvl cur;

	for ( size_t i = 0; i < nint; ++i ) {
		cur.start = gap + (double)i * ( span + gap );
		cur.stop = (double)(i + 1) * ( span + gap );
		cur.first = gap_samp + i * ( span_samp + gap_samp );
		cur.last = (i + 1) * ( span_samp + gap_samp );
		inv.push_back ( cur );
	}

	return;
}


void intervals_verify ( interval_list const & inv ) {

	size_t nint = 10;
	time_type gap = 1.0;
	time_type span = 123.4;
	index_type gap_samp = 5;
	index_type span_samp = 617;

	intrvl cur;

	for ( size_t i = 0; i < nint; ++i ) {
		cur.start = gap + (double)i * ( span + gap );
		cur.stop = (double)(i + 1) * ( span + gap );
		cur.first = gap_samp + i * ( span_samp + gap_samp );
		cur.last = (i + 1) * ( span_samp + gap_samp );

		EXPECT_EQ( cur.start, inv[i].start );
		EXPECT_EQ( cur.stop, inv[i].stop );
		EXPECT_EQ( cur.first, inv[i].first );
		EXPECT_EQ( cur.last, inv[i].last );
	}

	return;
}


intervalsTest::intervalsTest () {
	intervals_setup ( intrvls );
}


TEST_F( intervalsTest, MetaOps ) {

	intervals_verify ( intrvls );

	intervals intr;

	backend_path loc = intr.location();
	EXPECT_EQ( backend_type::none, loc.type );
	EXPECT_EQ( "", loc.path );
	EXPECT_EQ( "", loc.name );
	EXPECT_EQ( "", loc.meta );
	EXPECT_EQ( access_mode::read, loc.mode );

	intervals dummy ( intr );

	backend_path checkloc = dummy.location();
	EXPECT_EQ( loc.type, checkloc.type );
	EXPECT_EQ( loc.path, checkloc.path );
	EXPECT_EQ( loc.name, checkloc.name );
	EXPECT_EQ( loc.mode, checkloc.mode );

	dict dt;

	dict_setup ( dt );

	intervals intr2 ( dt, intrvls.size() );

	checkloc = intr2.location();
	EXPECT_EQ( loc.type, checkloc.type );
	EXPECT_EQ( loc.path, checkloc.path );
	EXPECT_EQ( loc.name, checkloc.name );
	EXPECT_EQ( loc.mode, checkloc.mode );	

}


TEST_F( intervalsTest, HDF5Backend ) {

#ifdef HAVE_HDF5

	dict dt;

	dict_setup ( dt );

	intervals test ( dt, intrvls.size() );

	// copy to metadata to hdf5 location

	backend_path loc;
	loc.type = backend_type::hdf5;
	loc.mode = access_mode::write;
	loc.path = ".";
	loc.name = "test_intervals_data.hdf5.out";

	intervals test2 ( test, "", loc );
	test2.flush();

	test2.write_data ( intrvls );

	// copy 

	loc.name = "copy_" + loc.name;

	intervals test3 ( test2, "", loc );
	test3.flush();

	data_copy ( test2, test3 );

	// read and verify
	
	interval_list check;
	test3.read_data ( check );

	intervals_verify ( check );

#else

	cout << "  skipping (not compiled with HDF5 support)" << endl;

#endif

}


