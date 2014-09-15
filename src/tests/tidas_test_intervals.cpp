/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;



TEST( intervalstest, metadata ) {

	intervals dummy1;

	backend_path loc = dummy1.location();
	EXPECT_EQ( loc.type, BACKEND_MEM );
	EXPECT_EQ( loc.path, "" );
	EXPECT_EQ( loc.name, "" );

	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_intervals_meta.hdf5.out";

	dummy1.relocate ( loc );

	backend_path checkloc = dummy1.location();
	EXPECT_EQ( loc.type, checkloc.type );
	EXPECT_EQ( loc.path, checkloc.path );
	EXPECT_EQ( loc.name, checkloc.name );

	intervals dummy2 ( loc );
	dummy2.write_meta();
	dummy2.read_meta();

	checkloc = dummy2.location();
	EXPECT_EQ( loc.type, checkloc.type );
	EXPECT_EQ( loc.path, checkloc.path );
	EXPECT_EQ( loc.name, checkloc.name );

}


TEST( intervalstest, data ) {

	interval_list intr;
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
		intr.push_back ( cur );
	}

	backend_path loc;
	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_intervals_data.hdf5.out";

	intervals dummy1;
	dummy1.relocate ( loc );

	dummy1.write_meta();
	dummy1.write_data ( intr );

	intervals dummy2 ( loc );

	interval_list check;

	dummy2.read_data ( check );

	for ( size_t i = 0; i < nint; ++i ) {
		cur.start = gap + (double)i * ( span + gap );
		cur.stop = (double)(i + 1) * ( span + gap );
		cur.first = gap_samp + i * ( span_samp + gap_samp );
		cur.last = (i + 1) * ( span_samp + gap_samp );
		EXPECT_EQ( cur.start, check[i].start );
		EXPECT_EQ( cur.stop, check[i].stop );
		EXPECT_EQ( cur.first, check[i].first );
		EXPECT_EQ( cur.last, check[i].last );
	}

	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_intervals_data_dup.hdf5.out";

	dummy2.duplicate ( loc );

	intervals dummy3 ( loc );

	dummy3.read_data ( check );

	for ( size_t i = 0; i < nint; ++i ) {
		cur.start = gap + (double)i * ( span + gap );
		cur.stop = (double)(i + 1) * ( span + gap );
		cur.first = gap_samp + i * ( span_samp + gap_samp );
		cur.last = (i + 1) * ( span_samp + gap_samp );
		EXPECT_EQ( cur.start, check[i].start );
		EXPECT_EQ( cur.stop, check[i].stop );
		EXPECT_EQ( cur.first, check[i].first );
		EXPECT_EQ( cur.last, check[i].last );
	}

}





