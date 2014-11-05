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
	EXPECT_EQ( loc.meta, "" );
	EXPECT_EQ( loc.mode, MODE_RW );

	intervals dummy2 ( dummy1 );

	backend_path checkloc = dummy2.location();
	EXPECT_EQ( loc.type, checkloc.type );
	EXPECT_EQ( loc.path, checkloc.path );
	EXPECT_EQ( loc.name, checkloc.name );
	EXPECT_EQ( loc.mode, checkloc.mode );

}


TEST( intervalstest, data ) {

	dict dt;

	string sval = "blahblahblah";
	double dval = 12345.6;
	int ival = 12345;
	long long lval = 1234567890123456L;

	dt.put ( "string", sval );
	dt.put ( "double", dval );
	dt.put ( "int", ival );
	dt.put ( "longlong", lval );

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
	loc.mode = MODE_RW;
	loc.path = ".";
	loc.name = "test_intervals_data.hdf5.out";

	intervals dummy1 ( dt, nint );
	dummy1.write ( loc );
	dummy1.read ( loc );

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

	intervals dummy3 ( dummy2, ".*", loc );

	data_copy ( dummy2, dummy3 );

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


