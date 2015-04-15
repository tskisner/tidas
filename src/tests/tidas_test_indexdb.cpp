/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>

#include <cstdlib>
#include <fstream>


using namespace std;
using namespace tidas;


#define NBLOCK 3
#define NGROUP 2
#define NINTERVAL 2
#define GSTART 12345.6
#define GSTOP 78901.2
#define GNSAMP 100


void indexdb_group_counts ( field_list const & fields, map < data_type, size_t > & counts ) {

	counts.clear();
	counts[ data_type::int8 ] = 0;
	counts[ data_type::uint8 ] = 0;
	counts[ data_type::int16 ] = 0;
	counts[ data_type::uint16 ] = 0;
	counts[ data_type::int32 ] = 0;
	counts[ data_type::uint32 ] = 0;
	counts[ data_type::int64 ] = 0;
	counts[ data_type::uint64 ] = 0;
	counts[ data_type::float32 ] = 0;
	counts[ data_type::float64 ] = 0;
	counts[ data_type::string ] = 0;

	// we always have a time field
	++counts[ data_type::float64 ];

	for ( auto fld : fields ) {
		if ( fld.name == group_time_field ) {
			// ignore time field, since we already counted it 
		} else {
			if ( fld.type == data_type::none ) {
				ostringstream o;
				o << "group schema field \"" << fld.name << "\" has type == none";
				TIDAS_THROW( o.str().c_str() );
			}
			++counts[ fld.type ];
		}
	}

	return;
}


void indexdb_setup ( tidas::indexdb * idx ) {

	backend_path loc;
	loc.path = "vol";
	loc.name = "root";

	idx->add_block ( loc );

	loc.path = "vol/root";

	for ( size_t b = 0; b < NBLOCK; ++b ) {
		ostringstream blkname;
		blkname << "blk" << b;

		loc.name = blkname.str();
		idx->add_block ( loc );

		for ( size_t g = 0; g < NGROUP; ++g ) {

			backend_path grouploc;
			grouploc.path = loc.path + path_sep + loc.name + path_sep + block_fs_group_dir;

			ostringstream grpname;
			grpname << "grp" << g;

			grouploc.name = grpname.str();

			field_list flist;
			schema_setup ( flist );

			dict dct;
			dict_setup ( dct );

			map < data_type, size_t > counts;
			indexdb_group_counts ( flist, counts );

			idx->add_group ( grouploc, GNSAMP, GSTART, GSTOP, counts );

			idx->add_schema ( grouploc, flist );

			idx->add_dict ( grouploc, dct.data(), dct.types() );

		}

		for ( size_t n = 0; n < NINTERVAL; ++n ) {

			interval_list inv;
			intervals_setup ( inv );

			backend_path intrloc;
			intrloc.path = loc.path + path_sep + loc.name + path_sep + block_fs_intervals_dir;

			ostringstream intrname;
			intrname << "intr" << n;

			intrloc.name = intrname.str();

			idx->add_intervals ( intrloc, inv.size() );

		}

	}

	return;
}



void indexdb_verify ( tidas::indexdb * idx ) {

	backend_path loc;
	loc.path = "vol";
	loc.name = "root";

	vector < string > root_child_blocks;
	vector < string > root_child_groups;
	vector < string > root_child_intervals;

	idx->query_block ( loc, root_child_blocks, root_child_groups, root_child_intervals );

	EXPECT_EQ( NBLOCK, root_child_blocks.size() );
	EXPECT_EQ( 0, root_child_groups.size() );
	EXPECT_EQ( 0, root_child_intervals.size() );

	loc.path = "vol/root";

	for ( size_t b = 0; b < NBLOCK; ++b ) {
		ostringstream blkname;
		blkname << "blk" << b;

		loc.name = blkname.str();

		vector < string > child_blocks;
		vector < string > child_groups;
		vector < string > child_intervals;

		idx->query_block ( loc, child_blocks, child_groups, child_intervals );

		EXPECT_EQ( 0, child_blocks.size() );
		EXPECT_EQ( NGROUP, child_groups.size() );
		EXPECT_EQ( NINTERVAL, child_intervals.size() );

		for ( size_t g = 0; g < NGROUP; ++g ) {

			field_list flist;
			schema_setup ( flist );

			dict dct;
			dict_setup ( dct );

			backend_path grouploc;
			grouploc.path = loc.path + path_sep + loc.name + path_sep + block_fs_group_dir;

			ostringstream grpname;
			grpname << "grp" << g;

			grouploc.name = grpname.str();

			map < data_type, size_t > counts;
			indexdb_group_counts ( flist, counts );

			index_type check_nsamp;
			time_type check_start;
			time_type check_stop;

			map < data_type, size_t > check_counts;

			idx->query_group ( grouploc, check_nsamp, check_start, check_stop, check_counts );

			EXPECT_EQ( GNSAMP, check_nsamp );
			EXPECT_EQ( GSTART, check_start );
			EXPECT_EQ( GSTOP, check_stop );

			EXPECT_EQ( counts.size(), check_counts.size() );

			for ( auto t : counts ) {
				EXPECT_EQ( t.second, check_counts[ t.first ] );
			}

			field_list check_fields;

			idx->query_schema ( grouploc, check_fields );
			schema_verify ( check_fields );

			map < string, string > ddata;
			map < string, data_type > dtypes;

			idx->query_dict ( grouploc, ddata, dtypes );

			EXPECT_EQ( dct.data().size(), ddata.size() );
			EXPECT_EQ( dct.types().size(), dtypes.size() );

			map < string, string > temp_data = dct.data();
			map < string, data_type > temp_types = dct.types();

			for ( auto d : ddata ) {
				EXPECT_EQ( temp_data[ d.first ], d.second );
				EXPECT_EQ( temp_types[ d.first ], dtypes[ d.first ] );
			}

		}

		for ( size_t n = 0; n < NINTERVAL; ++n ) {

			interval_list inv;
			intervals_setup ( inv );

			backend_path intrloc;
			intrloc.path = loc.path + path_sep + loc.name + path_sep + block_fs_intervals_dir;

			ostringstream intrname;
			intrname << "intr" << n;

			intrloc.name = intrname.str();

			size_t check_size;

			idx->query_intervals ( intrloc, check_size );

			EXPECT_EQ( inv.size(), check_size );

		}

	}

	return;
}



// recall EXPECT_EQ(expected, actual)


TEST( indexdbtest, pathsearch ) {

	// this is testing the general algorithm for seeking out 
	// direct path descendants from a std::map.

	string grdir = "_gr";
	string intdir = "_int";

	map < string, string > store;

	string root = "/root";

	store[ root ] = "";

	vector < string > blk;
	blk.push_back ( "a" );
	blk.push_back ( "b" );
	blk.push_back ( "c" );

	vector < string > grp;
	grp.push_back ( "1" );
	grp.push_back ( "2" );
	grp.push_back ( "3" );

	vector < string > intr;
	intr.push_back ( "x" );
	intr.push_back ( "y" );
	intr.push_back ( "z" );

	map < string, vector < string > > solution;

	for ( auto b : blk ) {
		string bpath = root + path_sep + b;
		store[ bpath ] = root;

		for ( auto g : grp ) {
			string gpath = bpath + path_sep + grdir + path_sep + g;
			store[ gpath ] = bpath;
			solution[ bpath ].push_back ( gpath );
		}

		for ( auto n : intr ) {
			string npath = bpath + path_sep + intdir + path_sep + n;
			store[ npath ] = bpath;
			solution[ bpath ].push_back ( npath );
		}

		for ( auto c : blk ) {
			string cpath = bpath + path_sep + c;
			store[ cpath ] = bpath;

			solution[ bpath ].push_back ( cpath );

			for ( auto g : grp ) {
				string gpath = cpath + path_sep + grdir + path_sep + g;
				store[ gpath ] = cpath;
			}

			for ( auto n : intr ) {
				string npath = cpath + path_sep + intdir + path_sep + n;
				store[ npath ] = cpath;
			}

		}

	}

	//cout << "---------- full -----------" << endl;
	//for ( auto it : store ) {
	//	cout << it.first << " : " << it.second << endl;
	//}

	// attempt to find descendant paths...

	//cout << "---------- descend -----------" << endl;

	for ( auto b : blk ) {
		string bpath = root + path_sep + b;

		vector < string > check;

		map < string, string > :: const_iterator it = store.lower_bound ( bpath );

		while ( ( it != store.end() ) && ( it->first.compare ( 0, bpath.size(), bpath ) == 0 ) ) {
			// we have some kind of descendent.  now check for direct descendants.

			//cout << "D1:  " << bpath << " --> " << it->first << endl;

			if ( it->first.size() > bpath.size() ) {
				//(we don't want the parent itself)

				size_t off = bpath.size() + 1;
				size_t pos = it->first.find ( path_sep, off );

				//cout << "D2:    check for " << path_sep << " in " << it->first.substr ( off ) << endl;

				if ( pos == string::npos ) {
					// direct descendant block
					//cout << "D2:     keeping " << it->first << endl;
					check.push_back( it->first );
				} else {
					// check for direct descendant group
					//cout << "D3:     check for " << grdir << " in " << it->first.substr ( off ) << endl;
					if ( it->first.compare ( off, grdir.size(), grdir ) == 0 ) {
						check.push_back( it->first );
					} else {
						// check for direct descendant intervals
						//cout << "D3:     check for " << intdir << " in " << it->first.substr ( off ) << endl;
						if ( it->first.compare ( off, intdir.size(), intdir ) == 0 ) {
							check.push_back( it->first );
						}
					}
				}

			}

			++it;
		}

		EXPECT_EQ( solution[bpath], check );

		//for ( auto c : solution[bpath] ) {
		//	cout << bpath << " : " << c << endl;
		//}

	}


}


TEST( indexdbtest, addquery_mem ) {

	indexdb_mem idx;

	indexdb_setup( &idx );

	indexdb_verify( &idx );

}


TEST( indexdbtest, addquery_sql ) {

	fs_rm ( "./indexdb_sql.out" );
	indexdb_sql idx( "./indexdb_sql.out", "", access_mode::readwrite );

	indexdb_setup( &idx );

	indexdb_verify( &idx );

}


TEST( indexdbtest, history ) {

	indexdb_mem idx;
	indexdb_mem check;

	indexdb_setup( &idx );

	check.replay ( idx.history() );

	indexdb_verify( &check );

}


TEST( indexdbtest, commit ) {

	indexdb_mem idx;

	indexdb_setup( &idx );

	fs_rm ( "./indexdb_sql_commit.out" );
	indexdb_sql check( "./indexdb_sql_commit.out", "", access_mode::readwrite );

	check.commit ( idx.history() );

	indexdb_verify( &check );

}


TEST( indexdbtest, treecommit ) {

	fs_rm ( "./indexdb_sql_tree.out" );
	fs_rm ( "./indexdb_sql_tree_check.out" );
	indexdb_sql idx( "./indexdb_sql_tree.out", "", access_mode::readwrite );
	indexdb_sql check( "./indexdb_sql_tree_check.out", "", access_mode::readwrite );

	indexdb_setup( &idx );

	std::deque < indexdb_transaction > data;

	backend_path root;
	root.path = "vol";
	root.name = "root";
	idx.tree ( root, "", data );

	check.commit ( data );

	indexdb_verify( &check );

}


TEST( indexdbtest, serialize ) {

	// test serialization of a std::map

	map < data_type, size_t > inmap;
	inmap[ data_type::float64 ] = 123;
	inmap[ data_type::uint16 ] = 456;
	inmap[ data_type::int64 ] = 789;

	map < data_type, size_t > outmap;

	{
		ostringstream str;
		cereal::PortableBinaryOutputArchive outarch ( str );
		outarch ( inmap );

		istringstream istr( str.str() );
		cereal::PortableBinaryInputArchive inarch ( istr );
		inarch ( outmap );
	}

	EXPECT_EQ( inmap, outmap );

	indexdb_mem idx;

	indexdb_setup( &idx );

	// serialize

	string outfile = "./indexdb_mem_serialized.out";

	{
		ofstream os( outfile, ios::binary );
  		cereal::PortableBinaryOutputArchive outarch ( os );
  		outarch ( idx );
  		os.close();

		ifstream is( outfile, ios::binary );
  		cereal::PortableBinaryInputArchive inarch ( is );
  		inarch ( idx );
  		is.close();
	}

	indexdb_mem check;

	check.replay ( idx.history() );

	indexdb_verify( &check );

}


TEST( indexdbtest, sqlite ) {

	string dbfile = "./indexdb_sql.out";
	fs_rm ( dbfile.c_str() );

	indexdb_sql idx ( dbfile, "", access_mode::readwrite );

	indexdb_setup( &idx );

	// copy constructor

	indexdb_sql check ( idx );
	indexdb_verify ( &check );

	// serialize to file

	string outfile = "./indexdb_sql_serialized.out";

	indexdb_sql check2;

	{
		ofstream os( outfile, ios::binary );
  		cereal::PortableBinaryOutputArchive outarch ( os );
  		outarch ( idx );
  		os.close();

		ifstream is( outfile, ios::binary );
  		cereal::PortableBinaryInputArchive inarch ( is );
  		inarch ( check2 );
  		is.close();
	}

	indexdb_verify ( &check2 );

	// serialize to string

	indexdb_sql check3;

	{
  		stringstream istr;
		cereal::PortableBinaryOutputArchive outarch ( istr );
		outarch ( idx );

		stringstream ostr;
		ostr << istr.str();
		cereal::PortableBinaryInputArchive inarch ( ostr );
		inarch ( check3 );
	}

	indexdb_verify ( &check3 );

	// duplicate
	/*
	string dupfile = "./indexdb_sql_dup.out";
	fs_rm ( dupfile.c_str() );

	check3.duplicate ( dupfile );

	indexdb check4 ( dupfile, access_mode::read );

	indexdb_verify ( check4 );
	*/
}


