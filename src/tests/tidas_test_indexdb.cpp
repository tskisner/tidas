/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>

#include <cstdlib>


using namespace std;
using namespace tidas;



// recall EXPECT_EQ(expected, actual)


TEST( indexdbtest, pathsearch ) {

	// this is testing the general algorithm for seeking out direct path descendants
	// from a std::map.

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


TEST( indexdbtest, history ) {


}




