/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#include <tidas_internal.hpp>

#include <regex>


extern "C" {
	#include <dirent.h>
}


using namespace std;
using namespace tidas;



void tidas::block_link_operator::operator() ( block const & blk ) {

	// find the location of this block relative to the start

	backend_path loc = blk.location();

	string blkpath = loc.path + path_sep + loc.name;

	if ( start == "" ) {
		start = blkpath;
	}

	if ( blkpath.compare ( 0, start.size(), start ) != 0 ) {
		TIDAS_THROW( "link operator path error" );
	}

	string relpath = blkpath.substr ( start.size(), string::npos );

	string linkpath = path + relpath;

	bool hard = ( type == LINK_HARD );

	// create directories

	fs_mkdir ( linkpath.c_str() );

	string groupdir = linkpath + path_sep + block_fs_group_dir;
	fs_mkdir ( groupdir.c_str() );

	string intrdir = linkpath + path_sep + block_fs_intervals_dir;
	fs_mkdir ( intrdir.c_str() );

	// compute link paths for groups and intervals

	string grouplink = linkpath + path_sep + block_fs_group_dir;
	string intrlink = linkpath + path_sep + block_fs_intervals_dir;

	// link groups

	vector < string > names;

	names = blk.all_groups();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {
		string full_link = grouplink + path_sep + (*it);
		blk.group_get( *it ).link ( type, full_link );
	}

	// link intervals

	names = blk.all_intervals();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {
		string full_link = intrlink + path_sep + (*it);
		blk.intervals_get( *it ).link ( type, full_link );
	}

	return;
}


void tidas::block_wipe_operator::operator() ( block const & blk ) {

	// wipe groups

	vector < string > names;

	names = blk.all_groups();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {
		blk.group_get( *it ).wipe();
	}

	// wipe intervals

	names = blk.all_intervals();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {
		blk.intervals_get( *it ).wipe();
	}

	// remove sub-block directories that have already been wiped.

	names = blk.all_blocks();

	for ( vector < string > :: const_iterator it = names.begin(); it != names.end(); ++it ) {
		backend_path loc = blk.block_get( *it ).location();
		string dir = loc.path + path_sep + loc.name;
		fs_rm_r ( dir.c_str() );
	}

	return;
}


tidas::block::block () {
	relocate ( loc_ );
}


tidas::block::~block () {
}


block & tidas::block::operator= ( block const & other ) {
	if ( this != &other ) {
		copy ( other, "", other.loc_ );
	}
	return *this;
}


tidas::block::block ( block const & other ) {
	copy ( other, "", other.loc_ );
}


tidas::block::block ( backend_path const & loc ) {
	relocate ( loc );
	sync();
}


tidas::block::block ( block const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::block::relocate ( backend_path const & loc ) {

	if ( loc.name == block_fs_intervals_dir ) {
		ostringstream o;
		o << "cannot give block reserved name \"" << block_fs_intervals_dir << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	if ( loc.name == block_fs_group_dir ) {
		ostringstream o;
		o << "cannot give block reserved name \"" << block_fs_group_dir << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	loc_ = loc;

	// relocate groups

	for ( map < string, group > :: iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		it->second.relocate ( group_loc ( loc_, it->first ) );
	}

	// relocate intervals

	for ( map < string, intervals > :: iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		it->second.relocate ( intervals_loc ( loc_, it->first ) );
	}

	// relocate sub blocks

	for ( map < string, block > :: iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		it->second.relocate ( block_loc ( loc_, it->first ) );
	}

	return;
}


void tidas::block::sync () {

	if ( loc_.type != BACKEND_NONE ) {

		// find all sub-blocks

		string fspath = loc_.path + path_sep + loc_.name;

		struct dirent * entry;
		DIR * dp;

		dp = opendir ( fspath.c_str() );

		if ( dp == NULL ) {
			ostringstream o;
			o << "cannot open block \"" << fspath << "\"";
			TIDAS_THROW( o.str().c_str() );
		}

		bool found_groups;
		bool found_intervals;

		while ( ( entry = readdir ( dp ) ) ) {
			string item = entry->d_name;

			if ( item == block_fs_group_dir ) {
				found_groups = true;
			} else if ( item == block_fs_intervals_dir ) {
				found_intervals = true;
			} else {
				// this must be a block!
				block_data_[ item ] = block ( block_loc ( loc_, item ) );
			}
		}

		closedir ( dp );

		if ( ( ! found_groups ) || ( ! found_intervals ) ) {
			ostringstream o;
			o << "block \"" << fspath << "\" is missing the group or intervals subdirectories!";
			TIDAS_THROW( o.str().c_str() );
		}

		// sync all groups

		string groupdir = fspath + path_sep + block_fs_group_dir;

		dp = opendir ( groupdir.c_str() );

		if ( dp == NULL ) {
			ostringstream o;
			o << "cannot open block group directory \"" << groupdir << "\"";
			TIDAS_THROW( o.str().c_str() );
		}

		while ( ( entry = readdir ( dp ) ) ) {
			string item = entry->d_name;
			group_data_[ item ] = group ( group_loc ( loc_, item ) );
		}

		closedir ( dp );

		// sync all intervals

		string intrdir = fspath + path_sep + block_fs_intervals_dir;

		dp = opendir ( intrdir.c_str() );

		if ( dp == NULL ) {
			ostringstream o;
			o << "cannot open block intervals directory \"" << intrdir << "\"";
			TIDAS_THROW( o.str().c_str() );
		}

		while ( ( entry = readdir ( dp ) ) ) {
			string item = entry->d_name;
			intervals_data_[ item ] = intervals ( intervals_loc ( loc_, item ) );
		}

		closedir ( dp );

	}

	return;
}


void tidas::block::flush () const {

	if ( loc_.type != BACKEND_NONE ) {

		if ( loc_.mode == MODE_RW ) {

			string dir = loc_.path + path_sep + loc_.name;
			fs_mkdir ( dir.c_str() );

			string groupdir = dir + path_sep + block_fs_group_dir;
			fs_mkdir ( groupdir.c_str() );

			string intrdir = dir + path_sep + block_fs_intervals_dir;
			fs_mkdir ( intrdir.c_str() );

		}

	}

	// flush groups

	for ( map < string, group > :: const_iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		it->second.flush();
	}

	// flush intervals

	for ( map < string, intervals > :: const_iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		it->second.flush();
	}

	// flush sub blocks

	for ( map < string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		it->second.flush();
	}	

	return;
}


void tidas::block::copy ( block const & other, string const & filter, backend_path const & loc ) {

	// split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX ---> [XX=XX,XX=XX] XXXX[XX=XX]/XXX

	string filt_local;
	string filt_sub;

	filter_block ( filter, filt_local, filt_sub );

	// split local filter string:  [XX=XX,XX=XX]

	map < string, string > filts = filter_split ( filt_local );

	// set location

	loc_ = loc;

	// copy groups

	string filt_name;
	string filt_pass;

	// XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]

	filter_sub ( filts[ group_submatch_key ], filt_name, filt_pass );

	regex groupre ( filt_name, std::regex::extended );

	group_data_.clear();

	for ( map < string, group > :: const_iterator it = other.group_data_.begin(); it != other.group_data_.end(); ++it ) {
		if ( regex_match ( it->first, groupre ) ) {
			group_data_[ it->first ].copy ( it->second, filt_pass, group_loc ( loc_, it->first ) );
		}
	}

	// copy intervals

	// XXX[dict=XXX] ---> XXX [dict=XXX]

	filter_sub ( filts[ intervals_submatch_key ], filt_name, filt_pass );

	regex intre ( filt_name, std::regex::extended );

	intervals_data_.clear();

	for ( map < string, intervals > :: const_iterator it = other.intervals_data_.begin(); it != other.intervals_data_.end(); ++it ) {
		if ( regex_match ( it->first, intre ) ) {
			intervals_data_[ it->first ].copy ( it->second, filt_pass, intervals_loc ( loc_, it->first ) );
		}
	}

	// copy sub-blocks

	// extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX [XX=XX]/XXX

	filter_sub ( filt_sub, filt_name, filt_pass );

	regex blockre ( filt_name, std::regex::extended );

	block_data_.clear();

	for ( map < string, block > :: const_iterator it = other.block_data_.begin(); it != other.block_data_.end(); ++it ) {
		if ( regex_match ( it->first, blockre ) ) {
			block_data_[ it->first ].copy ( it->second, filt_pass, block_loc ( loc_, it->first ) );
		}
	}

	return;
}


block tidas::block::select ( string const & filter ) const {

	block ret;
	ret.relocate ( loc_ );

	// split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX ---> [XX=XX,XX=XX] XXXX[XX=XX]/XXX

	string filt_local;
	string filt_sub;

	filter_block ( filter, filt_local, filt_sub );

	// split local filter string:  [XX=XX,XX=XX]

	map < string, string > filts = filter_split ( filt_local );

	// select groups.  Since we are not copying objects to a
	// new location, we strip off any sub-object filters.

	string filt_name;
	string filt_pass;

	// XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]

	filter_sub ( filts[ group_submatch_key ], filt_name, filt_pass );

	regex groupre ( filt_name, std::regex::extended );

	for ( map < string, group > :: const_iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		if ( regex_match ( it->first, groupre ) ) {
			ret.group_data_[ it->first ].copy ( it->second, "", group_loc ( ret.loc_, it->first ) );
		}
	}

	// select intervals.  Again, we strip off sub-filters.

	// XXX[dict=XXX] ---> XXX [dict=XXX]

	filter_sub ( filts[ intervals_submatch_key ], filt_name, filt_pass );

	regex intre ( filt_name, std::regex::extended );

	for ( map < string, intervals > :: const_iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		if ( regex_match ( it->first, intre ) ) {
			ret.intervals_data_[ it->first ].copy ( it->second, "", intervals_loc ( ret.loc_, it->first ) );
		}
	}

	// select sub-blocks

	// extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX [XX=XX]/XXX

	filter_sub ( filt_sub, filt_name, filt_pass );

	regex blockre ( filt_name, std::regex::extended );

	for ( map < string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		if ( regex_match ( it->first, blockre ) ) {
			ret.block_data_[ it->first ].copy ( it->second, filt_pass, block_loc ( ret.loc_, it->first ) );
		}
	}

	return ret;
}


void tidas::block::link ( link_type const & type, string const & path ) const {

	if ( type != LINK_NONE ) {

		if ( loc_.type != BACKEND_NONE ) {

			block_link_operator op;
			op.type = type;
			op.path = path;

			exec ( op, EXEC_DEPTH_LAST );

		}

	}

	return;
}


void tidas::block::wipe ( ) const {

	if ( loc_.type != BACKEND_NONE ) {

		if ( loc_.mode == MODE_RW ) {

			block_wipe_operator op;

			exec ( op, EXEC_DEPTH_FIRST );

		} else {
			TIDAS_THROW( "cannot wipe block in read-only mode" );
		}
	}

	return;
}


backend_path tidas::block::location () const {
	return loc_;
}


backend_path tidas::block::group_loc ( backend_path const & loc, string const & name ) const {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_group_dir;
	ret.name = name;

	return ret;
}


backend_path tidas::block::intervals_loc ( backend_path const & loc, string const & name ) const {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_intervals_dir;
	ret.name = name;

	return ret;
}


backend_path tidas::block::block_loc ( backend_path const & loc, string const & name ) const {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name;
	ret.name = name;

	return ret;
}


group & tidas::block::group_add ( string const & name, group const & grp ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot add group: block is open in read-only mode" );
	}

	if ( group_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add group with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	group_data_[ name ].copy ( grp, "", group_loc ( loc_, name ) );

	group_data_[ name ].flush();

	if ( ( loc_.type != BACKEND_NONE ) && ( grp.location().type != BACKEND_NONE ) ) {
		data_copy ( grp, group_data_.at ( name ) );
	}

	return group_data_.at( name );
}


group & tidas::block::group_get ( string const & name ) {

	if ( group_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "group with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return group_data_.at ( name );
}


group const & tidas::block::group_get ( string const & name ) const {

	if ( group_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "group with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return group_data_.at ( name );
}


void tidas::block::group_del ( string const & name ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot delete group: block is open in read-only mode" );
	}

	if ( group_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove group with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	group_data_[ name ].wipe();
	group_data_.erase ( name );

	return;
}


vector < string > tidas::block::all_groups () const {
	vector < string > ret;
	for ( map < string, group > :: const_iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		ret.push_back ( it->first );
	}
	return ret;
}


void tidas::block::clear_groups () {
	vector < string > all = all_groups();
	for ( vector < string > :: const_iterator it = all.begin(); it != all.end(); ++it ) {
		group_del ( *it );
	}
	return;
}


intervals & tidas::block::intervals_add ( string const & name, intervals const & intr ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot add intervals: block is open in read-only mode" );
	}

	if ( intervals_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add intervals with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	intervals_data_[ name ].copy ( intr, "", intervals_loc ( loc_, name ) );

	intervals_data_[ name ].flush();

	if ( ( loc_.type != BACKEND_NONE ) && ( intr.location().type != BACKEND_NONE ) ) {
		data_copy ( intr, intervals_data_.at ( name ) );
	}

	return intervals_data_.at( name );
}


intervals & tidas::block::intervals_get ( string const & name ) {

	if ( intervals_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "intervals with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return intervals_data_.at ( name );
}


intervals const & tidas::block::intervals_get ( string const & name ) const {

	if ( intervals_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "intervals with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return intervals_data_.at ( name );
}


void tidas::block::intervals_del ( string const & name ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot delete intervals: block is open in read-only mode" );
	}

	if ( intervals_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove intervals with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	intervals_data_[ name ].wipe();
	intervals_data_.erase ( name );

	return;
}


vector < string > tidas::block::all_intervals () const {
	vector < string > ret;
	for ( map < string, intervals > :: const_iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		ret.push_back ( it->first );
	}
	return ret;
}


void tidas::block::clear_intervals () {
	vector < string > all = all_intervals();
	for ( vector < string > :: const_iterator it = all.begin(); it != all.end(); ++it ) {
		intervals_del ( *it );
	}
	return;
}


block & tidas::block::block_add ( string const & name, block const & blk ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot add sub-block: block is open in read-only mode" );
	}

	if ( block_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add block with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	block_data_[ name ].copy ( blk, "", block_loc ( loc_, name ) );

	block_data_[ name ].flush();

	if ( ( loc_.type != BACKEND_NONE ) && ( blk.location().type != BACKEND_NONE ) ) {
		data_copy ( blk, block_data_.at ( name ) );
	}

	return block_data_.at ( name );
}


block & tidas::block::block_get ( string const & name ) {

	if ( block_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "block with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return block_data_.at ( name );
}


block const & tidas::block::block_get ( string const & name ) const {

	if ( block_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "block with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return block_data_.at ( name );
}


void tidas::block::block_del ( string const & name ) {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot delete sub-block: block is open in read-only mode" );
	}

	if ( block_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove block with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	block_data_[ name ].wipe();
	block_data_.erase ( name );

	return;
}


vector < string > tidas::block::all_blocks () const {
	vector < string > ret;
	for ( map < string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		ret.push_back ( it->first );
	}
	return ret;
}


void tidas::block::clear_blocks () {
	vector < string > all = all_blocks();
	for ( vector < string > :: const_iterator it = all.begin(); it != all.end(); ++it ) {
		block_del ( *it );
	}
	return;
}


void tidas::block::clear () {
	clear_groups();
	clear_intervals();
	clear_blocks();
	return;
}

