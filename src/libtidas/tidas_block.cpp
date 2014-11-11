/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas/re2/re2.h>


using namespace std;
using namespace tidas;




tidas::block::block () {
	relocate ( loc_ );
	flush();
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
	sync ();
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

	backend_path grouploc = loc_;
	grouploc.path = loc_.path + path_sep + loc_.name + path_sep + block_fs_group_dir;

	for ( map < string, group > :: iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		grouploc.name = it->first;
		it->second.relocate ( grouploc );
	}

	// relocate intervals

	backend_path intrloc = loc_;
	intrloc.path = loc_.path + path_sep + loc_.name + path_sep + block_fs_intervals_dir;

	for ( map < string, intervals > :: iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		intrloc.name = it->first;
		it->second.relocate ( intrloc );
	}

	// relocate sub blocks

	backend_path blockloc = loc_;
	blockloc.path = loc_.path + path_sep + loc_.name;

	for ( map < string, block > :: iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		blockloc.name = it->first;
		it->second.relocate ( blockloc );
	}

	return;
}


void tidas::block::sync () {

	// sync groups

	for ( map < string, group > :: iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		it->second.sync();
	}

	// sync intervals

	for ( map < string, intervals > :: iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		it->second.sync();
	}

	// sync sub blocks

	for ( map < string, block > :: iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		it->second.sync();
	}

	return;
}


void tidas::block::flush () {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot flush to read-only location" );
	}

	// flush sub blocks

	for ( map < string, block > :: iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		it->second.flush();
	}

	// flush groups

	for ( map < string, group > :: iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		it->second.flush();
	}

	// flush intervals

	for ( map < string, intervals > :: iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		it->second.flush();
	}

	return;
}


void tidas::block::copy ( block const & other, string const & filter, backend_path const & loc ) {

	// first split filter string into our local filter and the
	// filters to pass to sub-blocks.

	string filt_local;
	string filt_sub;

	filter_block ( filter, filt_local, filt_sub );

	// extract local filters

	map < string, string > local = filter_split ( filt_local );

	// are we copying to a new location?  if so, create directories

	if ( ( loc != other.loc_ ) && ( loc.type != BACKEND_MEM ) ) {

		string dir = loc.path + path_sep + loc.name;
		fs_mkdir ( dir.c_str() );

		string groupdir = dir + path_sep + block_fs_group_dir;
		fs_mkdir ( groupdir.c_str() );

		string intrdir = dir + path_sep + block_fs_intervals_dir;
		fs_mkdir ( intrdir.c_str() );

	}

	// set location

	loc_ = loc;

	// copy groups

	string filt_name;
	string filt_pass;

	filter_sub ( local[ group_submatch_key ], filt_name, filt_pass );

	RE2 groupre ( filt_name );

	group_data_.clear();

	for ( map < string, group > :: const_iterator it = other.group_data_.begin(); it != other.group_data_.end(); ++it ) {
		if ( RE2::FullMatch ( it->first, groupre ) ) {
			group_add ( it->first, it->second );
		}
	}

	// copy intervals

	filter_sub ( local[ intervals_submatch_key ], filt_name, filt_pass );

	RE2 intre ( filt_name );

	intervals_data_.clear();

	for ( map < string, intervals > :: const_iterator it = other.intervals_data_.begin(); it != other.intervals_data_.end(); ++it ) {
		if ( RE2::FullMatch ( it->first, intre ) ) {
			intervals_add ( it->first, it->second );
		}
	}

	// copy sub-blocks

	string filt_local_block;
	string filt_sub_block;

	filter_block ( filt_sub, filt_local_block, filt_sub_block );

	filter_sub ( filt_local_block, filt_name, filt_pass );

	string filt_block_pass;
	if ( filt_sub_block == "" ) {
		filt_block_pass = filt_pass;
	} else {
		if ( filt_pass == "" ) {
			filt_block_pass = filt_sub_block;
		} else {
			filt_block_pass = filt_pass + path_sep + filt_sub_block;
		}
	}

	RE2 blockre ( filt_name );

	block_data_.clear();

	for ( map < string, block > :: const_iterator it = other.block_data_.begin(); it != other.block_data_.end(); ++it ) {
		if ( RE2::FullMatch ( it->first, blockre ) ) {
			block_add ( it->first, it->second );
		}
	}

	return;
}


void tidas::block::duplicate ( backend_path const & loc ) {

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot duplicate to read-only location" );
	}

	// write our directory structure

	if ( loc.type != BACKEND_MEM ) {

		string dir = loc.path + path_sep + loc.name;
		fs_mkdir ( dir.c_str() );

		string groupdir = dir + path_sep + block_fs_group_dir;
		fs_mkdir ( groupdir.c_str() );

		string intrdir = dir + path_sep + block_fs_intervals_dir;
		fs_mkdir ( intrdir.c_str() );

	}

	// duplicate groups

	for ( map < string, group > :: iterator it = group_data_.begin(); it != group_data_.end(); ++it ) {
		it->second.duplicate ( group_loc ( loc, it->first ) );
	}

	// duplicate intervals

	for ( map < string, intervals > :: iterator it = intervals_data_.begin(); it != intervals_data_.end(); ++it ) {
		it->second.duplicate ( intervals_loc ( loc, it->first ) );
	}

	// duplicate sub blocks

	for ( map < string, block > :: iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
		it->second.duplicate ( block_loc ( loc, it->first ) );
	}

	return;
}


backend_path tidas::block::location () const {
	return loc_;
}


backend_path tidas::block::group_loc ( backend_path const & loc, string const & name ) {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_group_dir;
	ret.name = name;

	return ret;
}


backend_path tidas::block::intervals_loc ( backend_path const & loc, string const & name ) {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_intervals_dir;
	ret.name = name;

	return ret;
}


backend_path tidas::block::block_loc ( backend_path const & loc, string const & name ) {
	backend_path ret = loc;

	ret.path = loc.path + path_sep + loc.name;
	ret.name = name;

	return ret;
}


group & tidas::block::group_add ( string const & name, group const & grp ) {

	if ( group_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add group with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	group_data_[ name ].copy ( grp, "", group_loc ( loc_, name ) );

	return group_data_.at ( name );
}


group & tidas::block::group_get ( string const & name ) {

	if ( group_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "group with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return group_data_.at ( name );
}


void tidas::block::group_del ( string const & name ) {

	if ( group_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove group with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	group_data_.erase ( name );

	return;
}


intervals & tidas::block::intervals_add ( string const & name, intervals const & intr ) {

	if ( intervals_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add intervals with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	intervals_data_[ name ].copy ( intr, "", intervals_loc ( loc_, name ) );

	return intervals_data_.at ( name );
}


intervals & tidas::block::intervals_get ( string const & name ) {

	if ( intervals_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "intervals with name \"" << name << "\" does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	return intervals_data_.at ( name );
}


void tidas::block::intervals_del ( string const & name ) {

	if ( intervals_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove intervals with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	intervals_data_.erase ( name );

	return;
}


block & tidas::block::block_add ( string const & name, block const & blk ) {

	if ( block_data_.count ( name ) > 0 ) {
		ostringstream o;
		o << "cannot add block with name \"" << name << "\": already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	block_data_[ name ].copy ( blk, "", block_loc ( loc_, name ) );

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


void tidas::block::block_del ( string const & name ) {

	if ( block_data_.count ( name ) == 0 ) {
		ostringstream o;
		o << "cannot remove block with name \"" << name << "\": does not exist";
		TIDAS_THROW( o.str().c_str() );
	}

	block_data_.erase ( name );

	return;
}





