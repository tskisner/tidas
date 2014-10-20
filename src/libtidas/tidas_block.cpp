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

}


tidas::block::~block () {

}


tidas::block::block ( block const & other ) {
	copy ( other );
}


block & tidas::block::operator= ( block const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


void tidas::block::copy ( block const & other ) {
	dict_ = other.dict_;
	block_list_ = other.block_list_;
	group_list_ = other.group_list_;
	intervals_list_ = other.intervals_list_;
	loc_ = other.loc_;
}


tidas::block::block ( backend_path const & loc, string const & filter ) {
	relocate ( loc );
	read_meta ( filter );
}


void tidas::block::read_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	// read block-specific metadata

	if ( backend_ ) {
		backend_->read_meta ( loc_, filt );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	// extract dictionary filter and process
	string dict_filter;
	dict_.read_meta ( dict_filter );
	
	// read meta data from groups
	string group_filter;
	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); +it ) {
		it->read_meta ( group_filter );
	}

	// read meta data from intervals
	string intervals_filter;
	for ( std::vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); +it ) {
		it->read_meta ( intervals_filter );
	}

	// read meta data from child blocks
	string block_filter;
	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); +it ) {
		it->read_meta ( block_filter );
	}

	return;
}


void tidas::block::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	// write block-specific metadata

	if ( backend_ ) {
		backend_->write_meta ( loc_, filt );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	// extract dictionary filter and process
	string dict_filter;
	dict_.write_meta ( dict_filter );

	// write meta data to groups
	string group_filter;
	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); +it ) {
		it->write_meta ( group_filter );
	}

	// write meta data to intervals
	string intervals_filter;
	for ( std::vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); +it ) {
		it->write_meta ( intervals_filter );
	}

	// write meta data to child blocks
	string block_filter;
	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); +it ) {
		it->write_meta ( block_filter );
	}

	return;
}


void tidas::block::relocate ( backend_path const & loc ) {
	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new block_backend_mem ( nsamp_ ) );
			break;
		case BACKEND_HDF5:
			backend_.reset( new block_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	backend_path dict_loc ( loc_ );
	dict_loc.path = loc_.path + "/" + loc_.name;
	dict_loc.name = block_fs_dict_name;
	dict_loc.meta = block_dict_name;

	dict_.relocate ( loc_ );

	// relocate groups

	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); +it ) {
		backend_path group_loc ( loc_ );
		group_loc.path = loc_.path + "/" + loc_.name + "/" + block_fs_group_name;
		group_loc.name = it->location().name;
		group_loc.meta = "";
		it->relocate ( group_loc );
	}

	// relocate intervals

	for ( std::vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); +it ) {
		backend_path intervals_loc ( loc_ );
		intervals_loc.path = loc_.path + "/" + loc_.name + "/" + block_fs_intervals_name;
		intervals_loc.name = it->location().name;
		intervals_loc.meta = "";
		it->relocate ( intervals_loc );
	}

	// relocate sub-blocks

	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); +it ) {
		backend_path block_loc ( loc_ );
		block_loc.path = loc_.path + "/" + loc_.name;
		block_loc.name = it->location().name;
		block_loc.meta = "";
		it->relocate ( block_loc );
	}

	return;
}


backend_path tidas::block::location () const {
	return loc_;
}


// FIXME:  duplicate should not touch metadata?  calling duplicate recursively will do the 
// metadata copy twice...



block tidas::block::duplicate ( string const & filter, backend_path const & newloc ) {

	block newblock;

	newblock.dict_ = dict_;
	newblock.block_list_ = block_list_;
	newblock.group_list_ = group_list_;
	newblock.intervals_list_ = intervals_list_;

	newblock.relocate ( newloc );
	newblock.write_meta ( filter );

	// reload to pick up filtered metadata
	newblock.read_meta ( "" );

	// duplicate groups
	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); +it ) {

	// duplicate intervals


	// duplicate sub-blocks



	return newgroup;
}





void tidas::block::relocate ( backend_path const & loc ) {

	if ( loc.name == interval_fs_name ) {
		ostringstream o;
		o << "cannot give block reserved name \"" << interval_fs_name << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	if ( loc.name == group_fs_name ) {
		ostringstream o;
		o << "cannot give block reserved name \"" << group_fs_name << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	loc_ = loc;
	backend_path oldloc;
	backend_path newloc;

	for ( vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name + "/" + interval_fs_name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	for ( vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name + "/" + group_fs_name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	// recursively descend all child blocks...

	for ( vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	return;
}


backend_path tidas::block::location () const {
	return loc_;
}


void tidas::block::read_meta () {


	return;
}


void tidas::block::write_meta () {

	// make our parent directory if it does not exist

	fs_mkdir ( loc_.path.c_str() );

	string fspath = loc_.path + "/" + loc_.name;

	// make our directory if it does not exist

	fs_mkdir ( fspath.c_str() );

	// make group and interval directories

	string dirpath = fspath + "/" + interval_fs_name;
	fs_mkdir ( dirpath.c_str() );

	dirpath = fspath + "/" + group_fs_name;
	fs_mkdir ( dirpath.c_str() );

	// write out intervals

	for ( vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		it->write_meta();
	}

	// write out groups

	for ( vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		it->write_meta();
	}

	// recursively descend all child blocks...

	for ( vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); ++it ) {
		it->write_meta();
	}

	return;
}


void tidas::block::duplicate ( backend_path const & newloc, block_select const & selection ) {

	block newblock;

	// filter out intervals

	RE2 re_intr ( selection.intr_match );

	for ( vector < intervals > :: const_iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		if ( RE2::FullMatch ( it->location().name, re_intr ) ) {
			newblock.intervals_append ( it->location().name, (*it) );
		}
	}

	// filter out groups by name

	RE2 re_group ( selection.group_filter );

	for ( vector < group > :: const_iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		if ( RE2::FullMatch ( it->location().name, re_group ) ) {
			newblock.group_append ( it->location().name, (*it) );
		}
	}

	// now go through

	/*

	schema schm = schema_get();
	index_type n = nsamp();

	index_type newn = n;
	if ( selection.intr.size() > 0 ) {
		newn = intervals::total_samples ( selection.intr );
	}

	block newblock ( selection.schm, newn );
	newgroup.relocate ( newloc );

	newgroup.write_meta();
	
	*/


	return;
}




tidas::block_backend_mem::block_backend_mem () : block_backend () {

}


tidas::block_backend_mem::~block_backend_mem () {

}


tidas::block_backend_mem::block_backend_mem ( block_backend_mem const & other ) {
	store_ = other.store_;
}


block_backend_mem & tidas::block_backend_mem::operator= ( block_backend_mem const & other ) {
	if ( this != &other ) {
		store_ = other.store_;
	}
	return *this;
}


block_backend * tidas::block_backend_mem::clone () {
	block_backend_mem * ret = new block_backend_mem ( *this );
	ret->store_ = store_;
	return ret;
}


void tidas::block_backend_mem::read_meta ( backend_path const & loc ) {

	return;
}


void tidas::block_backend_mem::write_meta ( backend_path const & loc ) {

	return;
}


void tidas::block_backend_mem::read_data ( backend_path const & loc, interval_list & intr ) {
	intr = store_;
	return;
}


void tidas::block_backend_mem::write_data ( backend_path const & loc, interval_list const & intr ) {
	store_ = intr;
	return;
}






