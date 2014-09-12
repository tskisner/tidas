/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


tidas::block::block () {

}


tidas::block::block ( backend_path const & loc ) {
	relocate ( loc );
}


tidas::block::block ( block const & orig ) {
	loc_ = orig.loc;
	block_list_ = orig.block_list_;
	group_list_ = orig.group_list_;
	intervals_list = orig.intervals_list_;
}


tidas::block::~block () {

}


void tidas::block::relocate ( backend_path const & loc ) {

	if ( loc.name == interval_fs_name ) {
		std::ostringstream o;
		o << "cannot give block reserved name \"" << interval_fs_name << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	if ( loc.name == group_fs_name ) {
		std::ostringstream o;
		o << "cannot give block reserved name \"" << group_fs_name << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	loc_ = loc;
	backend_path oldloc;
	backend_path newloc;

	for ( std::vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name + "/" + interval_fs_name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name + "/" + group_fs_name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	// recursively descend all child blocks...

	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); ++it ) {
		oldloc = it->location();
		newloc.type = loc_.type;
		newloc.path = loc_.path + "/" + loc_.name;
		newloc.name = oldloc.name;
		it->relocate ( newloc );
	}

	return;
}


backend_path tidas::block::location () {
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

	for ( std::vector < intervals > :: iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		it->write_meta();
	}

	// write out groups

	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		it->write_meta();
	}

	// recursively descend all child blocks...

	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); ++it ) {
		it->write_meta();
	}

	return;
}


void tidas::block::duplicate ( backend_path const & newloc, block_select const & selection ) {

	block newblock();

	// filter out intervals

	RE2 re_intr ( selection.intr_match );

	for ( std::vector < intervals > :: const_iterator it = intervals_list_.begin(); it != intervals_list_.end(); ++it ) {
		if ( RE2::FullMatch ( it->location().name, re ) ) {
			newblock.intervals_append ( it->location().name, (*it) );
		}
	}

	// filter out groups by name

	RE2 re_group ( selection.group_match );

	for ( std::vector < group > :: const_iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		if ( RE2::FullMatch ( it->location().name, re ) ) {
			newblock.group_append ( it->location().name, (*it) );
		}
	}

	// now go through


	schema schm = schema_get();
	index_type n = nsamp();

	index_type newn = n;
	if ( selection.intr.size() > 0 ) {
		newn = intervals::total_samples ( selection.intr );
	}

	block newblock ( selection.schm, newn );
	newgroup.relocate ( newloc );

	newgroup.write_meta();
	




	class block_select {

		public :

			std::string intr_match;
			std::string group_filter;
			group_select group_sel;
			std::string block_filter;
			std::map < std::string, block_select > block_sel;

	};


	return;
}


void tidas::block::block_append ( std::string const & name, block const & blk ) {
	backend_path oldloc = blk.location();

	backend_path newloc;
	newloc.type = loc_.type;
	newloc.fspath = loc_.fspath + "/" + name;
	newloc.metapath = 

	FIXME:  some kind of "rebase" option?


	return;
}


void tidas::block::group_append ( std::string const & name, group const & grp ) {

	return;
}


void tidas::block::intervals_append ( std::string const & name, intervals const & intr ) {

	return;
}



