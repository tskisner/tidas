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


void tidas::block::read () {


	return;
}


void tidas::block::write () {

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
		it->write();
	}

	// write out groups

	for ( std::vector < group > :: iterator it = group_list_.begin(); it != group_list_.end(); ++it ) {
		it->write();
	}

	// recursively descend all child blocks...

	for ( std::vector < block > :: iterator it = block_list_.begin(); it != block_list_.end(); ++it ) {
		it->write();
	}

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



