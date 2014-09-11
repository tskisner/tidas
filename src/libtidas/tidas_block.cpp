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

	// FIXME:  relocate all blocks, intervals, groups

	return;
}


backend_path tidas::block::location () {
	return loc_;
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



