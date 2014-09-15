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



tidas::volume::volume ( std::string const & path, backend_type type ) {

}


tidas::volume::volume ( std::string const & path ) {

}



tidas::volume::~volume () {

}


void tidas::volume::read_meta () {


	return;
}



void tidas::volume::write_meta () {


	return;
}



void tidas::volume::index () {


	return;
}



select tidas::volume::query ( std::string const & match ) {
	select ret;
	return ret;
}



block & tidas::volume::root () {
	return root_;
}



void tidas::volume::duplicate ( std::string const & newpath, backend_type newtype, select const & selection ) {


	return;
}



void tidas::volume::block_append ( std::string const & name, block const & blk ) {


	return;
}



