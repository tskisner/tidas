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


class volume {

		public :

tidas::volume::volume () {


}


volume ( string const & path, string const & filter ) {


}


~volume () {


	// free index if needed

}


void write ( string const & path, string const & filter, backend_type type, compression_type comp ) {

	return;
}


void duplicate ( string const & path, string const & filter, backend_type type, compression_type comp ) {

	return;
}


void index () {

	dirty_ = false;
}


void set_dirty () {
	dirty_ = true;
}


vector < block > & blocks () {
	return root_.blocks();
}


block & block_append ( string const & name, block const & blk ) {
	backend_path blockloc;
	blockloc.type = loc_.type;
	blockloc.comp = loc_.comp;
	blockloc.path = loc_.path;
	blockloc.meta = "";
	blockloc.name = name;
	blockloc.vol = this;

	root_.blocks().push_back( block( blk ) );

	vector < block > iterator final = root_.blocks().rbegin();

	final->relocate ( blockloc );

	return (*final);
}


