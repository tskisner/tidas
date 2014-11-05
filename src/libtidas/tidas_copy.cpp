/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


void tidas::data_copy ( intervals & in, intervals & out ) {

	if ( in.location() == out.location() ) {
		TIDAS_THROW( "cannot do data_copy between intervals with same location" );
	}

	if ( in.size() != out.size() ) {
		ostringstream o;
		o << "cannot data_copy between input intervals of size " << in.size() << " and output intervals of size " << out.size();
		TIDAS_THROW( o.str().c_str() );
	}

	interval_list intr;

	in.read_data ( intr );
	out.write_data ( intr );

	return;
}

/*
void tidas::data_copy ( group & in, group & out ) {


	return;
}


void tidas::data_copy ( block & in, block & out ) {


	return;
}


void tidas::data_copy ( volume & in, volume & out ) {


	return;
}


*/

