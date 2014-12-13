/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


void tidas::data_copy ( intervals const & in, intervals & out ) {

	if ( ( in.location() == out.location() ) && ( in.location().type != BACKEND_NONE ) ) {
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

void tidas::data_copy ( group const & in, group & out ) {

	if ( ( in.location() == out.location() ) && ( in.location().type != BACKEND_MEM ) ) {
		TIDAS_THROW( "cannot do data_copy between groups with same location" );
	}

	if ( in.size() != out.size() ) {
		ostringstream o;
		o << "cannot data_copy between input group of size " << in.size() << " and output group of size " << out.size();
		TIDAS_THROW( o.str().c_str() );
	}

	index_type nsamp = out.size();

	// copy time field
	group_helper_copy < time_type > ( in, out, group_time_field, nsamp );

	schema schm = out.schema_get();

	field_list fields = schm.fields();

	// copy other fields

	for ( field_list :: const_iterator it = fields.begin(); it != fields.end(); ++it ) {

		if ( it->name != group_time_field ) {

			switch ( it->type ) {
				case TYPE_INT8:
					group_helper_copy < int8_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_UINT8:
					group_helper_copy < uint8_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_INT16:
					group_helper_copy < int16_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_UINT16:
					group_helper_copy < uint16_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_INT32:
					group_helper_copy < int32_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_UINT32:
					group_helper_copy < uint32_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_INT64:
					group_helper_copy < int64_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_UINT64:
					group_helper_copy < uint64_t > ( in, out, it->name, nsamp );
					break;
				case TYPE_FLOAT32:
					group_helper_copy < float > ( in, out, it->name, nsamp );
					break;
				case TYPE_FLOAT64:
					group_helper_copy < double > ( in, out, it->name, nsamp );
					break;
				case TYPE_STRING:
					group_helper_copy < string > ( in, out, it->name, nsamp );
					break;
				default:
					TIDAS_THROW( "data type not recognized" );
					break;
			}

		}

	}

	return;
}


void tidas::data_copy ( block const & in, block & out ) {


	return;
}
*/

/*

void tidas::data_copy ( volume & in, volume & out ) {


	return;
}


*/

