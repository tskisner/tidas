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

	if ( in.location() == out.location() ) {
		return;
	}

	if ( ( in.location().type == backend_type::none ) || ( out.location().type == backend_type::none ) ) {
		TIDAS_THROW( "cannot do data_copy between intervals with unassigned backends" );
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


void tidas::data_copy ( group const & in, group & out ) {

	if ( in.location() == out.location() ) {
		return;
	}

	if ( ( in.location().type == backend_type::none ) || ( out.location().type == backend_type::none ) ) {
		TIDAS_THROW( "cannot do data_copy between groups with unassigned backends" );
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
				case data_type::int8:
					group_helper_copy < int8_t > ( in, out, it->name, nsamp );
					break;
				case data_type::uint8:
					group_helper_copy < uint8_t > ( in, out, it->name, nsamp );
					break;
				case data_type::int16:
					group_helper_copy < int16_t > ( in, out, it->name, nsamp );
					break;
				case data_type::uint16:
					group_helper_copy < uint16_t > ( in, out, it->name, nsamp );
					break;
				case data_type::int32:
					group_helper_copy < int32_t > ( in, out, it->name, nsamp );
					break;
				case data_type::uint32:
					group_helper_copy < uint32_t > ( in, out, it->name, nsamp );
					break;
				case data_type::int64:
					group_helper_copy < int64_t > ( in, out, it->name, nsamp );
					break;
				case data_type::uint64:
					group_helper_copy < uint64_t > ( in, out, it->name, nsamp );
					break;
				case data_type::float32:
					group_helper_copy < float > ( in, out, it->name, nsamp );
					break;
				case data_type::float64:
					group_helper_copy < double > ( in, out, it->name, nsamp );
					break;
				case data_type::string:
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

	vector < string > grps = in.all_groups();

	for ( vector < string > :: const_iterator it = grps.begin(); it != grps.end(); ++it ) {
		group & ref = out.group_add ( (*it), in.group_get ( (*it) ) );
	}

	vector < string > intrs = in.all_intervals();

	for ( vector < string > :: const_iterator it = intrs.begin(); it != intrs.end(); ++it ) {
		intervals & ref = out.intervals_add ( (*it), in.intervals_get ( (*it) ) );
	}

	vector < string > blks = in.all_blocks();

	for ( vector < string > :: const_iterator it = blks.begin(); it != blks.end(); ++it ) {
		block & ref = out.block_add ( (*it), in.block_get ( (*it) ) );
	}

	return;
}


void tidas::data_copy ( volume const & in, volume & out ) {

	// just copy the root block

	data_copy ( in.root(), out.root() );

	return;
}



