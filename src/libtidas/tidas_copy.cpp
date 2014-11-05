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


template < class T >
void tidas_group_helper_copy ( group & oldgr, group & newgr, string const & field, index_type old_n, index_type new_n, interval_list const & newintr ) {

	vector < T > data ( old_n );
	oldgr.read_field ( field, 0, data );

	if ( new_n == old_n ) {

		// just copy the full data
		newgr.write_field ( field, 0, data );
	
	} else {
		
		// we need to copy out only the desired intervals
		vector < T > new_data ( new_n );
		index_type offset = 0;
		for ( interval_list::const_iterator it = newintr.begin(); it != newintr.end(); ++it ) {
			index_type nint = it->last - it->first + 1;
			for ( index_type i = 0; i < nint; ++i ) {
				new_data[ offset + i ] = data[ it->first + i ];
			}
			offset += nint;
		}
		newgr.write_field ( field, 0, new_data );

	}

	return;
}


group tidas::group::duplicate ( string const & filter, backend_path const & newloc, interval_list const & intr ) {

	index_type newn = nsamp_;
	if ( intr.size() > 0 ) {
		newn = intervals::total_samples ( intr );
	}

	group newgroup;
	newgroup.schm_ = schm_;
	newgroup.dict_ = dict_;
	newgroup.nsamp_ = nsamp_;
	newgroup.relocate ( newloc );
	newgroup.write_meta ( filter );

	// reload to pick up filtered metadata
	newgroup.read_meta ( "" );

	// copy tidas time field

	tidas_group_helper_copy < time_type > ( (*this), newgroup, group_time_field, nsamp_, newn, intr );

	// copy all field data included in the new schema

	field_list fields = newgroup.schema_get().fields();

	for ( field_list::const_iterator it = fields.begin(); it != fields.end(); ++it ) {

		switch ( it->type ) {
			case TYPE_INT8:
				tidas_group_helper_copy < int8_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_UINT8:
				tidas_group_helper_copy < uint8_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_INT16:
				tidas_group_helper_copy < int16_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_UINT16:
				tidas_group_helper_copy < uint16_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_INT32:
				tidas_group_helper_copy < int32_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_UINT32:
				tidas_group_helper_copy < uint32_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_INT64:
				tidas_group_helper_copy < int64_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_UINT64:
				tidas_group_helper_copy < uint64_t > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_FLOAT32:
				tidas_group_helper_copy < float > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			case TYPE_FLOAT64:
				tidas_group_helper_copy < double > ( (*this), newgroup, it->name, nsamp_, newn, intr );
				break;
			default:
				TIDAS_THROW( "data type not recognized" );
				break;
		}

	}

	return newgroup;
}

*/


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

