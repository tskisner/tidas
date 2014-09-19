/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas/re2/re2.h>


#define TIDAS_MEM_FIELD_N 100


using namespace std;
using namespace tidas;



tidas::group::group () {

}


tidas::group::~group () {

}


tidas::group::group ( group const & other ) {
	copy ( other );
}


group & tidas::group::operator= ( group const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


void tidas::group::copy ( group const & other ) {
	schm_ = other.schm_;
	dict_ = other.dict_;
	nsamp_ = other.nsamp_;
	loc_ = other.loc_;
	if ( other.backend_ ) {
		backend_.reset( other.backend_->clone() );
	}
}


tidas::group::group ( backend_path const & loc, string const & filter ) {
	relocate ( loc );
	read_meta ( filter );
}


void tidas::group::read_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	// extract schema filter and process
	string schm_filter;
	schm_.read_meta ( schm_filter );

	// extract dictionary filter and process
	string dict_filter;
	dict_.read_meta ( dict_filter );
	
	if ( backend_ ) {
		backend_->read_meta ( loc_, filt, nsamp_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::group::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	if ( backend_ ) {
		backend_->write_meta ( loc_, filt, schm_, nsamp_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	// extract dictionary filter and process
	string dict_filter;
	dict_.write_meta ( dict_filter );

	// extract schema filter and process
	string schm_filter;
	schm_.write_meta ( schm_filter );

	return;
}


void tidas::group::relocate ( backend_path const & loc ) {
	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new group_backend_mem ( nsamp_ ) );
			break;
		case BACKEND_HDF5:
			backend_.reset( new group_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	dict_.relocate ( loc_ );

	backend_path schm_loc = loc_;
	schm_loc.meta = schema_meta;

	schm_.relocate ( schm_loc );

	return;
}


backend_path tidas::group::location () const {
	return loc_;
}


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


dict & tidas::group::dictionary () const {
	return dict_;
}


schema const & tidas::group::schema_get () const {
	return schm_;
}


index_type tidas::group::nsamp () const {
	return nsamp_;
}


void tidas::group::read_times ( vector < time_type > & data ) {
	data.resize ( nsamp_ );
	read_field ( group_time_field, 0, data );
	return;
}


void tidas::group::write_times ( vector < time_type > const & data ) {
	write_field ( group_time_field, 0, data );
	return;
}


tidas::group_backend_mem::group_backend_mem ( index_type nsamp ) {
	nsamp_ = nsamp;
}


tidas::group_backend_mem::~group_backend_mem () {

}


tidas::group_backend_mem::group_backend_mem ( group_backend_mem const & other ) {
	copy ( other );
}


group_backend_mem & tidas::group_backend_mem::operator= ( group_backend_mem const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


group_backend * tidas::group_backend_mem::clone () {
	group_backend_mem * ret = new group_backend_mem ( *this );
	ret->copy ( *this );
	return ret;
}


void tidas::group_backend_mem::copy ( group_backend_mem const & other ) {
	nsamp_ = other.nsamp_;
	data_int8_ = other.data_int8_;
	data_uint8_ = other.data_uint8_;
	data_int16_ = other.data_int16_;
	data_uint16_ = other.data_uint16_;
	data_int32_ = other.data_int32_;
	data_uint32_ = other.data_uint32_;
	data_int64_ = other.data_int64_;
	data_uint64_ = other.data_uint64_;
	data_float_ = other.data_float_;
	data_double_ = other.data_double_;
	return;
}


void tidas::group_backend_mem::read_meta ( backend_path const & loc, string const & filter, index_type & nsamp ) {
	nsamp = nsamp_;
	return;
}


void tidas::group_backend_mem::write_meta ( backend_path const & loc, string const & filter, schema const & schm, index_type nsamp ) {
	nsamp_ = nsamp;
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int8_t > & data ) {
	if ( data_int8_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_int8_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int8_t > const & data ) {
	data_int8_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int8_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint8_t > & data ) {
	if ( data_uint8_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_uint8_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint8_t > const & data ) {
	data_uint8_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint8_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int16_t > & data ) {
	if ( data_int16_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_int16_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int16_t > const & data ) {
	data_int16_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int16_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint16_t > & data ) {
	if ( data_uint16_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_uint16_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint16_t > const & data ) {
	data_uint16_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint16_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int32_t > & data ) {
	if ( data_int32_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_int32_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int32_t > const & data ) {
	data_int32_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int32_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint32_t > & data ) {
	if ( data_uint32_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_uint32_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint32_t > const & data ) {
	data_uint32_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint32_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int64_t > & data ) {
	if ( data_int64_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_int64_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < int64_t > const & data ) {
	data_int64_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int64_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint64_t > & data ) {
	if ( data_uint64_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_uint64_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < uint64_t > const & data ) {
	data_uint64_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint64_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < float > & data ) {
	if ( data_float_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_float_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < float > const & data ) {
	data_float_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_float_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, index_type offset, vector < double > & data ) {
	if ( data_double_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = data_double_[ field_name ][ offset + i ];
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, index_type offset, vector < double > const & data ) {
	data_double_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_double_[ field_name ][ offset + i ] = data[ i ];
	}
	return;
}


