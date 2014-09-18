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
	init();
}


tidas::group::~group () {
	clear();
}


tidas::group::group ( group const & other ) {
	init();
	clear();
	copy ( other );
}


group & tidas::group::operator= ( group const & other ) {
	if ( this != &other ) {
		clear();
		copy ( other );
	}
	return *this;
}


void tidas::group::init () {
	nsamp_ = 0;
	backend_ = NULL;
	return;
}


void tidas::group::clear () {
	if ( backend_ ) {
		delete backend_;
		backend_ = NULL;
	}
	return;
}


void tidas::group::copy ( group const & other ) {
	schm_ = other.schm_;
	dict_ = other.dict_;
	nsamp_ = other.nsamp_;
	loc_ = other.loc_;
	if ( other.backend_ ) {
		backend_ = other.backend_->clone();
	}
}


tidas::group::group ( backend_path const & loc, string const & filter ) {
	init();
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
		backend_->read_meta ( loc_ );
	} else {
		TIDAS_THROW( "backend not assigned" );
	}

	return;
}


void tidas::group::write_meta ( string const & filter ) {

	string filt = filter_default ( filter );

	if ( backend_ ) {
		backend_->write_meta ( loc_ );
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

	if ( backend_ ) {
		delete backend_;
	}
	backend_ = NULL;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_ = new group_backend_mem ();
			break;
		case BACKEND_HDF5:
			backend_ = new group_backend_hdf5 ();
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	backend_path dict_loc = loc_;
	dict_loc.meta = group_meta;

	dict_.relocate ( dict_loc );

	backend_path schm_loc = loc_;
	schm_loc.meta = schema_meta;

	schm_.relocate ( schema_loc );

	return;
}


backend_path tidas::group::location () const {
	return loc_;
}


template < class T >
void tidas_group_helper_copy ( group & oldgr, group & newgr, string const & field, index_type old_n, index_type new_n, interval_list const & newintr ) {

	std::vector < T > data ( old_n );
	oldgr.read_field ( field, 0, data );

	if ( new_n == old_n ) {

		// just copy the full data
		newgr.write_field ( field, 0, data );
	
	} else {
		
		// we need to copy out only the desired intervals
		std::vector < T > new_data ( new_n );
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


group tidas::group::duplicate ( std::string const & filter, backend_path const & newloc, interval_list const & intr ) {

	index_type newn = n;
	if ( selection.intr.size() > 0 ) {
		newn = intervals::total_samples ( selection.intr );
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

	tidas_group_helper_copy < time_type > ( (*this), newgroup, time_field, nsamp_, newn, intr );

	// copy all field data included in the new schema

	field_list fields = newgroup.schema_get().fields();

	for ( field_list::const_iterator it = fields.begin(); it != fields.end(); ++it ) {

		switch ( it->type ) {
			case TYPE_INT8:
				tidas_group_helper_copy < int8_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_UINT8:
				tidas_group_helper_copy < uint8_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_INT16:
				tidas_group_helper_copy < int16_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_UINT16:
				tidas_group_helper_copy < uint16_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_INT32:
				tidas_group_helper_copy < int32_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_UINT32:
				tidas_group_helper_copy < uint32_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_INT64:
				tidas_group_helper_copy < int64_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_UINT64:
				tidas_group_helper_copy < uint64_t > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_FLOAT32:
				tidas_group_helper_copy < float > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			case TYPE_FLOAT64:
				tidas_group_helper_copy < double > ( (*this), newgroup, it->name, n, newn, intr );
				break;
			default:
				TIDAS_THROW( "data type not recognized" );
				break;
		}

	}

	return newgroup;
}


dict const & tidas::group::dictionary () const {
	return dict_;
}


schema const & tidas::group::schema_get () const {
	return schm_;
}


index_type tidas::group::nsamp () const {
	return nsamp_;
}


void tidas::group::times ( vector < time_type > & data ) {
	data.resize ( nsamp_ );
	read_field ( time_field, 0, data );
	return;
}


tidas::group_backend_mem::group_backend_mem ( index_type nsamp, schema const & schm, dict const & dictionary ) {
	schm_ = schm;
	dict_ = dictionary;
	nsamp_ = nsamp;
}


tidas::group_backend_mem::~group_backend_mem () {

	
}


group_backend_mem * tidas::group_backend_mem::clone () {
	group_backend_mem * ret = new group_backend_mem ( *this );
	ret->schm_ = schm_;
	ret->dict_ = dict_;
	ret->nsamp_ = nsamp_;
	return ret;
}


void tidas::group_backend_mem::read ( backend_path const & loc, schema & schm, dict & dictionary, index_type & nsamp ) {
	schm = schm_;
	dictionary = dict_;
	nsamp = nsamp_;
	return;
}


void tidas::group_backend_mem::write ( backend_path const & loc, schema const & schm, dict const & dictionary, index_type nsamp ) {
	schm_ = schm;
	dict_ = dictionary;
	nsamp_ = nsamp;
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < int8_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int8_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < uint8_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint8_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < int16_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int16_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < uint16_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint16_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < int32_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int32_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < uint32_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint32_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < int64_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < int64_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > & data ) {
	if ( data_int_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < uint64_t > ( data_int_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < uint64_t > const & data ) {
	data_int_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > & data ) {
	if ( data_float_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < float > ( data_float_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0.0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < float > const & data ) {
	data_float_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_float_[ field_name ][ offset + i ] = static_cast < double > ( data[ i ] );
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > & data ) {
	if ( data_float_.count ( field_name ) > 0 ) {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = static_cast < double > ( data_float_[ field_name ][ offset + i ] );
		}
	} else {
		for ( index_type i = 0; i < data.size(); ++i ) {
			data[ i ] = 0.0;
		}
	}
	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, std::vector < double > const & data ) {
	data_float_[ field_name ].resize ( nsamp_ );
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_float_[ field_name ][ offset + i ] = static_cast < double > ( data[ i ] );
	}
	return;
}


