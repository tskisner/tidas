/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


#define TIDAS_MEM_FIELD_N 100


using namespace std;
using namespace tidas;


tidas::group_backend_mem::group_backend_mem ( index_type nsamp, schema const & schm ) {
	schm_ = schm;
	nsamp_ = nsamp;
}


tidas::group_backend_mem::~group_backend_mem () {

	
}


group_backend_mem * tidas::group_backend_mem::clone () {
	group_backend_mem * ret = new group_backend_mem ( *this );
	return ret;
}


void tidas::group_backend_mem::read ( backend_path const & loc, schema & schm, index_type & nsamp ) {
	schm = schm_;
	nsamp = nsamp_;
	return;
}


void tidas::group_backend_mem::write ( backend_path const & loc, schema const & schm, index_type nsamp ) {
	schm_ = schm;
	nsamp_ = nsamp;
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data ) {

	switch ( type ) {
		case TYPE_INT8:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int8_t * > ( data ) )[ i ] = static_cast < int8_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int8_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_UINT8:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint8_t * > ( data ) )[ i ] = static_cast < uint8_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint8_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_INT16:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int16_t * > ( data ) )[ i ] = static_cast < int16_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int16_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_UINT16:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint16_t * > ( data ) )[ i ] = static_cast < uint16_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint16_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_INT32:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int32_t * > ( data ) )[ i ] = static_cast < int32_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int32_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_UINT32:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint32_t * > ( data ) )[ i ] = static_cast < uint32_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint32_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_INT64:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int64_t * > ( data ) )[ i ] = static_cast < int64_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < int64_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_UINT64:
			if ( data_int_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint64_t * > ( data ) )[ i ] = static_cast < uint64_t > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < uint64_t * > ( data ) )[ i ] = 0;
				}
			}
			break;
		case TYPE_FLOAT32:
			if ( data_float_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < float * > ( data ) )[ i ] = static_cast < float > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < float * > ( data ) )[ i ] = 0.0;
				}
			}
			break;
		case TYPE_FLOAT64:
			if ( data_float_.count ( field_name ) > 0 ) {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < double * > ( data ) )[ i ] = static_cast < double > ( data_int_[ field_name ][ offset + i ] );
				}
			} else {
				for ( index_type i = 0; i < n; ++i ) {
					( static_cast < double * > ( data ) )[ i ] = 0.0;
				}
			}
			break;
		default:
			TIDAS_THROW( "data type not recognized" );
			break;
	}

	return;
}


void tidas::group_backend_mem::write_field ( backend_path const & loc, std::string const & field_name, index_type offset, index_type n, data_type type, void * data ) {

	switch ( type ) {
		case TYPE_INT8:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < int8_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_UINT8:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < uint8_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_INT16:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < int16_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_UINT16:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < uint16_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_INT32:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < int32_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_UINT32:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < uint32_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_INT64:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < int64_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_UINT64:
			data_int_[ field_name ].reserve ( nsamp_ );
			data_int_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_int_[ field_name ][ offset + i ] = static_cast < int64_t > ( ( static_cast < uint64_t * > ( data ) )[ i ] );
			}
			break;
		case TYPE_FLOAT32:
			data_float_[ field_name ].reserve ( nsamp_ );
			data_float_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_float_[ field_name ][ offset + i ] = static_cast < double > ( ( static_cast < float * > ( data ) )[ i ] );
			}
			break;
		case TYPE_FLOAT64:
			data_float_[ field_name ].reserve ( nsamp_ );
			data_float_[ field_name ].resize ( nsamp_ );
			for ( index_type i = 0; i < n; ++i ) {
				data_float_[ field_name ][ offset + i ] = static_cast < double > ( ( static_cast < double * > ( data ) )[ i ] );
			}
			break;
		default:
			TIDAS_THROW( "data type not recognized" );
			break;
	}

	return;
}


tidas::group::group () {
	nsamp_ = 0;
	backend_ = NULL;
}


tidas::group::group ( schema const & schm, index_type nsamp ) {
	schm_ = schm;
	nsamp_ = nsamp;
	backend_ = NULL;
}


tidas::group::group ( backend_path const & loc ) {
	backend_ = NULL;
	relocate ( loc );
	read ();
}


tidas::group::group ( group const & orig ) {
	schm_ = orig.schm_;
	nsamp_ = orig.nsamp_;
	loc_ = orig.loc_;
	backend_ = orig.backend_->clone();
}


tidas::group::~group () {
	if ( backend_ ) {
		delete backend_;
	} else {
		TIDAS_THROW( "In destructor, group backend is NULL.  This should never happen..." );
	}
}


void tidas::group::read () {

	backend_->read ( loc_, schm_, nsamp_ );

	return;
}


void tidas::group::write () {

	backend_->write ( loc_, schm_, nsamp_ );

	return;
}


void tidas::group::relocate ( backend_path const & loc ) {

	loc_ = loc;

	if ( backend_ ) {
		delete backend_;
	}	

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_ = new group_backend_mem ( nsamp_, schm_ );
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

	return;
}


backend_path tidas::group::location () {
	return loc_;
}


void tidas::group::duplicate ( backend_path const & newloc ) {

	schema schm = schema_get();
	index_type n = nsamp();

	group newgroup ( schm, n );
	newgroup.relocate ( newloc );

	newgroup.write();
	
	// copy tidas time field

	std::vector < time_type > data_time ( n );
	read_field ( time_field, 0, data_time );
	newgroup.write_field ( time_field, 0, data_time );
	data_time.resize(0);

	// copy all field data

	field_list fields = schm.fields();

	for ( field_list::const_iterator it = fields.begin(); it != fields.end(); ++it ) {

	}



	return;
}


schema const & tidas::group::schema_get () const {
	return schm_;
}


index_type tidas::group::nsamp () const {
	return nsamp_;
}


void tidas::group::time_cache () {
	if ( times_.size() != nsamp_ ) {
		times_.resize ( nsamp_ );
		read_field ( time_field, 0, times_ );
	}
	return;
}


void tidas::group::time_flush () {
	if ( times_.size() > 0 ) {
		times_.resize(0);
	}
	return;
}


std::vector < time_type > const & tidas::group::times() {
	time_cache();
	return times_;
}


intrvl tidas::group::range_time ( span const & spn ) {
	intrvl ret;
	bool cached = ( times_.size() > 0 );

	if ( ! cached ) {
		// read the times for just these samples
		std::vector < time_type > data ( 1 );

		read_field ( time_field, spn.first, data );
		ret.first = data[0];

		read_field ( time_field, spn.second, data );
		ret.second = data[0];
	} else {
		// grab values from the cache
		ret.first = times_[ spn.first ];
		ret.second = times_[ spn.second ];
	}

	return ret;
}


span tidas::group::range_index_inner ( intrvl const & intv ) {
	span ret;
	bool cached = ( times_.size() > 0 );

	if ( ! cached ) {
		time_cache();
	}

	time_type start = times_[0]
	time_type stop = times_[ nsamp_ - 1 ];

	if ( ( intv.first > stop ) || ( intv.second < start ) ) {

		// no overlap
		ret.first = -1;
		ret.second = -1;
	
	} else {

		index_type guess_low = 0;
		if ( intv.first >= start ) {
			guess_low = (index_type)( (time_type)nsamp_ * ( intv.first - start ) / ( stop - start ) );
		}

		index_type guess_high = 0;
		if ( intv.second >= start ) {
			guess_high = (index_type)( (time_type)nsamp_ * ( intv.second - start ) / ( stop - start ) );
		}

		// linear search for correct samples

		while ( ( guess_low > 0 ) && ( times_[ guess_low - 1 ] >= intv.first ) ) {
			--guess_low;
		}

		while ( ( guess_low < nsamp_ - 1 ) && ( times_[ guess_low + 1 ] <= intv.first ) ) {
			++guess_low;
		}

		while ( ( guess_high > 0 ) && ( times_[ guess_high - 1 ] >= intv.second ) ) {
			--guess_high;
		}

		while ( ( guess_high < nsamp_ - 1 ) && ( times_[ guess_high + 1 ] <= intv.second ) ) {
			++guess_high;
		}

		if ( guess_high < guess_low ) {
			// the time interval lies fully between two samples
			ret.first = -1;
			ret.second = -1;
		} else {
			ret.first = guess_low;
			ret.second = guess_high;
		}

	}

	if ( ! cached ) {
		time_flush();
	}

	return ret;
}


span tidas::group::range_index_outer ( intrvl const & intv ) {

	span ret = rang_index_inner ( intv );

	// enlarge by one sample if needed



	return ret;
}







