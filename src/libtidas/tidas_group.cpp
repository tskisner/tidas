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
	size_ = 0;
}


tidas::group::group ( schema const & schm, dict const & d, size_t const & size ) {
	size_ = size;
	dict_ = d;
	schm_ = schm;
	compute_counts();
}


tidas::group::~group () {

}


group & tidas::group::operator= ( group const & other ) {
	if ( this != &other ) {
		copy ( other, "", other.loc_ );
	}
	return *this;
}


tidas::group::group ( group const & other ) {
	copy ( other, "", other.loc_ );
}


tidas::group::group ( backend_path const & loc ) {
	read ( loc );
}


tidas::group::group ( group const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::group::read ( backend_path const & loc ) {

	// set backend

	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new group_backend_mem () );
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

	// read dict

	backend_path dictloc = loc_;
	dictloc.meta = backend_->dict_meta();

	dict_.read ( dictloc );

	// read schema

	backend_path schmloc = loc_;
	schmloc.meta = backend_->schema_meta();

	schm_.read ( schmloc );

	compute_counts();

	// read our own metadata

	map < data_type, size_t > check;
	backend_->read ( loc_, size_, check );

	// verify that schema counts match data file

	if ( check != counts_ ) {
		TIDAS_THROW( "number of fields of each type in group schema does not match data!" );
	}

	return;
}


void tidas::group::copy ( group const & other, string const & filter, backend_path const & loc ) {	

	// extract filters

	map < string, string > filts = filter_split ( filter );

	string fil = filter_default ( filts[ "root" ] );

	if ( ( loc == other.loc_ ) && ( fil != ".*" ) ) {
		TIDAS_THROW( "copy of group with a filter requires a new location" );
	}

	// set backend

	loc_ = loc;

	switch ( loc_.type ) {
		case BACKEND_MEM:
			backend_.reset( new group_backend_mem () );
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

	// filtered copy of our metadata

	if ( fil != ".*" ) {
		TIDAS_THROW( "group class does not accept filters- apply to schema instead" );
	}
	size_ = other.size_;

	if ( loc_ != other.loc_ ) {
		if ( loc_.mode == MODE_RW ) {

			// copy schema first, in order to filter fields

			backend_path schmloc = loc_;
			schmloc.meta = backend_->schema_meta();

			schm_.copy ( other.schm_, filts[ schema_submatch_key ], schmloc );

			// compute counts

			compute_counts();

			// write our metadata

			backend_->write ( loc_, size_, counts_ );

			// copy dict

			backend_path dictloc = loc_;
			dictloc.meta = backend_->dict_meta();

			dict_.copy ( other.dict_, filts[ dict_submatch_key ], dictloc );

		} else {
			TIDAS_THROW( "cannot copy to new read-only location" );
		}
	} else {
		// just copy in memory
		dict_ = other.dict_;
		schm_ = other.schm_;
		counts_ = other.counts_;
		type_indx_ = other.type_indx_;
	}

	return;
}


void tidas::group::write ( backend_path const & loc ) {

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot write to read-only location" );
	}

	// write our metadata

	unique_ptr < group_backend > backend;

	switch ( loc.type ) {
		case BACKEND_MEM:
			backend.reset( new group_backend_mem () );
			break;
		case BACKEND_HDF5:
			backend.reset( new group_backend_hdf5 () );
			break;
		case BACKEND_GETDATA:
			TIDAS_THROW( "GetData backend not yet implemented" );
			break;
		default:
			TIDAS_THROW( "backend not recognized" );
			break;
	}

	backend->write ( loc, size_, counts_ );

	// write schema

	backend_path schmloc = loc;
	schmloc.meta = backend->schema_meta();

	schm_.write ( schmloc );

	// write dict

	backend_path dictloc = loc;
	dictloc.meta = backend->dict_meta();

	dict_.write ( dictloc );

	return;
}


backend_path tidas::group::location () const {
	return loc_;
}


void tidas::group::compute_counts() {

	// clear counts
	counts_.clear();
	counts_[ TYPE_INT8 ] = 0;
	counts_[ TYPE_UINT8 ] = 0;
	counts_[ TYPE_INT16 ] = 0;
	counts_[ TYPE_UINT16 ] = 0;
	counts_[ TYPE_INT32 ] = 0;
	counts_[ TYPE_UINT32 ] = 0;
	counts_[ TYPE_INT64 ] = 0;
	counts_[ TYPE_UINT64 ] = 0;
	counts_[ TYPE_FLOAT32 ] = 0;
	counts_[ TYPE_FLOAT64 ] = 0;
	counts_[ TYPE_STRING ] = 0;

	// clear type_indx
	type_indx_.clear();

	// we always have a time field
	type_indx_[ group_time_field ] = counts_[ TYPE_FLOAT64 ];
	++counts_[ TYPE_FLOAT64 ];

	field_list fields = schm_.fields();

	for ( field_list::const_iterator it = fields.begin(); it != fields.end(); ++it ) {
		if ( it->name == group_time_field ) {
			// ignore time field, if it exists, since we already counted it 
		} else {
			if ( it->type == TYPE_NONE ) {
				ostringstream o;
				o << "group schema field \"" << it->name << "\" has type == TYPE_NONE";
				TIDAS_THROW( o.str().c_str() );
			}
			type_indx_[ it->name ] = counts_[ it->type ];
			++counts_[ it->type ];
		}
	}

	return;
}



dict const & tidas::group::dictionary () const {
	return dict_;
}


schema const & tidas::group::schema_get () const {
	return schm_;
}


index_type tidas::group::size () const {
	return size_;
}


void tidas::group::read_times ( vector < time_type > & data ) {
	data.resize ( size_ );
	read_field ( group_time_field, 0, data );
	return;
}


void tidas::group::write_times ( vector < time_type > const & data ) {
	write_field ( group_time_field, 0, data );
	return;
}


tidas::group_backend_mem::group_backend_mem () {

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
	counts_ = other.counts_;
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
	data_string_ = other.data_string_;
	return;
}


void tidas::group_backend_mem::read ( backend_path const & loc, index_type & nsamp, map < data_type, size_t > & counts ) {
	nsamp = nsamp_;
	counts = counts_;
	return;
}


void tidas::group_backend_mem::write ( backend_path const & loc, index_type const & nsamp, map < data_type, size_t > const & counts ) {
	nsamp_ = nsamp;
	counts_ = counts;
	reset_data ( data_int8_, counts_[ TYPE_INT8 ], nsamp_ );
	reset_data ( data_uint8_, counts_[ TYPE_UINT8 ], nsamp_ );
	reset_data ( data_int16_, counts_[ TYPE_INT16 ], nsamp_ );
	reset_data ( data_uint16_, counts_[ TYPE_UINT16 ], nsamp_ );
	reset_data ( data_int32_, counts_[ TYPE_INT32 ], nsamp_ );
	reset_data ( data_uint32_, counts_[ TYPE_UINT32 ], nsamp_ );
	reset_data ( data_int64_, counts_[ TYPE_INT64 ], nsamp_ );
	reset_data ( data_uint64_, counts_[ TYPE_UINT64 ], nsamp_ );
	reset_data ( data_float_, counts_[ TYPE_FLOAT32 ], nsamp_ );
	reset_data ( data_double_, counts_[ TYPE_FLOAT64 ], nsamp_ );
	reset_data ( data_string_, counts_[ TYPE_STRING ], nsamp_ );
	return;
}


string tidas::group_backend_mem::dict_meta () {
	return string("");
}


string tidas::group_backend_mem::schema_meta () {
	return string("");
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_int8_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int8_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_uint8_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint8_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_int16_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int16_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_uint16_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint16_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_int32_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int32_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_uint32_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint32_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_int64_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_int64_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_uint64_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_uint64_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_float_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_float_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_double_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_double_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data[ i ] = data_string_[ type_indx ][ offset + i ];
	}
	return;
}

void tidas::group_backend_mem::write_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > const & data ) {
	for ( index_type i = 0; i < data.size(); ++i ) {
		data_string_[ type_indx ][ offset + i ] = data[ i ];
	}
	return;
}


