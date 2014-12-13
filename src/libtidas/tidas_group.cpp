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



tidas::group::group () {
	compute_counts();
	relocate ( loc_ );
	flush();
}


tidas::group::group ( schema const & schm, dict const & d, size_t const & size ) {
	size_ = size;
	dict_ = d;
	schm_ = schm;
	compute_counts();
	relocate ( loc_ );
	flush();
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
	relocate ( loc );
	sync ();
	compute_counts();
}


tidas::group::group ( group const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::group::set_backend ( backend_path const & loc, std::unique_ptr < group_backend > & backend ) {

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

	return;
}


void tidas::group::relocate ( backend_path const & loc ) {

	loc_ = loc;

	// set backend

	set_backend ( loc_, backend_ );

	// relocate dict

	backend_path dictloc = loc_;
	dictloc.meta = backend_->dict_meta();
	dict_.relocate ( dictloc );

	// relocate schema

	backend_path schmloc = loc_;
	schmloc.meta = backend_->schema_meta();
	schm_.relocate ( schmloc );

	return;
}


void tidas::group::sync () const {

	// sync dict

	dict_.sync();

	// sync schema

	schm_.sync();

	// read our own metadata

	backend_->read ( loc_, size_, counts_ );

	return;
}


void tidas::group::flush () {

	if ( loc_.mode == MODE_R ) {
		TIDAS_THROW( "cannot flush to read-only location" );
	}

	// write our own metadata

	backend_->write ( loc_, size_, counts_ );

	// flush schema

	schm_.flush();

	// flush dict

	dict_.flush();

	return;
}


void tidas::group::copy ( group const & other, string const & filter, backend_path const & loc ) {	

	// extract filters

	map < string, string > filts = filter_split ( filter );

	// set backend

	loc_ = loc;
	set_backend ( loc_, backend_ );

	// copy of our metadata

	size_ = other.size_;

	// copy schema first, in order to filter fields

	backend_path schmloc = loc_;
	schmloc.meta = backend_->schema_meta();
	schm_.copy ( other.schm_, filts[ schema_submatch_key ], schmloc );

	if ( ( loc_ != other.loc_ ) || ( loc.type == BACKEND_MEM ) ) {
		if ( loc_.mode == MODE_RW ) {

			// compute counts

			compute_counts();

			// write our metadata

			backend_->write ( loc_, size_, counts_ );

		} else {
			TIDAS_THROW( "cannot copy to new read-only location" );
		}
	}

	// copy dict

	backend_path dictloc = loc_;
	dictloc.meta = backend_->dict_meta();
	dict_.copy ( other.dict_, filts[ dict_submatch_key ], dictloc );

	return;
}


void tidas::group::duplicate ( backend_path const & loc ) {

	if ( loc.type == BACKEND_MEM ) {
		TIDAS_THROW( "calling duplicate() with memory backend makes no sense" );
	}

	if ( loc.mode != MODE_RW ) {
		TIDAS_THROW( "cannot duplicate to read-only location" );
	}

	// write our metadata

	unique_ptr < group_backend > backend;
	set_backend ( loc, backend );

	backend->write ( loc, size_, counts_ );

	// duplicate schema

	backend_path schmloc = loc;
	schmloc.meta = backend->schema_meta();
	schm_.duplicate ( schmloc );

	// duplicate dict

	backend_path dictloc = loc;
	dictloc.meta = backend->dict_meta();
	dict_.duplicate ( dictloc );

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


void tidas::group::read_times ( vector < time_type > & data ) const {
	data.resize ( size_ );
	read_field ( group_time_field, 0, data );
	return;
}


void tidas::group::write_times ( vector < time_type > const & data ) {
	write_field ( group_time_field, 0, data );
	return;
}


tidas::group_backend_mem::group_backend_mem () {
	nsamp_ = 0;
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


void tidas::group_backend_mem::read ( backend_path const & loc, index_type & nsamp, map < data_type, size_t > & counts ) const {
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


string tidas::group_backend_mem::dict_meta () const {
	return string("");
}


string tidas::group_backend_mem::schema_meta () const {
	return string("");
}


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int8_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint8_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int16_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint16_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int32_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint32_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < int64_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < uint64_t > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < float > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < double > & data ) const {
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


void tidas::group_backend_mem::read_field ( backend_path const & loc, string const & field_name, size_t type_indx, index_type offset, vector < string > & data ) const {
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


