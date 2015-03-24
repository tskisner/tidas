/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <limits>


using namespace std;
using namespace tidas;



tidas::group::group () {
	size_ = 0;

	compute_counts();
	relocate ( loc_ );
}


tidas::group::group ( schema const & schm, dict const & d, size_t const & size ) {
	size_ = size;
	dict_ = d;
	schm_ = schm;

	start_ = numeric_limits < time_type > :: max();
	stop_ = numeric_limits < time_type > :: min();

	// we ensure that the schema always includes the correct time field

	field time;
	time.name = group_time_field;
	time.type = data_type::float64;
	time.units = "seconds";

	schm_.field_del( time.name );
	schm_.field_add( time );

	compute_counts();
	relocate ( loc_ );
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
	sync();
	compute_counts();
}


tidas::group::group ( group const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::group::set_backend ( ) {

	switch ( loc_.type ) {
		case backend_type::none:
			backend_.reset();
			break;
		case backend_type::hdf5:
			backend_.reset( new group_backend_hdf5 () );
			break;
		case backend_type::getdata:
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
	set_backend();

	// relocate dict

	backend_path dictloc;
	dict_loc ( dictloc );
	dict_.relocate ( dictloc );

	// relocate schema

	backend_path schmloc;
	schema_loc ( schmloc );
	schm_.relocate ( schmloc );

	return;
}


void tidas::group::sync () {

	// sync dict

	dict_.sync();

	// sync schema

	schm_.sync();

	compute_counts();

	// read our own metadata

	if ( loc_.type != backend_type::none ) {

		// if we have an index, use it!

		if ( loc_.idx ) {



		} else {
			map < data_type, size_t > backend_counts;
			backend_->read ( loc_, size_, start_, stop_, backend_counts );

			for ( auto c : backend_counts ) {
				if ( c.second != counts_[ c.first ] ) {
					ostringstream o;
					o << "group backend counts does not match schema";
					TIDAS_THROW( o.str().c_str() );
				}
			}
		}

	}

	return;
}


void tidas::group::flush () const {

	if ( loc_.type != backend_type::none ) {

		if ( loc_.mode == access_mode::readwrite ) {	
			// write our own metadata

			backend_->write ( loc_, size_, counts_ );
			backend_->update_range ( loc_, start_, stop_ );

			// update index

			if ( loc_.idx ) {



			}
		}

	}

	// flush schema

	schm_.flush();

	// flush dict

	dict_.flush();

	return;
}


void tidas::group::copy ( group const & other, string const & filter, backend_path const & loc ) {	

	string filt = filter_default ( filter );

	if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
		TIDAS_THROW( "copy of non-memory group with a filter requires a new location" );
	}

	// extract filters

	map < string, string > filts = filter_split ( filter );

	// set backend

	loc_ = loc;
	set_backend();

	// copy schema first, in order to filter fields

	backend_path schmloc;
	schema_loc ( schmloc );
	schm_.copy ( other.schm_, filts[ schema_submatch_key ], schmloc );

	// copy our metadata

	size_ = other.size_;
	compute_counts();

	// copy dict

	backend_path dictloc;
	dict_loc ( dictloc );
	dict_.copy ( other.dict_, filts[ dict_submatch_key ], dictloc );

	return;
}


void tidas::group::link ( link_type const & type, string const & path ) const {

	if ( type != link_type::none ) {

		if ( loc_.type != backend_type::none ) {
			backend_->link ( loc_, type, path );
		}

	}

	return;
}


void tidas::group::wipe () const {

	if ( loc_.type != backend_type::none ) {

		if ( loc_.mode == access_mode::readwrite ) {
			backend_->wipe ( loc_ );
		} else {
			TIDAS_THROW( "cannot wipe group in read-only mode" );
		}
	}

	return;
}


backend_path tidas::group::location () const {
	return loc_;
}


void tidas::group::compute_counts() {

	// clear counts
	counts_.clear();
	counts_[ data_type::int8 ] = 0;
	counts_[ data_type::uint8 ] = 0;
	counts_[ data_type::int16 ] = 0;
	counts_[ data_type::uint16 ] = 0;
	counts_[ data_type::int32 ] = 0;
	counts_[ data_type::uint32 ] = 0;
	counts_[ data_type::int64 ] = 0;
	counts_[ data_type::uint64 ] = 0;
	counts_[ data_type::float32 ] = 0;
	counts_[ data_type::float64 ] = 0;
	counts_[ data_type::string ] = 0;

	// clear type_indx
	type_indx_.clear();

	// we always have a time field
	type_indx_[ group_time_field ] = counts_[ data_type::float64 ];
	++counts_[ data_type::float64 ];

	field_list fields = schm_.fields();

	for ( auto fld : fields ) {
		if ( fld.name == group_time_field ) {
			// ignore time field, since we already counted it 
		} else {
			if ( fld.type == data_type::none ) {
				ostringstream o;
				o << "group schema field \"" << fld.name << "\" has type == none";
				TIDAS_THROW( o.str().c_str() );
			}
			type_indx_[ fld.name ] = counts_[ fld.type ];
			++counts_[ fld.type ];
		}
	}

	return;
}


void tidas::group::dict_loc ( backend_path & dloc ) {
	dloc = loc_;
	if ( loc_.type != backend_type::none ) {
		dloc.meta = backend_->dict_meta();
	}
	return;
}


void tidas::group::schema_loc ( backend_path & sloc ) {
	sloc = loc_;
	if ( loc_.type != backend_type::none ) {
		sloc.meta = backend_->schema_meta();
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


void tidas::group::resize ( index_type const & newsize ) {

	if ( loc_.type != backend_type::none ) {

		if ( loc_.mode == access_mode::readwrite ) {
			backend_->resize ( loc_, newsize );
			size_ = newsize;

			// update index


			
		} else {
			TIDAS_THROW( "cannot resize group in read-only mode" );
		}
	}

	return;
}


void tidas::group::range ( time_type & start, time_type & stop ) const {
	start = start_;
	stop = stop_;
	return;
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


void tidas::group::write_field ( std::string const & field_name, index_type offset, std::vector < time_type > const & data ) {
	field check = schm_.field_get ( field_name );
	if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
		std::ostringstream o;
		o << "cannot write non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
		TIDAS_THROW( o.str().c_str() );
	}
	index_type n = data.size();
	if ( offset + n > size_ ) {
		std::ostringstream o;
		o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " to group " << loc_.name << " (" << size_ << " samples)";
		TIDAS_THROW( o.str().c_str() );
	}
	if ( loc_.type != backend_type::none ) {
		backend_->write_field ( loc_, field_name, type_indx_.at( field_name ), offset, data );

		// if we are writing to the time field, update group range

		if ( field_name == group_time_field ) {
			bool update = false;

			if ( data[0] < start_ ) {
				start_ = data[0];
				update = true;
			}

			if ( data[ data.size() - 1 ] > stop_ ) {
				stop_ = data[ data.size() - 1 ];
				update = true;
			}
			
			if ( update ) {
				backend_->update_range ( loc_, start_, stop_ );

				if ( loc_.idx ) {
					// update index


				}
			}
		}

	} else {
		TIDAS_THROW( "cannot write field- backend not assigned" );
	}
	return;
}


