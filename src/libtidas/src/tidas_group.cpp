/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
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

        map < data_type, size_t > backend_counts;

        // if we have an index, use it!

        bool found = false;

        if ( loc_.idx ) {
            found = loc_.idx->query_group ( loc_, size_, start_, stop_, backend_counts );
        }

        if ( ! found ) {
            backend_->read ( loc_, size_, start_, stop_, backend_counts );
        }

        for ( auto c : backend_counts ) {
            if ( c.second != counts_[ c.first ] ) {
                ostringstream o;
                o << "group backend counts does not match schema";
                TIDAS_THROW( o.str().c_str() );
            }
        }

        if ( ! found ) {
            if ( loc_.idx && ( loc_.mode == access_mode::write ) ) {
                loc_.idx->add_group ( loc_, size_, start_, stop_, backend_counts );
            }
        }

    }

    return;
}


void tidas::group::flush () const {

    //timer tm;

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {
            // write our own metadata
            //tm.start();
            backend_->write ( loc_, size_, counts_ );
            //tm.stop();
            //tm.report("backend write size and counts");

            //tm.start();
            backend_->update_range ( loc_, start_, stop_ );
            //tm.stop();
            //tm.report("backend write range (start/stop)");

            // update index

            //tm.start();
            if ( loc_.idx ) {
                loc_.idx->add_group ( loc_, size_, start_, stop_, counts_ );
            }
            //tm.stop();
            //tm.report("index add group");
        }

    }

    // flush schema
    //tm.start();
    schm_.flush();
    //tm.stop();
    //tm.report("flush schema");

    // flush dict
    //tm.start();
    dict_.flush();
    //tm.stop();
    //tm.report("flush dict");

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
    start_ = other.start_;
    stop_ = other.stop_;
    compute_counts();

    // copy dict

    backend_path dictloc;
    dict_loc ( dictloc );
    dict_.copy ( other.dict_, filts[ dict_submatch_key ], dictloc );

    // update index

    if ( loc_.idx && ( loc_.mode == access_mode::write ) && ( loc != other.loc_ ) ) {
        loc_.idx->update_group ( loc_, size_, start_, stop_, counts_ );
    }

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

        if ( loc_.mode == access_mode::write ) {
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

        if ( loc_.mode == access_mode::write ) {
            backend_->resize ( loc_, newsize );
            size_ = newsize;

            // update index

            if ( loc_.idx ) {
                loc_.idx->update_group ( loc_, size_, start_, stop_, counts_ );
            }

        } else {
            TIDAS_THROW( "cannot resize group in read-only mode" );
        }
    } else {
        size_ = newsize;
    }

    return;
}


void tidas::group::range ( time_type & start, time_type & stop ) const {
    start = start_;
    stop = stop_;
    return;
}


void tidas::group::update_range () {
    std::vector < time_type > data(1);

    read_field ( group_time_field, 0, data );
    time_type newstart = data[0];

    read_field ( group_time_field, (size_ - 1), data );
    time_type newstop = data[0];

    bool update = false;

    if ( newstart < start_ ) {
        start_ = newstart;
        update = true;
    }

    if ( newstop > stop_ ) {
        stop_ = newstop;
        update = true;
    }

    if ( update ) {
        backend_->update_range ( loc_, start_, stop_ );
        if ( loc_.idx ) {
            loc_.idx->update_group ( loc_, size_, start_, stop_, counts_ );
        }
    }
    return;
}


void tidas::group::read_field_astype ( std::string const & field_name,
    index_type offset, index_type ndata, tidas::data_type type, void * data ) const {
    field check = schm_.field_get ( field_name );
    if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
        std::ostringstream o;
        o << "cannot read non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
        TIDAS_THROW( o.str().c_str() );
    }
    if ( offset + ndata > size_ ) {
        std::ostringstream o;
        o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+ndata-1) << " from group " << loc_.name << " (" << size_ << " samples)";
        TIDAS_THROW( o.str().c_str() );
    }
    if ( loc_.type != backend_type::none ) {
        switch ( type ) {
            case data_type::int8:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int8_t * > (data) );
                break;
            case data_type::uint8:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint8_t * > (data) );
                break;
            case data_type::int16:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int16_t * > (data) );
                break;
            case data_type::uint16:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint16_t * > (data) );
                break;
            case data_type::int32:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int32_t * > (data) );
                break;
            case data_type::uint32:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint32_t * > (data) );
                break;
            case data_type::int64:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int64_t * > (data) );
                break;
            case data_type::uint64:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint64_t * > (data) );
                break;
            case data_type::float32:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < float * > (data) );
                break;
            case data_type::float64:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < double * > (data) );
                break;
            case data_type::string:
                backend_->read_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < char ** > (data) );
                break;
            default:
                TIDAS_THROW( "data type not recognized" );
                break;
        }
    } else {
        TIDAS_THROW( "cannot read field- backend not assigned" );
    }
    return;
}


void tidas::group::write_field_astype ( std::string const & field_name, index_type offset, index_type ndata, tidas::data_type type, void const * data ) {
    field check = schm_.field_get ( field_name );
    if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
        std::ostringstream o;
        o << "cannot write non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
        TIDAS_THROW( o.str().c_str() );
    }
    if ( offset + ndata > size_ ) {
        std::ostringstream o;
        o << "cannot write field " << field_name << ", samples " << offset << " - " << (offset+ndata-1) << " to group " << loc_.name << " (" << size_ << " samples)";
        TIDAS_THROW( o.str().c_str() );
    }
    if ( loc_.type != backend_type::none ) {
        switch ( type ) {
            case data_type::int8:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int8_t const * > (data) );
                break;
            case data_type::uint8:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint8_t const * > (data) );
                break;
            case data_type::int16:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int16_t const * > (data) );
                break;
            case data_type::uint16:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint16_t const * > (data) );
                break;
            case data_type::int32:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int32_t const * > (data) );
                break;
            case data_type::uint32:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint32_t const * > (data) );
                break;
            case data_type::int64:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < int64_t const * > (data) );
                break;
            case data_type::uint64:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < uint64_t const * > (data) );
                break;
            case data_type::float32:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < float const * > (data) );
                break;
            case data_type::float64:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < double const * > (data) );
                break;
            case data_type::string:
                backend_->write_field ( loc_, field_name,
                    type_indx_.at( field_name ), offset, ndata,
                    static_cast < char * const * > (data) );
                break;
            default:
                TIDAS_THROW( "data type not recognized" );
                break;
        }
    } else {
        TIDAS_THROW( "cannot write field- backend not assigned" );
    }
    return;
}


void tidas::group::read_field ( std::string const & field_name, index_type offset, std::vector < std::string > & data ) const {
    // create string buffer for read
    char ** buffer = c_string_alloc ( data.size(), backend_string_size );
    read_field ( field_name, offset, data.size(), buffer );
    for ( size_t i = 0; i < data.size(); ++i ) {
        data[i] = buffer[i];
    }
    c_string_free ( data.size(), buffer );
    return;
}


void tidas::group::write_field ( std::string const & field_name, index_type offset, std::vector < std::string > const & data ) {
    // create and populate string buffer for write
    char ** buffer = c_string_alloc ( data.size(), backend_string_size );
    for ( size_t i = 0; i < data.size(); ++i ) {
        if ( data[i].size() > backend_string_size ) {
            std::ostringstream o;
            o << "cannot write string \"" << data[i] << "\" to field " << field_name << ".  Maximum data string length is " << backend_string_size;
            TIDAS_THROW( o.str().c_str() );
        }
        strncpy ( buffer[i], data[i].c_str(), backend_string_size );
    }
    write_field ( field_name, offset, data.size(), buffer );
    c_string_free ( data.size(), buffer );
    return;
}


void tidas::group::read_times ( index_type ndata, time_type * data,
    index_type offset ) const {
    read_field ( group_time_field, offset, ndata, data );
    return;
}


void tidas::group::read_times ( std::vector < time_type > & data,
    index_type offset ) const {
    read_times ( data.size(), data.data(), offset );
    return;
}


void tidas::group::write_times ( index_type ndata, time_type const * data,
    index_type offset ) {
    write_field ( group_time_field, offset, ndata, data );
    update_range ();
    return;
}


void tidas::group::write_times ( std::vector < time_type > const & data,
    index_type offset ) {
    write_times ( data.size(), data.data(), offset );
    return;
}


void tidas::group::info ( std::ostream & out, size_t indent ) const {
    std::ostringstream ind;
    ind.str("");
    ind << "TIDAS:  ";
    for ( size_t i = 0; i < indent; ++i ) {
        ind << " ";
    }
    tidas::time_type start;
    tidas::time_type stop;
    range(start, stop);
    out << ind.str() << size() << " samples, start = " << start
        << ", stop = " << stop << std::endl;
    // print dictionary values
    out << ind.str() << "Properties:" << std::endl;
    for ( auto const & dit : dictionary().data() ) {
        out << ind.str() << "  " << dit.first << " = " << dit.second
            << std::endl;
    }
    // print schema
    out << ind.str() << "Schema:" << std::endl;
    for ( auto const & sit : schema_get().fields() ) {
        out << ind.str() << "  " << sit.name << ": "
            << tidas::data_type_to_string(sit.type) << ", "
            << sit.units << std::endl;
    }
    return;
}
