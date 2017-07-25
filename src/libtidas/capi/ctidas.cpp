/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <ctidas.hpp>

using namespace std;
using namespace tidas;


// enumerated type conversion


data_type ctidas::convert_from_c ( ctidas_data_type in ) {
    data_type ret;
    switch ( in ) {
        case type_none :
            ret = data_type::none;
            break;
        case type_int8 :
            ret = data_type::int8;
            break;
        case type_int16 :
            ret = data_type::int16;
            break;
        case type_int32 :
            ret = data_type::int32;
            break;
        case type_int64 :
            ret = data_type::int64;
            break;
        case type_uint8 :
            ret = data_type::uint8;
            break;
        case type_uint16 :
            ret = data_type::uint16;
            break;
        case type_uint32 :
            ret = data_type::uint32;
            break;
        case type_uint64 :
            ret = data_type::uint64;
            break;
        case type_float32 :
            ret = data_type::float32;
            break;
        case type_float64 :
            ret = data_type::float64;
            break;
        case type_string :
            ret = data_type::string;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_data_type value" );
            break;
    }
    return ret;
}


ctidas_data_type ctidas::convert_to_c ( data_type in ) {
    ctidas_data_type ret;
    switch ( in ) {
        case data_type::none :
            ret = type_none;
            break;
        case data_type::int8 :
            ret = type_int8;
            break;
        case data_type::int16 :
            ret = type_int16;
            break;
        case data_type::int32 :
            ret = type_int32;
            break;
        case data_type::int64 :
            ret = type_int64;
            break;
        case data_type::uint8 :
            ret = type_uint8;
            break;
        case data_type::uint16 :
            ret = type_uint16;
            break;
        case data_type::uint32 :
            ret = type_uint32;
            break;
        case data_type::uint64 :
            ret = type_uint64;
            break;
        case data_type::float32 :
            ret = type_float32;
            break;
        case data_type::float64 :
            ret = type_float64;
            break;
        case data_type::string :
            ret = type_string;
            break;
        default :
            TIDAS_THROW( "invalid data_type value" );
            break;
    }
    return ret;
}


backend_type ctidas::convert_from_c ( ctidas_backend_type in ) {
    backend_type ret;
    switch ( in ) {
        case back_none :
            ret = backend_type::none;
            break;
        case back_hdf5 :
            ret = backend_type::hdf5;
            break;
        case back_getdata :
            ret = backend_type::getdata;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_backend_type value" );
            break;
    }
    return ret;
}


ctidas_backend_type ctidas::convert_to_c ( backend_type in ) {
    ctidas_backend_type ret;
    switch ( in ) {
        case backend_type::none :
            ret = back_none;
            break;
        case backend_type::hdf5 :
            ret = back_hdf5;
            break;
        case backend_type::getdata :
            ret = back_getdata;
            break;
        default :
            TIDAS_THROW( "invalid backend_type value" );
            break;
    }
    return ret;
}


compression_type ctidas::convert_from_c ( ctidas_compression_type in ) {
    compression_type ret;
    switch ( in ) {
        case comp_none :
            ret = compression_type::none;
            break;
        case comp_gzip :
            ret = compression_type::gzip;
            break;
        case comp_bzip2 :
            ret = compression_type::bzip2;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_compression_type value" );
            break;
    }
    return ret;
}


ctidas_compression_type ctidas::convert_to_c ( compression_type in ) {
    ctidas_compression_type ret;
    switch ( in ) {
        case compression_type::none :
            ret = comp_none;
            break;
        case compression_type::gzip :
            ret = comp_gzip;
            break;
        case compression_type::bzip2 :
            ret = comp_bzip2;
            break;
        default :
            TIDAS_THROW( "invalid compression_type value" );
            break;
    }
    return ret;
}


link_type ctidas::convert_from_c ( ctidas_link_type in ) {
    link_type ret;
    switch ( in ) {
        case link_none :
            ret = link_type::none;
            break;
        case link_hard :
            ret = link_type::hard;
            break;
        case link_soft :
            ret = link_type::soft;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_link_type value" );
            break;
    }
    return ret;
}


ctidas_link_type ctidas::convert_to_c ( link_type in ) {
    ctidas_link_type ret;
    switch ( in ) {
        case link_type::none :
            ret = link_none;
            break;
        case link_type::hard :
            ret = link_hard;
            break;
        case link_type::soft :
            ret = link_soft;
            break;
        default :
            TIDAS_THROW( "invalid link_type value" );
            break;
    }
    return ret;
}


access_mode ctidas::convert_from_c ( ctidas_access_mode in ) {
    access_mode ret;
    switch ( in ) {
        case acc_read :
            ret = access_mode::read;
            break;
        case acc_write :
            ret = access_mode::write;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_access_mode value" );
            break;
    }
    return ret;
}


ctidas_access_mode ctidas::convert_to_c ( access_mode in ) {
    ctidas_access_mode ret;
    switch ( in ) {
        case access_mode::read :
            ret = acc_read;
            break;
        case access_mode::write :
            ret = acc_write;
            break;
        default :
            TIDAS_THROW( "invalid access_mode value" );
            break;
    }
    return ret;
}


exec_order ctidas::convert_from_c ( ctidas_exec_order in ) {
    exec_order ret;
    switch ( in ) {
        case order_depth_first :
            ret = exec_order::depth_first;
            break;
        case order_depth_last :
            ret = exec_order::depth_last;
            break;
        case order_leaf :
            ret = exec_order::leaf;
            break;
        default :
            TIDAS_THROW( "invalid ctidas_exec_order value" );
            break;
    }
    return ret;
}


ctidas_exec_order ctidas::convert_to_c ( exec_order in ) {
    ctidas_exec_order ret;
    switch ( in ) {
        case exec_order::depth_first :
            ret = order_depth_first;
            break;
        case exec_order::depth_last :
            ret = order_depth_last;
            break;
        case exec_order::leaf :
            ret = order_leaf;
            break;
        default :
            TIDAS_THROW( "invalid exec_order value" );
            break;
    }
    return ret;
}


char ** ctidas_string_alloc ( size_t nstring, size_t length ) {
    return c_string_alloc ( nstring, length );
}

void ctidas_string_free ( size_t nstring, char ** str ) {
    c_string_free ( nstring, str );
    return;
}

size_t ctidas_backend_string_size () {
    return backend_string_size;
}

ctidas_dict * ctidas_dict_alloc ( ) {
    return reinterpret_cast < ctidas_dict * > ( new dict() );
}

void ctidas_dict_free ( ctidas_dict * dct ) {
    delete reinterpret_cast < dict * > ( dct );
    return;
}

void ctidas_dict_clear ( ctidas_dict * dct ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->clear();
    return;
}

size_t ctidas_dict_nkeys ( ctidas_dict const * dct ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->data().size();
}

char ** ctidas_dict_keys ( ctidas_dict const * dct, size_t * nkeys ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    (*nkeys) = d->data().size();
    char ** ret = (char**)malloc((*nkeys) * sizeof(char*));
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate vector of dict keys\n");
        exit(1);
    }
    size_t cur = 0;
    for ( auto k : d->data() ) {
        ret[cur] = (char*)malloc(k.first.size() + 1);
        if ( ! ret[cur] ) {
            fprintf(stderr, "failed to allocate dict key %d\n", (int)cur);
            exit(1);
        }
        strncpy(ret[cur], k.first.c_str(), k.first.size());
        ret[cur][k.first.size()] = '\0';
        ++cur;
    }
    return ret;
}

ctidas_data_type ctidas_dict_type ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return ctidas::convert_to_c( d->types().at(string(key)) );
}

void ctidas_dict_put_int8 ( ctidas_dict * dct, char const * key, int8_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_int16 ( ctidas_dict * dct, char const * key, int16_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_int32 ( ctidas_dict * dct, char const * key, int32_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_int64 ( ctidas_dict * dct, char const * key, int64_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_uint8 ( ctidas_dict * dct, char const * key, uint8_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_uint16 ( ctidas_dict * dct, char const * key, uint16_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_uint32 ( ctidas_dict * dct, char const * key, uint32_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_uint64 ( ctidas_dict * dct, char const * key, uint64_t val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_float ( ctidas_dict * dct, char const * key, float val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_double ( ctidas_dict * dct, char const * key, double val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

void ctidas_dict_put_string ( ctidas_dict * dct, char const * key, char const * val ) {
    dict * d = reinterpret_cast < dict * > ( dct );
    d->put ( key, val );
    return;
}

int8_t ctidas_dict_get_int8 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < int8_t > ( key );
}

int16_t ctidas_dict_get_int16 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < int16_t > ( key );
}

int32_t ctidas_dict_get_int32 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < int32_t > ( key );
}

int64_t ctidas_dict_get_int64 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < int64_t > ( key );
}

uint8_t ctidas_dict_get_uint8 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < uint8_t > ( key );
}

uint16_t ctidas_dict_get_uint16 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < uint16_t > ( key );
}

uint32_t ctidas_dict_get_uint32 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < uint32_t > ( key );
}

uint64_t ctidas_dict_get_uint64 ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < uint64_t > ( key );
}

float ctidas_dict_get_float ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < float > ( key );
}

double ctidas_dict_get_double ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    return d->get < double > ( key );
}

char * ctidas_dict_get_string ( ctidas_dict const * dct, char const * key ) {
    dict const * d = reinterpret_cast < dict const * > ( dct );
    string str = d->get < string > ( key );
    char * ret = (char*)malloc(str.size() + 1);
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate dict string value return\n");
        exit(1);
    }
    strncpy( ret, str.c_str(), str.size() );
    ret[str.size()] = '\0';
    return ret;
}


ctidas_field * ctidas_field_alloc ( ) {
    return reinterpret_cast < ctidas_field * > ( new field() );
}

void ctidas_field_free ( ctidas_field * fld ) {
    delete reinterpret_cast < field * > ( fld );
    return;
}

void ctidas_field_type_set ( ctidas_field * fld, ctidas_data_type type ) {
    field * f = reinterpret_cast < field * > ( fld );
    f->type = ctidas::convert_from_c( type );
    return;
}

ctidas_data_type ctidas_field_type_get ( ctidas_field const * fld ) {
    field const * f = reinterpret_cast < field const * > ( fld );
    return ctidas::convert_to_c( f->type );
}

void ctidas_field_name_set ( ctidas_field * fld, char const * name ) {
    field * f = reinterpret_cast < field * > ( fld );
    f->name = string(name);
    return;
}

char const * ctidas_field_name_get ( ctidas_field const * fld ) {
    field const * f = reinterpret_cast < field const * > ( fld );
    return f->name.c_str();
}

void ctidas_field_units_set ( ctidas_field * fld, char const * units ) {
    field * f = reinterpret_cast < field * > ( fld );
    f->units = string(units);
    return;
}

char const * ctidas_field_units_get ( ctidas_field const * fld ) {
    field const * f = reinterpret_cast < field const * > ( fld );
    return f->units.c_str();
}


ctidas_schema * ctidas_schema_alloc ( ) {
    return reinterpret_cast < ctidas_schema * > ( new schema() );
}

void ctidas_schema_free ( ctidas_schema * schm ) {
    delete reinterpret_cast < schema * > ( schm );
    return;
}

void ctidas_schema_clear ( ctidas_schema * schm ) {
    schema * s = reinterpret_cast < schema * > ( schm );
    s->clear();
}

char ** ctidas_schema_fields ( ctidas_schema const * schm, size_t * nfields ) {
    schema const * s = reinterpret_cast < schema const * > ( schm );
    (*nfields) = s->fields().size();
    char ** ret = (char**)malloc((*nfields) * sizeof(char*));
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate vector of schema fields\n");
        exit(1);
    }
    size_t cur = 0;
    for ( auto const & f : s->fields() ) {
        ret[cur] = (char*)malloc(f.name.size() + 1);
        if ( ! ret[cur] ) {
            fprintf(stderr, "failed to allocate schema field %d\n", (int)cur);
            exit(1);
        }
        strncpy(ret[cur], f.name.c_str(), f.name.size());
        ret[cur][f.name.size()] = '\0';
        ++cur;
    }
    return ret;
}

void ctidas_schema_field_add ( ctidas_schema * schm, ctidas_field const * fld ) {
    schema * s = reinterpret_cast < schema * > ( schm );
    field const * f = reinterpret_cast < field const * > ( fld );
    s->field_add( *f );
    return;
}

void ctidas_schema_field_del ( ctidas_schema * schm, char const * name ) {
    schema * s = reinterpret_cast < schema * > ( schm );
    s->field_del( name );
    return;
}

ctidas_field * ctidas_schema_field_get ( ctidas_schema const * schm, char const * name ) {
    schema const * s = reinterpret_cast < schema const * > ( schm );
    ctidas_field * ret = ctidas_field_alloc();
    field * f = reinterpret_cast < field * > ( ret );
    (*f) = s->field_get( name );
    return ret;
}


ctidas_intrvl * ctidas_intrvl_alloc ( ) {
    return reinterpret_cast < ctidas_intrvl * > ( new intrvl() );
}

void ctidas_intrvl_free ( ctidas_intrvl * intr ) {
    delete reinterpret_cast < intrvl * > ( intr );
    return;
}

void ctidas_intrvl_start_set ( ctidas_intrvl * intr, double start ) {
    intrvl * t = reinterpret_cast < intrvl * > ( intr );
    t->start = start;
    return;
}

double ctidas_intrvl_start_get ( ctidas_intrvl const * intr ) {
    intrvl const * t = reinterpret_cast < intrvl const * > ( intr );
    return t->start;
}

void ctidas_intrvl_stop_set ( ctidas_intrvl * intr, double stop ) {
    intrvl * t = reinterpret_cast < intrvl * > ( intr );
    t->stop = stop;
    return;    
}

double ctidas_intrvl_stop_get ( ctidas_intrvl const * intr ) {
    intrvl const * t = reinterpret_cast < intrvl const * > ( intr );
    return t->stop;
}

void ctidas_intrvl_first_set ( ctidas_intrvl * intr, int64_t first ) {
    intrvl * t = reinterpret_cast < intrvl * > ( intr );
    t->first = first;
    return;
}

int64_t ctidas_intrvl_first_get ( ctidas_intrvl const * intr ) {
    intrvl const * t = reinterpret_cast < intrvl const * > ( intr );
    return t->first;
}

void ctidas_intrvl_last_set ( ctidas_intrvl * intr, int64_t last ) {
    intrvl * t = reinterpret_cast < intrvl * > ( intr );
    t->last = last;
    return;
}

int64_t ctidas_intrvl_last_get ( ctidas_intrvl const * intr ) {
    intrvl const * t = reinterpret_cast < intrvl const * > ( intr );
    return t->last;
}


ctidas_intrvl ** ctidas_intrvl_list_alloc ( size_t n ) {
    ctidas_intrvl ** ret = (ctidas_intrvl **)malloc( n * sizeof(ctidas_intrvl *));
    for ( size_t i = 0; i < n; ++i ) {
        ret[i] = ctidas_intrvl_alloc();
    }
    return ret;
}

void ctidas_intrvl_list_free ( ctidas_intrvl ** intrl, size_t n ) {
    for ( size_t i = 0; i < n; ++i ) {
        ctidas_intrvl_free( intrl[i] );
    }
    free(intrl);
    return;
}


ctidas_intervals * ctidas_intervals_alloc ( ctidas_dict const * dct, size_t size ) {
    ctidas_intervals * ret;
    if ( dct == NULL ) {
        dict empty;
        ret = reinterpret_cast < ctidas_intervals * > ( new intervals( empty, size ) );
    } else {
        dict const * d = reinterpret_cast < dict const * > ( dct );
        ret = reinterpret_cast < ctidas_intervals * > ( new intervals( (*d), size ) );
    }
    return ret;
}

void ctidas_intervals_free ( ctidas_intervals * inv ) {
    delete reinterpret_cast < intervals * > ( inv );
    return;
}

size_t ctidas_intervals_size ( ctidas_intervals const * inv ) {
    intervals const * v = reinterpret_cast < intervals const * > ( inv );
    return v->size();
}

ctidas_dict const * ctidas_intervals_dict ( ctidas_intervals const * inv ) {
    intervals const * v = reinterpret_cast < intervals const * > ( inv );
    dict const & d = v->dictionary();
    return reinterpret_cast < ctidas_dict const * > ( &d );
}

ctidas_intrvl ** ctidas_intervals_read ( ctidas_intervals const * inv, size_t * n ) {
    interval_list il;
    intervals const * v = reinterpret_cast < intervals const * > ( inv );
    v->read_data( il );
    (*n) = il.size();

    ctidas_intrvl ** ret = ctidas_intrvl_list_alloc ( *n );
    
    for ( size_t i = 0; i < (*n); ++i ) {
        intrvl * t = reinterpret_cast < intrvl * > ( ret[i] );
        (*t) = il[i];
    }

    return ret;
}

void ctidas_intervals_write ( ctidas_intervals * inv, ctidas_intrvl * const * intrl, size_t n ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    intervals * v = reinterpret_cast < intervals * > ( inv );
    v->write_data( il );

    return;
}

ctidas_index_type ctidas_intervals_samples ( ctidas_intrvl * const * intrl, size_t n ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    return intervals::total_samples( il );
}

ctidas_time_type ctidas_intervals_time ( ctidas_intrvl * const * intrl, size_t n ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    return intervals::total_time( il );
}

ctidas_intrvl * ctidas_intervals_seek ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    intrvl t = intervals::seek ( il, time );

    ctidas_intrvl * cret = ctidas_intrvl_alloc();
    intrvl * ret = reinterpret_cast < intrvl * > ( cret );

    (*ret) = t;

    return cret;
}

ctidas_intrvl * ctidas_intervals_seek_ceil ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    intrvl t = intervals::seek_ceil ( il, time );

    ctidas_intrvl * cret = ctidas_intrvl_alloc();
    intrvl * ret = reinterpret_cast < intrvl * > ( cret );

    (*ret) = t;

    return cret;
}

ctidas_intrvl * ctidas_intervals_seek_floor ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time ) {
    interval_list il;

    for ( size_t i = 0; i < n; ++i ) {
        intrvl const * t = reinterpret_cast < intrvl const * > ( intrl[i] );
        il.push_back( (*t) );
    }

    intrvl t = intervals::seek_floor ( il, time );

    ctidas_intrvl * cret = ctidas_intrvl_alloc();
    intrvl * ret = reinterpret_cast < intrvl * > ( cret );

    (*ret) = t;

    return cret;
}


ctidas_group * ctidas_group_alloc ( ctidas_schema const * schm, ctidas_dict const * dct, ctidas_index_type size ) {
    ctidas_group * ret;
    if ( schm == NULL ) {
        fprintf(stderr, "You must provide a schema when creating a group\n");
        exit(1);
    }
    schema const * s = reinterpret_cast < schema const * > ( schm );
    if ( dct == NULL ) {
        dict empty;
        ret = reinterpret_cast < ctidas_group * > ( new group( (*s), empty, size ) );
    } else {
        dict const * d = reinterpret_cast < dict const * > ( dct );
        ret = reinterpret_cast < ctidas_group * > ( new group( (*s), (*d), size ) );
    }
    return ret;
}

void ctidas_group_free ( ctidas_group * grp ) {
    delete reinterpret_cast < group * > ( grp );
    return;
}

ctidas_dict const * ctidas_group_dict ( ctidas_group const * grp ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    dict const & d = g->dictionary();
    return reinterpret_cast < ctidas_dict const * > ( &d );
}

ctidas_schema const * ctidas_group_schema ( ctidas_group const * grp ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    schema const & s = g->schema_get();
    return reinterpret_cast < ctidas_schema const * > ( &s );
}

ctidas_index_type ctidas_group_size ( ctidas_group const * grp ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    return g->size();
}

void ctidas_group_resize ( ctidas_group * grp, ctidas_index_type newsize ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->resize( newsize );
    return;
}

void ctidas_group_range ( ctidas_group const * grp, ctidas_time_type * start, ctidas_time_type * stop ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    time_type gstart;
    time_type gstop;
    g->range( gstart, gstop );
    (*start) = gstart;
    (*stop) = gstop;
    return;
}

void ctidas_group_read_times ( ctidas_group const * grp, ctidas_index_type ndata, ctidas_time_type * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_times( ndata, data );
    return;
}

void ctidas_group_write_times ( ctidas_group * grp, ctidas_index_type ndata, ctidas_time_type const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_times( ndata, data );
    return;
}

void ctidas_group_read_int8 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int8_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_int8 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int8_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_int16 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int16_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_int16 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int16_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_int32 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int32_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_int32 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int32_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_int64 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int64_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_int64 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int64_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_uint8 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint8_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_uint8 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint8_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_uint16 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint16_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_uint16 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint16_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_uint32 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint32_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_uint32 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint32_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_uint64 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint64_t * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_uint64 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint64_t const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_float ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, float * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_float ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, float const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_double ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, double * data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_double ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, double const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_read_string ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, char ** data ) {
    group const * g = reinterpret_cast < group const * > ( grp );
    g->read_field ( string(field), offset, ndata, data );
    return;
}

void ctidas_group_write_string ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, char * const * data ) {
    group * g = reinterpret_cast < group * > ( grp );
    g->write_field ( string(field), offset, ndata, data );
    return;
}


ctidas_block * ctidas_block_alloc() {
    return reinterpret_cast < ctidas_block * > ( new block() );
}

void ctidas_block_free ( ctidas_block * blk ) {
    delete reinterpret_cast < block * > ( blk );
    return;
}

char * ctidas_block_aux_dir ( ctidas_block * blk ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    string dir = b->aux_dir();
    char * ret = (char*)malloc(dir.size() + 1);
    strncpy(ret, dir.c_str(), dir.size());
    ret[dir.size()] = '\0';
    return ret;
}

void ctidas_block_range ( ctidas_block const * blk, ctidas_time_type * start, ctidas_time_type * stop ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    time_type bstart;
    time_type bstop;
    b->range( bstart, bstop );
    (*start) = bstart;
    (*stop) = bstop;
    return;
}

void ctidas_block_clear ( ctidas_block * blk ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->clear();
    return;
}


ctidas_group * ctidas_block_group_add ( ctidas_block * blk, char const * name, ctidas_group * grp ) {
    block * b = reinterpret_cast < block * > ( blk );
    if ( grp == NULL ) {
        fprintf(stderr, "Cannot add a NULL group pointer to block\n");
        exit(1);
    }
    group * g = reinterpret_cast < group * > ( grp );
    group & gref = b->group_add ( string(name), (*g) );
    return reinterpret_cast < ctidas_group * > ( new group( gref ) );
}

ctidas_group * ctidas_block_group_get ( ctidas_block const * blk, char const * name ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    group const & gref = b->group_get ( string(name) );
    ctidas_group * ret = reinterpret_cast < ctidas_group * > ( new group( gref ) );
    return ret;
}

void ctidas_block_group_del ( ctidas_block * blk, char const * name ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->group_del ( string(name) );
    return;
}

char ** ctidas_block_all_groups ( ctidas_block const * blk, size_t * ngroup ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    vector < string > grps = b->all_groups();
    (*ngroup) = grps.size();
    char ** ret = (char**)malloc((*ngroup) * sizeof(char*));
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate vector of group names\n");
        exit(1);
    }
    size_t cur = 0;
    for ( auto const & g : grps ) {
        ret[cur] = (char*)malloc(g.size() + 1);
        if ( ! ret[cur] ) {
            fprintf(stderr, "failed to allocate group name %d\n", (int)cur);
            exit(1);
        }
        strncpy(ret[cur], g.c_str(), g.size());
        ret[cur][g.size()] = '\0';
        ++cur;
    }
    return ret;
}

void ctidas_block_clear_groups ( ctidas_block * blk ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->clear_groups();
    return;
}


ctidas_intervals * ctidas_block_intervals_add ( ctidas_block * blk, char const * name, ctidas_intervals * inv ) {
    block * b = reinterpret_cast < block * > ( blk );
    if ( inv == NULL ) {
        fprintf(stderr, "Cannot add a NULL interval pointer to block\n");
        exit(1);
    }
    intervals * t = reinterpret_cast < intervals * > ( inv );
    intervals & iref = b->intervals_add ( string(name), (*t) );
    return reinterpret_cast < ctidas_intervals * > ( new intervals ( iref ) );
}

ctidas_intervals * ctidas_block_intervals_get ( ctidas_block const * blk, char const * name ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    intervals const & iref = b->intervals_get ( string(name) );
    ctidas_intervals * ret = reinterpret_cast < ctidas_intervals * > ( new intervals( iref ) );
    return ret;
}

void ctidas_block_intervals_del ( ctidas_block * blk, char const * name ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->intervals_del ( string(name) );
    return;
}

char ** ctidas_block_all_intervals ( ctidas_block const * blk, size_t * nintervals ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    vector < string > intr = b->all_intervals();
    (*nintervals) = intr.size();
    char ** ret = (char**)malloc((*nintervals) * sizeof(char*));
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate vector of intervals names\n");
        exit(1);
    }
    size_t cur = 0;
    for ( auto const & t : intr ) {
        ret[cur] = (char*)malloc(t.size() + 1);
        if ( ! ret[cur] ) {
            fprintf(stderr, "failed to allocate intervals name %d\n", (int)cur);
            exit(1);
        }
        strncpy(ret[cur], t.c_str(), t.size());
        ret[cur][t.size()] = '\0';
        ++cur;
    }
    return ret;
}

void ctidas_block_clear_intervals ( ctidas_block * blk ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->clear_intervals();
    return;
}


ctidas_block * ctidas_block_child_add ( ctidas_block * blk, char const * name, ctidas_block * child ) {
    block * b = reinterpret_cast < block * > ( blk );
    if ( child == NULL ) {
        fprintf(stderr, "Cannot add a NULL block pointer to block\n");
        exit(1);
    }
    block * c = reinterpret_cast < block * > ( child );
    block & bref = b->block_add ( string(name), (*c) );
    return reinterpret_cast < ctidas_block * > ( new block ( bref ) );
}

ctidas_block * ctidas_block_child_get ( ctidas_block const * blk, char const * name ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    block const & bref = b->block_get ( string(name) );
    ctidas_block * ret = reinterpret_cast < ctidas_block * > ( new block( bref ) );
    return ret;
}

void ctidas_block_child_del ( ctidas_block * blk, char const * name ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->block_del ( string(name) );
    return;
}

char ** ctidas_block_all_children ( ctidas_block const * blk, size_t * nchild ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    vector < string > chld = b->all_blocks();
    (*nchild) = chld.size();
    char ** ret = (char**)malloc((*nchild) * sizeof(char*));
    if ( ! ret ) {
        fprintf(stderr, "failed to allocate vector of child block names\n");
        exit(1);
    }
    size_t cur = 0;
    for ( auto const & c : chld ) {
        ret[cur] = (char*)malloc(c.size() + 1);
        if ( ! ret[cur] ) {
            fprintf(stderr, "failed to allocate child block name %d\n", (int)cur);
            exit(1);
        }
        strncpy(ret[cur], c.c_str(), c.size());
        ret[cur][c.size()] = '\0';
        ++cur;
    }
    return ret;
}

void ctidas_block_clear_children ( ctidas_block * blk ) {
    block * b = reinterpret_cast < block * > ( blk );
    b->clear_blocks();
    return;
}


ctidas_block * ctidas_block_select ( ctidas_block const * blk, char const * filter ) {
    block const * b = reinterpret_cast < block const * > ( blk );    
    block result = b->select( string(filter) );
    ctidas_block * ret = reinterpret_cast < ctidas_block * > ( new block( result ) );
    return ret;
}


void ctidas::block_operator::operator() ( block const & blk ) {
    ctidas_block const * b = reinterpret_cast < ctidas_block const * > ( &blk );
    op ( b, aux );
    return;
}

void ctidas_block_exec ( ctidas_block const * blk, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux ) {
    block const * b = reinterpret_cast < block const * > ( blk );
    ctidas::block_operator opcpp;
    opcpp.op = op;
    opcpp.aux = aux;
    b->exec ( opcpp, ctidas::convert_from_c(order) );
    return;
}


ctidas_volume * ctidas_volume_create ( char const * path, ctidas_backend_type type, ctidas_compression_type comp ) {
    return reinterpret_cast < ctidas_volume * > ( new volume( string(path), ctidas::convert_from_c(type), ctidas::convert_from_c(comp) ) );
}

ctidas_volume * ctidas_volume_open ( char const * path, ctidas_access_mode mode ) {
    return reinterpret_cast < ctidas_volume * > ( new volume( string(path), ctidas::convert_from_c(mode) ) );
}

void ctidas_volume_close ( ctidas_volume * vol ) {
    delete reinterpret_cast < volume * > ( vol );
    return;
}

ctidas_block * ctidas_volume_root ( ctidas_volume * vol ) {
    volume * v = reinterpret_cast < volume * > ( vol );
    block & b = v->root();
    // note this DOES NOT allocate a new block, but rather returns
    // a pointer to the fixed root block in the volume.
    ctidas_block * ret = reinterpret_cast < ctidas_block * > ( &b );
    return ret;
}

void ctidas_volume_exec ( ctidas_volume * vol, ctidas_exec_order order, char const * filter, CTIDAS_EXEC_OP op, void * aux ) {
    volume * v = reinterpret_cast < volume * > ( vol );
    ctidas::block_operator opcpp;
    opcpp.op = op;
    opcpp.aux = aux;
    v->exec ( opcpp, ctidas::convert_from_c(order), string(filter) );
    return;
}


void ctidas_data_intervals_copy ( ctidas_intervals const * in, ctidas_intervals * out ) {
    intervals const * tin = reinterpret_cast < intervals const * > ( in );
    intervals * tout = reinterpret_cast < intervals * > ( out );
    data_copy ( (*tin), (*tout) );
    return;
}

void ctidas_data_group_copy ( ctidas_group const * in, ctidas_group * out ) {
    group const * gin = reinterpret_cast < group const * > ( in );
    group * gout = reinterpret_cast < group * > ( out );
    data_copy ( (*gin), (*gout) );
    return;
}

void ctidas_data_block_copy ( ctidas_block const * in, ctidas_block * out ) {
    block const * bin = reinterpret_cast < block const * > ( in );
    block * bout = reinterpret_cast < block * > ( out );
    data_copy ( (*bin), (*bout) );
    return;
}

void ctidas_data_volume_copy ( ctidas_volume const * in, ctidas_volume * out ) {
    volume const * vin = reinterpret_cast < volume const * > ( in );
    volume * vout = reinterpret_cast < volume * > ( out );
    data_copy ( (*vin), (*vout) );
    return;
}




