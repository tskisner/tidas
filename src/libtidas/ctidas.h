/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef CTOAST_H
#define CTOAST_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif


typedef double ctidas_time_type;

typedef int64_t ctidas_index_type;


typedef enum {
	type_none,
	type_int8,
	type_uint8,
	type_int16,
	type_uint16,
	type_int32,
	type_uint32,
	type_int64,
	type_uint64,
	type_float32,
	type_float64,
	type_string
} ctidas_data_type;


typedef enum {
    back_none,
    back_hdf5,
    back_getdata
} ctidas_backend_type;


typedef enum {
    comp_none,
    comp_gzip,
    comp_bzip2
} ctidas_compression_type;


typedef enum {
    acc_read,
    acc_readwrite
} ctidas_access_mode;


typedef enum {
    link_none,
    link_hard,
    link_soft
} ctidas_link_type;


/* Dictionary */

struct ctidas_dict_;
typedef struct ctidas_dict_ ctidas_dict;

ctidas_dict * ctidas_dict_alloc ( );

void ctidas_dict_free ( ctidas_dict * dct );

void ctidas_dict_clear ( ctidas_dict * dct );

size_t ctidas_dict_nkeys ( ctidas_dict * dct );

char ** ctidas_dict_keys ( ctidas_dict * dct, size_t * nkeys );

ctidas_data_type ctidas_dict_type ( ctidas_dict * dct, char const * key );

void ctidas_dict_put_int8 ( ctidas_dict * dct, char const * key, int8_t val );

void ctidas_dict_put_int16 ( ctidas_dict * dct, char const * key, int16_t val );

void ctidas_dict_put_int32 ( ctidas_dict * dct, char const * key, int32_t val );

void ctidas_dict_put_int64 ( ctidas_dict * dct, char const * key, int64_t val );

void ctidas_dict_put_uint8 ( ctidas_dict * dct, char const * key, uint8_t val );

void ctidas_dict_put_uint16 ( ctidas_dict * dct, char const * key, uint16_t val );

void ctidas_dict_put_uint32 ( ctidas_dict * dct, char const * key, uint32_t val );

void ctidas_dict_put_uint64 ( ctidas_dict * dct, char const * key, uint64_t val );

void ctidas_dict_put_float ( ctidas_dict * dct, char const * key, float val );

void ctidas_dict_put_double ( ctidas_dict * dct, char const * key, double val );

void ctidas_dict_put_string ( ctidas_dict * dct, char const * key, char const * val );

int8_t ctidas_dict_get_int8 ( ctidas_dict * dct, char const * key );

int16_t ctidas_dict_get_int16 ( ctidas_dict * dct, char const * key );

int32_t ctidas_dict_get_int32 ( ctidas_dict * dct, char const * key );

int64_t ctidas_dict_get_int64 ( ctidas_dict * dct, char const * key );

uint8_t ctidas_dict_get_uint8 ( ctidas_dict * dct, char const * key );

uint16_t ctidas_dict_get_uint16 ( ctidas_dict * dct, char const * key );

uint32_t ctidas_dict_get_uint32 ( ctidas_dict * dct, char const * key );

uint64_t ctidas_dict_get_uint64 ( ctidas_dict * dct, char const * key );

float ctidas_dict_get_float ( ctidas_dict * dct, char const * key );

double ctidas_dict_get_double ( ctidas_dict * dct, char const * key );

char * ctidas_dict_get_string ( ctidas_dict * dct, char const * key );


/* Schema Field */


struct ctidas_field_;
typedef struct ctidas_field_ ctidas_field;

ctidas_field * ctidas_field_alloc ( );

void ctidas_field_free ( ctidas_field * fld );

void ctidas_field_type_set ( ctidas_field * fld, ctidas_data_type type );

ctidas_data_type ctidas_field_type_get ( ctidas_field * fld );

void ctidas_field_name_set ( ctidas_field * fld, char const * name );

char * ctidas_field_name_get ( ctidas_field * fld );

void ctidas_field_units_set ( ctidas_field * fld, char const * units );

char * ctidas_field_units_get ( ctidas_field * fld );


/* Schema */


struct ctidas_schema_;
typedef struct ctidas_schema_ ctidas_schema;

ctidas_schema * ctidas_schema_alloc ( );

void ctidas_schema_free ( ctidas_schema * schm );

void ctidas_schema_clear ( ctidas_schema * schm );

char ** ctidas_schema_fields ( ctidas_schema * schm, size_t * nfields );

void ctidas_schema_field_add ( ctidas_schema * schm, ctidas_field const * fld );

void ctidas_schema_field_del ( ctidas_schema * schm, char const * name );

ctidas_field * ctidas_schema_field_get ( ctidas_schema * schm, char const * name );


/* Single interval */


struct ctidas_intrvl_;
typedef struct ctidas_intrvl_ ctidas_intrvl;

ctidas_intrvl * ctidas_intrvl_alloc ( );

void ctidas_intrvl_free ( ctidas_intrvl * intr );

void ctidas_intrvl_start_set ( ctidas_intrvl * intr, double start );

double ctidas_intrvl_start_get ( ctidas_intrvl * intr );

void ctidas_intrvl_stop_set ( ctidas_intrvl * intr, double stop );

double ctidas_intrvl_stop_get ( ctidas_intrvl * intr );

void ctidas_intrvl_first_set ( ctidas_intrvl * intr, int64_t first );

int64_t ctidas_intrvl_first_get ( ctidas_intrvl * intr );

void ctidas_intrvl_last_set ( ctidas_intrvl * intr, int64_t last );

int64_t ctidas_intrvl_last_get ( ctidas_intrvl * intr );

ctidas_intrvl ** ctidas_intrvl_list_alloc ( size_t n );

void ctidas_intrvl_list_free ( ctidas_intrvl ** intrl, size_t n );


/* Intervals */


struct ctidas_intervals_;
typedef struct ctidas_intervals_ ctidas_intervals;

ctidas_intervals * ctidas_intervals_alloc ( ctidas_dict * dct, size_t size );

void ctidas_intervals_free ( ctidas_intervals * inv );

size_t ctidas_intervals_size ( ctidas_intervals * inv );

ctidas_dict * ctidas_intervals_dict ( ctidas_intervals * inv );

ctidas_intrvl ** ctidas_intervals_read ( ctidas_intervals * inv, size_t * n );

void ctidas_intervals_write ( ctidas_intervals * inv, ctidas_intrvl ** intrl, size_t n );

ctidas_index_type ctidas_intervals_samples ( ctidas_intrvl ** intrl, size_t n );

ctidas_time_type ctidas_intervals_time ( ctidas_intrvl ** intrl, size_t n );

ctidas_intrvl * ctidas_intervals_seek ( ctidas_intrvl ** intrl, size_t n, ctidas_time_type time );

ctidas_intrvl * ctidas_intervals_seek_ceil ( ctidas_intrvl ** intrl, size_t n, ctidas_time_type time );

ctidas_intrvl * ctidas_intervals_seek_floor ( ctidas_intrvl ** intrl, size_t n, ctidas_time_type time );


/* Group */

/*
struct ctidas_group_;
typedef struct ctidas_group_ ctidas_group;

ctidas_group * ctidas_group_alloc ( ctidas_schema * schm, ctidas_dict * dct, ctidas_index_type size );

void ctidas_group_free ( ctidas_group * grp );

ctidas_dict * ctidas_group_dict ( ctidas_group * grp );

ctidas_schema * ctidas_group_schema ( ctidas_group * grp );

ctidas_index_type ctidas_group_size ( ctidas_group * grp );

void ctidas_group_resize ( ctidas_group * grp, ctidas_index_type newsize );

void ctidas_group_range ( ctidas_group * grp, ctidas_time_type * start, ctidas_time_type * stop );

void ctidas_group_read_times ( ctidas_group * grp, ctidas_time_type * data );

void ctidas_group_write_times ( ctidas_group * grp, ctidas_time_type * data );


      void read_times ( std::vector < time_type > & data ) const;

      void write_times ( std::vector < time_type > const & data );

      template < class T >
      void read_field ( std::string const & field_name, index_type offset, std::vector < T > & data ) const {
        field check = schm_.field_get ( field_name );
        if ( ( check.name != field_name ) && ( field_name != group_time_field ) ) {
          std::ostringstream o;
          o << "cannot read non-existent field " << field_name << " from group " << loc_.path << "/" << loc_.name;
          TIDAS_THROW( o.str().c_str() );
        }
        index_type n = data.size();
        if ( offset + n > size_ ) {
          std::ostringstream o;
          o << "cannot read field " << field_name << ", samples " << offset << " - " << (offset+n-1) << " from group " << loc_.name << " (" << size_ << " samples)";
          TIDAS_THROW( o.str().c_str() );
        }
        if ( loc_.type != backend_type::none ) {
          backend_->read_field ( loc_, field_name, type_indx_.at( field_name ), offset, data );
        } else {
          TIDAS_THROW( "cannot read field- backend not assigned" );
        }
        return;
      }

      template < class T >
      void write_field ( std::string const & field_name, index_type offset, std::vector < T > const & data ) {
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
        } else {
          TIDAS_THROW( "cannot write field- backend not assigned" );
        }
        return;
      }

      // overload for time_type which updates range

      void write_field ( std::string const & field_name, index_type offset, std::vector < time_type > const & data );
*/

#ifdef __cplusplus
}
#endif

#endif

