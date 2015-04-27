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

char * ctidas_dict_key ( ctidas_dict * dct, size_t index );

ctidas_data_type ctidas_dict_type ( ctidas_dict * dct, size_t index );

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

void ctidas_field_free ( ctidas_field * field );

void ctidas_field_type_set ( ctidas_field * field, ctidas_data_type type );

ctidas_data_type ctidas_field_type_get ( ctidas_field * field );

void ctidas_field_name_set ( ctidas_field * field, char const * name );

char * ctidas_field_name_get ( ctidas_field * field );

void ctidas_field_units_set ( ctidas_field * field, char const * units );

char * ctidas_field_units_get ( ctidas_field * field );


/* Schema */


typedef struct {
  void * handle;
} ctidas_schema;

ctidas_schema * ctidas_schema_alloc ( );

void ctidas_schema_free ( ctidas_schema * schm );

void ctidas_schema_clear ( ctidas_schema * schm );

size_t ctidas_schema_nfields ( ctidas_schema * schm );

void ctidas_schema_field_add ( ctidas_schema * schm, ctidas_field const * fld );

void ctidas_schema_field_del ( ctidas_schema * schm, char const * name );

ctidas_field * ctidas_schema_field_get_byname ( ctidas_schema * schm, char const * name );

ctidas_field * ctidas_schema_field_get_byindex ( ctidas_schema * schm, size_t index );


/* Single interval */


typedef struct {
  void * handle;
} ctidas_intrvl;

ctidas_intrvl * ctidas_intrvl_alloc ( );

void ctidas_intrvl_free ( ctidas_intrvl * intrvl );

void ctidas_intrvl_start_set ( ctidas_intrvl * intrvl, double start );

double ctidas_intrvl_start_get ( ctidas_intrvl * intrvl );

void ctidas_intrvl_stop_set ( ctidas_intrvl * intrvl, double stop );

double ctidas_intrvl_stop_get ( ctidas_intrvl * intrvl );

void ctidas_intrvl_first_set ( ctidas_intrvl * intrvl, int64_t first );

int64_t ctidas_intrvl_first_get ( ctidas_intrvl * intrvl );

void ctidas_intrvl_last_set ( ctidas_intrvl * intrvl, int64_t last );

int64_t ctidas_intrvl_last_get ( ctidas_intrvl * intrvl );


/* Intervals */


typedef struct {
  void * handle;
} ctidas_intervals;

ctidas_intervals * ctidas_intervals_alloc ( );

void ctidas_intervals_free ( ctidas_intervals * intr );

void ctidas_intervals_wipe ( ctidas_intervals * intr );

size_t ctidas_intervals_size ( ctidas_intervals * intr );

ctidas_dict * ctidas_intervals_dict ( ctidas_intervals * intr );


/*


      void read_data ( interval_list & intr ) const;

      void write_data ( interval_list const & intr );

      static index_type total_samples ( interval_list const & intr );

      static time_type total_time ( interval_list const & intr );

      static intrvl seek ( interval_list const & intr, time_type time );

      static intrvl seek_ceil ( interval_list const & intr, time_type time );
      
      static intrvl seek_floor ( interval_list const & intr, time_type time );
*/


#ifdef __cplusplus
}
#endif

#endif

