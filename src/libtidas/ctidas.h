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
    acc_write
} ctidas_access_mode;


typedef enum {
    link_none,
    link_hard,
    link_soft
} ctidas_link_type;


typedef enum {
    order_depth_first,
    order_depth_last,
    order_leaf
} ctidas_exec_order;


/* Dictionary */

struct ctidas_dict_;
typedef struct ctidas_dict_ ctidas_dict;

ctidas_dict * ctidas_dict_alloc ( );

void ctidas_dict_free ( ctidas_dict * dct );

void ctidas_dict_clear ( ctidas_dict * dct );

size_t ctidas_dict_nkeys ( ctidas_dict const * dct );

char ** ctidas_dict_keys ( ctidas_dict const * dct, size_t * nkeys );

ctidas_data_type ctidas_dict_type ( ctidas_dict const * dct, char const * key );

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

int8_t ctidas_dict_get_int8 ( ctidas_dict const * dct, char const * key );

int16_t ctidas_dict_get_int16 ( ctidas_dict const * dct, char const * key );

int32_t ctidas_dict_get_int32 ( ctidas_dict const * dct, char const * key );

int64_t ctidas_dict_get_int64 ( ctidas_dict const * dct, char const * key );

uint8_t ctidas_dict_get_uint8 ( ctidas_dict const * dct, char const * key );

uint16_t ctidas_dict_get_uint16 ( ctidas_dict const * dct, char const * key );

uint32_t ctidas_dict_get_uint32 ( ctidas_dict const * dct, char const * key );

uint64_t ctidas_dict_get_uint64 ( ctidas_dict const * dct, char const * key );

float ctidas_dict_get_float ( ctidas_dict const * dct, char const * key );

double ctidas_dict_get_double ( ctidas_dict const * dct, char const * key );

char * ctidas_dict_get_string ( ctidas_dict const * dct, char const * key );


/* Schema Field */


struct ctidas_field_;
typedef struct ctidas_field_ ctidas_field;

ctidas_field * ctidas_field_alloc ( );

void ctidas_field_free ( ctidas_field * fld );

void ctidas_field_type_set ( ctidas_field * fld, ctidas_data_type type );

ctidas_data_type ctidas_field_type_get ( ctidas_field const * fld );

void ctidas_field_name_set ( ctidas_field * fld, char const * name );

char const * ctidas_field_name_get ( ctidas_field const * fld );

void ctidas_field_units_set ( ctidas_field * fld, char const * units );

char const * ctidas_field_units_get ( ctidas_field const * fld );


/* Schema */


struct ctidas_schema_;
typedef struct ctidas_schema_ ctidas_schema;

ctidas_schema * ctidas_schema_alloc ( );

void ctidas_schema_free ( ctidas_schema * schm );

void ctidas_schema_clear ( ctidas_schema * schm );

char ** ctidas_schema_fields ( ctidas_schema const * schm, size_t * nfields );

void ctidas_schema_field_add ( ctidas_schema * schm, ctidas_field const * fld );

void ctidas_schema_field_del ( ctidas_schema * schm, char const * name );

ctidas_field * ctidas_schema_field_get ( ctidas_schema const * schm, char const * name );


/* Single interval */


struct ctidas_intrvl_;
typedef struct ctidas_intrvl_ ctidas_intrvl;

ctidas_intrvl * ctidas_intrvl_alloc ( );

void ctidas_intrvl_free ( ctidas_intrvl * intr );

void ctidas_intrvl_start_set ( ctidas_intrvl * intr, double start );

double ctidas_intrvl_start_get ( ctidas_intrvl const * intr );

void ctidas_intrvl_stop_set ( ctidas_intrvl * intr, double stop );

double ctidas_intrvl_stop_get ( ctidas_intrvl const * intr );

void ctidas_intrvl_first_set ( ctidas_intrvl * intr, int64_t first );

int64_t ctidas_intrvl_first_get ( ctidas_intrvl const * intr );

void ctidas_intrvl_last_set ( ctidas_intrvl * intr, int64_t last );

int64_t ctidas_intrvl_last_get ( ctidas_intrvl const * intr );

ctidas_intrvl ** ctidas_intrvl_list_alloc ( size_t n );

void ctidas_intrvl_list_free ( ctidas_intrvl ** intrl, size_t n );


/* Intervals */


struct ctidas_intervals_;
typedef struct ctidas_intervals_ ctidas_intervals;

ctidas_intervals * ctidas_intervals_alloc ( ctidas_dict const * dct, size_t size );

void ctidas_intervals_free ( ctidas_intervals * inv );

size_t ctidas_intervals_size ( ctidas_intervals const * inv );

ctidas_dict const * ctidas_intervals_dict ( ctidas_intervals const * inv );

ctidas_intrvl ** ctidas_intervals_read ( ctidas_intervals const * inv, size_t * n );

void ctidas_intervals_write ( ctidas_intervals * inv, ctidas_intrvl * const * intrl, size_t n );

ctidas_index_type ctidas_intervals_samples ( ctidas_intrvl * const * intrl, size_t n );

ctidas_time_type ctidas_intervals_time ( ctidas_intrvl * const * intrl, size_t n );

ctidas_intrvl * ctidas_intervals_seek ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time );

ctidas_intrvl * ctidas_intervals_seek_ceil ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time );

ctidas_intrvl * ctidas_intervals_seek_floor ( ctidas_intrvl * const * intrl, size_t n, ctidas_time_type time );


/* Group */


struct ctidas_group_;
typedef struct ctidas_group_ ctidas_group;

ctidas_group * ctidas_group_alloc ( ctidas_schema const * schm, ctidas_dict const * dct, ctidas_index_type size );

void ctidas_group_free ( ctidas_group * grp );

ctidas_dict const * ctidas_group_dict ( ctidas_group const * grp );

ctidas_schema const * ctidas_group_schema ( ctidas_group const * grp );

ctidas_index_type ctidas_group_size ( ctidas_group const * grp );

void ctidas_group_resize ( ctidas_group * grp, ctidas_index_type newsize );

void ctidas_group_range ( ctidas_group const * grp, ctidas_time_type * start, ctidas_time_type * stop );

void ctidas_group_read_times ( ctidas_group const * grp, ctidas_index_type ndata, ctidas_time_type * data );

void ctidas_group_write_times ( ctidas_group * grp, ctidas_index_type ndata, ctidas_time_type const * data );


void ctidas_group_read_int8 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int8_t * data );

void ctidas_group_write_int8 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int8_t const * data );

void ctidas_group_read_int16 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int16_t * data );

void ctidas_group_write_int16 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int16_t const * data );

void ctidas_group_read_int32 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int32_t * data );

void ctidas_group_write_int32 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int32_t const * data );

void ctidas_group_read_int64 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int64_t * data );

void ctidas_group_write_int64 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, int64_t const * data );


void ctidas_group_read_uint8 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint8_t * data );

void ctidas_group_write_uint8 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint8_t const * data );

void ctidas_group_read_uint16 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint16_t * data );

void ctidas_group_write_uint16 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint16_t const * data );

void ctidas_group_read_uint32 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint32_t * data );

void ctidas_group_write_uint32 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint32_t const * data );

void ctidas_group_read_uint64 ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint64_t * data );

void ctidas_group_write_uint64 ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, uint64_t const * data );


void ctidas_group_read_float ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, float * data );

void ctidas_group_write_float ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, float const * data );

void ctidas_group_read_double ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, double * data );

void ctidas_group_write_double ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, double const * data );


void ctidas_group_read_string ( ctidas_group const * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, char ** data );

void ctidas_group_write_string ( ctidas_group * grp, char const * field, ctidas_index_type offset, ctidas_index_type ndata, char * const * data );


/* Block */


struct ctidas_block_;
typedef struct ctidas_block_ ctidas_block;

ctidas_block * ctidas_block_alloc();

void ctidas_block_free ( ctidas_block * blk );

void ctidas_block_range ( ctidas_block const * blk, ctidas_time_type * start, ctidas_time_type * stop );

void ctidas_block_clear ( ctidas_block * blk );


ctidas_group * ctidas_block_group_add ( ctidas_block * blk, char const * name, ctidas_group * grp );

ctidas_group * ctidas_block_group_get ( ctidas_block * blk, char const * name );

ctidas_group const * ctidas_block_group_cget ( ctidas_block const * blk, char const * name );

void ctidas_block_group_del ( ctidas_block * blk, char const * name );

char ** ctidas_block_all_groups ( ctidas_block const * blk, size_t * ngroup );

void ctidas_block_clear_groups ( ctidas_block * blk );


ctidas_intervals * ctidas_block_intervals_add ( ctidas_block * blk, char const * name, ctidas_intervals * inv );

ctidas_intervals * ctidas_block_intervals_get ( ctidas_block * blk, char const * name );

ctidas_intervals const * ctidas_block_intervals_cget ( ctidas_block const * blk, char const * name );

void ctidas_block_intervals_del ( ctidas_block * blk, char const * name );

char ** ctidas_block_all_intervals ( ctidas_block const * blk, size_t * nintervals );

void ctidas_block_clear_intervals ( ctidas_block * blk );


ctidas_block * ctidas_block_child_add ( ctidas_block * blk, char const * name, ctidas_block * child );

ctidas_block * ctidas_block_child_get ( ctidas_block * blk, char const * name );

ctidas_block const * ctidas_block_child_cget ( ctidas_block const * blk, char const * name );

void ctidas_block_child_del ( ctidas_block * blk, char const * name );

char ** ctidas_block_all_children ( ctidas_block const * blk, size_t * nchild );

void ctidas_block_clear_children ( ctidas_block * blk );


ctidas_block * ctidas_block_select ( ctidas_block const * blk, char const * filter );


typedef void (*CTIDAS_EXEC_OP) ( ctidas_block const * blk, void * aux );

void ctidas_block_exec ( ctidas_block const * blk, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux );


/* Volume */


struct ctidas_volume_;
typedef struct ctidas_volume_ ctidas_volume;

ctidas_volume * ctidas_volume_create ( char const * path, ctidas_backend_type type, ctidas_compression_type comp );

ctidas_volume * ctidas_volume_open ( char const * path, ctidas_access_mode mode );

void ctidas_volume_close ( ctidas_volume * vol );

ctidas_block * ctidas_volume_root ( ctidas_volume * vol );

void ctidas_volume_exec ( ctidas_volume * vol, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux );


/* Data copy */


void ctidas_data_intervals_copy ( ctidas_intervals const * in, ctidas_intervals * out );

void ctidas_data_group_copy ( ctidas_group const * in, ctidas_group * out );

void ctidas_data_block_copy ( ctidas_block const * in, ctidas_block * out );

void ctidas_data_volume_copy ( ctidas_volume const * in, ctidas_volume * out );



#ifdef __cplusplus
}
#endif

#endif

