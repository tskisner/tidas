/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.h>

#include <float.h>
#include <math.h>

#ifdef HAVE_HDF5
#include <hdf5.h>
#endif



void tidas_intrvl_init ( void * addr ) {

	tidas_intrvl * elem = (tidas_intrvl *)addr;

	elem->start = 0.0;
	elem->stop = 0.0;

	return;
}


void tidas_intrvl_clear ( void * addr ) {

	tidas_intrvl * elem = (tidas_intrvl *)addr;

	elem->start = 0.0;
	elem->stop = 0.0;

	return;
}


void tidas_intrvl_copy ( void * dest, void const * src ) {

	tidas_intrvl * elem_dest = (tidas_intrvl *)dest;

	tidas_intrvl const * elem_src = (tidas_intrvl *)src;

	elem_dest->start = elem_src->start;
	elem_dest->stop = elem_src->stop;

	return;
}


int tidas_intrvl_comp ( void const * addr1, void const * addr2 ) {
	int ret;
	tidas_intrvl const * elem1 = (tidas_intrvl *)addr1;
	tidas_intrvl const * elem2 = (tidas_intrvl *)addr2;

	if ( fabs ( elem1->start - elem2->start ) < DBL_EPSILON ) {
		ret = 0;
	} else if ( elem1->start <= elem2->start ) {
		ret = -1;
	} else {
		ret = 1;
	}
	return ret;
}


tidas_intervals * tidas_intervals_alloc () {
	tidas_intervals * intervals = (tidas_intervals *) malloc ( sizeof( tidas_intervals ) );
	if ( ! intervals ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate tidas intervals", NULL );
	}

	tidas_vector_ops ops;
	ops.size = sizeof( tidas_intrvl );
	ops.init = tidas_intrvl_init;
	ops.clear = tidas_intrvl_clear;
	ops.copy = tidas_intrvl_copy;
	ops.comp = tidas_intrvl_comp;

	intervals->data = tidas_vector_alloc ( ops );

	return intervals;
}


void tidas_intervals_free ( tidas_intervals * intervals ) {
	if ( intervals ) {
		tidas_vector_free ( intervals->data );
		free ( intervals );
	}
	return;
}


void tidas_intervals_clear ( tidas_intervals * intervals ) {
	TIDAS_PTR_CHECK( intervals );
	tidas_vector_clear ( intervals->data );
	return;
}


tidas_intervals * tidas_intervals_copy ( tidas_intervals const * orig ) {
	tidas_intervals * intervals = (tidas_intervals *) malloc ( sizeof( tidas_intervals ) );
	if ( ! intervals ) {
		TIDAS_ERROR_VAL( TIDAS_ERR_ALLOC, "cannot allocate tidas intervals", NULL );
	}
	intervals->data = tidas_vector_copy ( orig->data );
	return intervals;
}


void tidas_intervals_append ( tidas_intervals * intervals, tidas_intrvl const * intrvl ) {
	size_t cursize;
	size_t newsize;
	void * cursor;

	TIDAS_PTR_CHECK( intervals );

	cursize = intervals->data->n;
	newsize = cursize + 1;

	tidas_vector_resize ( intervals->data, newsize );

	cursor = tidas_vector_set ( intervals->data, cursize );

	tidas_intrvl_copy ( cursor, (void const *) intrvl );

	return;
}


tidas_intrvl const * tidas_intervals_get ( tidas_intervals const * intervals, size_t indx ) {
	TIDAS_PTR_CHECK( intervals );
	return (tidas_intrvl const *) tidas_vector_get ( intervals->data, indx );
}


tidas_intrvl const * tidas_intervals_seek ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time ) {
	size_t i;
	tidas_intrvl const * cursor;
	tidas_intrvl const * found = NULL;

	TIDAS_PTR_CHECK( intervals );

	for ( i = 0; i < intervals->data->n; ++i ) {
		cursor = (tidas_intrvl const *) tidas_vector_get ( intervals->data, i );
		if ( ( time >= cursor->start ) && ( time <= cursor->stop ) ) {
			found = cursor;
			break;
		}
	}

	return found;
}


tidas_intrvl const * tidas_intervals_seek_ceil ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time ) {
	size_t i;
	tidas_intrvl const * cursor;
	tidas_intrvl const * found = NULL;

	TIDAS_PTR_CHECK( intervals );

	for ( i = 0; i < intervals->data->n; ++i ) {
		cursor = (tidas_intrvl const *) tidas_vector_get ( intervals->data, i );
		if ( time < cursor->stop ) {
			/* we are inside the interval, or after the stop of the previous one */
			found = cursor;
			break;
		}
	}

	return found;
}


tidas_intrvl const * tidas_intervals_seek_floor ( tidas_intervals const * intervals, TIDAS_DTYPE_TIME time ) {
	size_t i;
	tidas_intrvl const * cursor;
	tidas_intrvl const * found = NULL;

	TIDAS_PTR_CHECK( intervals );

	for ( i = 0; i < intervals->data->n; ++i ) {
		cursor = (tidas_intrvl const *) tidas_vector_get ( intervals->data, i );
		if ( time < cursor->start ) {
			/* we are inside the previous interval or before the start of this one */
			break;
		}
		found = cursor;
	}

	return found;
}


void tidas_intervals_read ( tidas_intervals * intervals, tidas_backend backend, char const * fspath, char const * metapath, char const * name ) {

	TIDAS_PTR_CHECK( intervals );

	switch ( backend ) {
		case TIDAS_BACKEND_MEM:
			/* this is a no-op */
			break;
		case TIDAS_BACKEND_HDF5:
			tidas_intervals_read_hdf5 ( intervals, fspath, metapath, name );
			break;
		case TIDAS_BACKEND_GETDATA:
			TIDAS_ERROR_VOID( TIDAS_ERR_OPTION, "getdata backend not yet implemented" );
			break;
		default:
			TIDAS_ERROR_VOID( TIDAS_ERR_OPTION, "backend not recognized" );
			break;
	}

	return;
}


void tidas_intervals_write ( tidas_intervals const * intervals, tidas_backend backend, char const * fspath, char const * metapath, char const * name ) {

	TIDAS_PTR_CHECK( intervals );

	switch ( backend ) {
		case TIDAS_BACKEND_MEM:
			/* this is a no-op */
			break;
		case TIDAS_BACKEND_HDF5:
			tidas_intervals_write_hdf5 ( intervals, fspath, metapath, name );
			break;
		case TIDAS_BACKEND_GETDATA:
			TIDAS_ERROR_VOID( TIDAS_ERR_OPTION, "getdata backend not yet implemented" );
			break;
		default:
			TIDAS_ERROR_VOID( TIDAS_ERR_OPTION, "backend not recognized" );
			break;
	}

	return;
}


#ifdef HAVE_HDF5

void tidas_intervals_read_hdf5 ( tidas_intervals * intervals, char const * path, char const * h5path, char const * name ) {
	hid_t file;
	herr_t status;
	int64_t fsize;
	double * buffer;
	hid_t dataset;
	hid_t datatype;
	hid_t dataspace;
	int ndims;
	hsize_t dims;
	hsize_t maxdims;
	int ret;
	size_t n;
	size_t i;
	tidas_intrvl intrvl;
	char metapath[ TIDAS_PATH_LEN ];

	/* check if file exists */

	fsize = tidas_fs_stat ( path );
	if ( fsize <= 0 ) {
		TIDAS_ERROR_VOID( TIDAS_ERR_HDF5, "file does not exist" );
	}

	/* clear out intervals */

	tidas_intervals_clear ( intervals );

	/* open file */

	file = H5Fopen ( path, H5F_ACC_RDONLY, H5P_DEFAULT );

	/* open dataset and get dimensions */

	snprintf ( metapath, TIDAS_PATH_LEN, "%s/%s", h5path, name );

	dataset = H5Dopen ( file, metapath, H5P_DEFAULT);

	dataspace = H5Dget_space ( dataset );
	datatype = H5Dget_type ( dataset );

	ndims = H5Sget_simple_extent_ndims ( dataspace );

	if ( ndims != 1 ) {
		TIDAS_ERROR_VOID( TIDAS_ERR_HDF5, "intervals dataset has wrong dimensions" );
	}

	ret = H5Sget_simple_extent_dims ( dataspace, &dims, &maxdims );

	buffer = tidas_double_alloc ( (size_t)dims );

	status = H5Dread ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer );

	/* clean up */

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );
	status = H5Fclose ( file ); 

	/* copy buffer into intervals */

	n = (size_t)( dims / 2 );

	for ( i = 0; i < n; ++i ) {
		intrvl.start = buffer[ 2 * i ];
		intrvl.stop = buffer[ 2 * i + 1];
		tidas_intervals_append ( intervals, &intrvl );
	}

	return;
}


typedef struct {
	double * buffer;
} tidas_intervals_hdf5_pack;


void tidas_intervals_hdf5_packer ( void const * addr, size_t indx, void * props ) {
	tidas_intrvl const * elem = (tidas_intrvl const *)addr;
	tidas_intervals_hdf5_pack * pack = (tidas_intervals_hdf5_pack *)props;

	pack->buffer[ 2 * indx ] = elem->start;
	pack->buffer[ 2 * indx + 1 ] = elem->stop;

	return;
}


void tidas_intervals_write_hdf5 ( tidas_intervals const * intervals, char const * path, char const * h5path, char const * name ) {
	hid_t file;
	herr_t status;
	hid_t dataset;
	hid_t datatype;
	hid_t dataspace;
	hsize_t dims[1];
	char metapath[ TIDAS_PATH_LEN ];

	tidas_intervals_hdf5_pack pack;

	/* open file in write mode */

	file = H5Fopen ( path, H5F_ACC_RDWR, H5P_DEFAULT );

	/* create dataset */

	dims[0] = 2 * intervals->data->n;

	dataspace = H5Screate_simple ( 1, dims, NULL ); 

	datatype = H5Tcopy ( H5T_NATIVE_DOUBLE );

	status = H5Tset_order ( datatype, H5T_ORDER_LE );

	snprintf ( metapath, TIDAS_PATH_LEN, "%s/%s", h5path, name );

	dataset = H5Dcreate ( file, metapath, datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	/* repack interval data into contiguous buffer */

	pack.buffer = tidas_double_alloc ( 2 * intervals->data->n * sizeof(double) );

	tidas_vector_view ( intervals->data, tidas_intervals_hdf5_packer, (void*)( &pack ) );

	status = H5Dwrite ( dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, pack.buffer );

	free ( pack.buffer );

	/* clean up */

	H5Sclose ( dataspace );
	H5Tclose ( datatype );
	H5Dclose ( dataset );
	status = H5Fclose ( file ); 

	return;
}

#else

void tidas_intervals_read_hdf5 ( tidas_intervals * intervals, char const * path ) {
	TIDAS_ERROR_VOID( TIDAS_ERR_HDF5, "TIDAS not compiled with HDF5 support" );
	return;
}


void tidas_intervals_write_hdf5 ( tidas_intervals const * intervals, char const * path ) {
	TIDAS_ERROR_VOID( TIDAS_ERR_HDF5, "TIDAS not compiled with HDF5 support" );
	return;
}

#endif














