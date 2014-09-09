/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.h>


typedef struct {
	char name[ TIDAS_NAME_LEN ];
	tidas_backend backend;
	tidas_schema * schema;
	void * backdat;
} tidas_group;


void tidas_group_init ( void * addr ) {

	tidas_group * elem = (tidas_group *)addr;

	strcpy ( elem->name, "" );
	elem->backend = TIDAS_BACKEND_MEM;
	elem->schema = NULL;
	elem->backdat = NULL;

	return;
}


void tidas_group_clear ( void * addr ) {

	tidas_group * elem = (tidas_group *)addr;

	strcpy ( elem->name, "" );

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


void tidas_group_copy ( void * dest, void const * src ) {

	tidas_group * elem_dest = (tidas_group *)dest;

	tidas_group const * elem_src = (tidas_group *)src;

	elem_dest->start = elem_src->start;
	elem_dest->stop = elem_src->stop;

	return;
}


int tidas_group_comp ( void const * addr1, void const * addr2 ) {
	int ret;
	tidas_group const * elem1 = (tidas_group *)addr1;
	tidas_group const * elem2 = (tidas_group *)addr2;

	if ( fabs ( elem1->start - elem2->start ) < DBL_EPSILON ) {
		ret = 0;
	} else if ( elem1->start <= elem2->start ) {
		ret = -1;
	} else {
		ret = 1;
	}
	return ret;
}





