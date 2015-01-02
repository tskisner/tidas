/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>


using namespace std;
using namespace tidas;



#ifdef HAVE_HDF5


hid_t tidas::hdf5_data_type ( data_type type ) {

	hid_t datatype;
	herr_t status;

	switch ( type ) {
		case TYPE_INT8:
			datatype = H5Tcopy ( H5T_NATIVE_INT8 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_UINT8:
			datatype = H5Tcopy ( H5T_NATIVE_UINT8 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_INT16:
			datatype = H5Tcopy ( H5T_NATIVE_INT16 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_UINT16:
			datatype = H5Tcopy ( H5T_NATIVE_UINT16 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_INT32:
			datatype = H5Tcopy ( H5T_NATIVE_INT32 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_UINT32:
			datatype = H5Tcopy ( H5T_NATIVE_UINT32 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_INT64:
			datatype = H5Tcopy ( H5T_NATIVE_INT64 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_UINT64:
			datatype = H5Tcopy ( H5T_NATIVE_UINT64 );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_FLOAT32:
			datatype = H5Tcopy ( H5T_NATIVE_FLOAT );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_FLOAT64:
			datatype = H5Tcopy ( H5T_NATIVE_DOUBLE );
			status = H5Tset_order ( datatype, H5T_ORDER_LE );
			break;
		case TYPE_STRING:
			datatype = H5Tcopy ( H5T_C_S1 );
			status = H5Tset_size ( datatype, backend_string_size );
			break;
		default:
			TIDAS_THROW( "data type not recognized" );
			break;
	}

	return datatype;
}




#endif




