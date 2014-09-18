/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


data_type tidas::data_type_get ( type_info const & test ) {
	data_type ret = TYPE_NONE;

	int8_t type_int8;
	uint8_t type_uint8;
	int16_t type_int16;
	uint16_t type_uint16;
	int32_t type_int32;
	uint32_t type_uint32;
	int64_t type_int64;
	uint64_t type_uint64;
	float type_float;
	double type_double;
	char * type_string;

	if ( test == typeid ( type_int8 ) ) {
		ret = TYPE_INT8;
	} else if ( test == typeid ( type_uint8 ) ) {
		ret = TYPE_UINT8;
	} else if ( test == typeid ( type_int16 ) ) {
		ret = TYPE_INT16;
	} else if ( test == typeid ( type_uint16 ) ) {
		ret = TYPE_UINT16;
	} else if ( test == typeid ( type_int32 ) ) {
		ret = TYPE_INT32;
	} else if ( test == typeid ( type_uint32 ) ) {
		ret = TYPE_UINT32;
	} else if ( test == typeid ( type_int64 ) ) {
		ret = TYPE_INT64;
	} else if ( test == typeid ( type_uint64 ) ) {
		ret = TYPE_UINT64;
	} else if ( test == typeid ( type_float ) ) {
		ret = TYPE_FLOAT32;
	} else if ( test == typeid ( type_double ) ) {
		ret = TYPE_FLOAT64;
	} else if ( test == typeid ( type_string ) ) {
		ret = TYPE_STRING;
	}

	return ret;
}


string data_type_to_string ( data_type type ) {
	string ret;
	switch ( type ) {
		case TYPE_INT8:
			ret = "TYPE_INT8";
			break;
		case TYPE_UINT8:
			ret = "TYPE_UINT8";
			break;
		case TYPE_INT16:
			ret = "TYPE_INT16";
			break;
		case TYPE_UINT16:
			ret = "TYPE_UINT16";
			break;
		case TYPE_INT32:
			ret = "TYPE_INT32";
			break;
		case TYPE_UINT32:
			ret = "TYPE_UINT32";
			break;
		case TYPE_INT64:
			ret = "TYPE_INT64";
			break;
		case TYPE_UINT64:
			ret = "TYPE_UINT64";
			break;
		case TYPE_FLOAT32:
			ret = "TYPE_FLOAT32";
			break;
		case TYPE_FLOAT64:
			ret = "TYPE_FLOAT64";
			break;
		case TYPE_STRING:
			ret = "TYPE_STRING";
			break;
		default:
			TIDAS_THROW( "data type not recognized" );
			break;
	}
	return ret;
}


data_type data_type_from_string ( string const & name ) {
	data_type ret;
	if ( name == "TYPE_INT8" ) {
		ret = TYPE_INT8;
	} else if ( name == "TYPE_UINT8" ) {
		ret = TYPE_UINT8;
	} else if ( name == "TYPE_INT16" ) {
		ret = TYPE_INT16;
	} else if ( name == "TYPE_UINT16" ) {
		ret = TYPE_UINT16;
	} else if ( name == "TYPE_INT32" ) {
		ret = TYPE_INT32;
	} else if ( name == "TYPE_UINT32" ) {
		ret = TYPE_UINT32;
	} else if ( name == "TYPE_INT64" ) {
		ret = TYPE_INT64;
	} else if ( name == "TYPE_UINT64" ) {
		ret = TYPE_UINT64;
	} else if ( name == "TYPE_FLOAT32" ) {
		ret = TYPE_FLOAT32;
	} else if ( name == "TYPE_FLOAT64" ) {
		ret = TYPE_FLOAT64;
	} else if ( name == "TYPE_STRING" ) {
		ret = TYPE_STRING;
	} else {
		TIDAS_THROW( "data type not recognized" );
	}
	return ret;
}


tidas::backend_path::backend_path () {
	path = "";
	name = "";
	meta = "";
	type = BACKEND_MEM;
	comp = COMPRESS_NONE;
	//vol = NULL;
}


