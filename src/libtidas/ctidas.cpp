/*
	TImestream DAta Storage (TIDAS)
	(c) 2014-2015, The Regents of the University of California, 
	through Lawrence Berkeley National Laboratory.  See top
	level LICENSE file for details.
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

size_t ctidas_dict_nkeys ( ctidas_dict * dct ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->data().size();
}

char ** ctidas_dict_keys ( ctidas_dict * dct, size_t * nkeys ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	size_t n = d->data().size();
	char ** ret = (char**)malloc(n * sizeof(char*));
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
		++cur;
	}
	return ret;
}

ctidas_data_type ctidas_dict_type ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
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

int8_t ctidas_dict_get_int8 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < int8_t > ( key );
}

int16_t ctidas_dict_get_int16 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < int16_t > ( key );
}

int32_t ctidas_dict_get_int32 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < int32_t > ( key );
}

int64_t ctidas_dict_get_int64 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < int64_t > ( key );
}

uint8_t ctidas_dict_get_uint8 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < uint8_t > ( key );
}

uint16_t ctidas_dict_get_uint16 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < uint16_t > ( key );
}

uint32_t ctidas_dict_get_uint32 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < uint32_t > ( key );
}

uint64_t ctidas_dict_get_uint64 ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < uint64_t > ( key );
}

float ctidas_dict_get_float ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < float > ( key );
}

double ctidas_dict_get_double ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	return d->get < double > ( key );
}

char * ctidas_dict_get_string ( ctidas_dict * dct, char const * key ) {
	dict * d = reinterpret_cast < dict * > ( dct );
	string str = d->get < string > ( key );
	char * ret = (char*)malloc(str.size() + 1);
	if ( ! ret ) {
		fprintf(stderr, "failed to allocate dict string value return\n");
		exit(1);
	}
	strncpy( ret, str.c_str(), str.size() );
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

ctidas_data_type ctidas_field_type_get ( ctidas_field * fld ) {
	field * f = reinterpret_cast < field * > ( fld );
	return ctidas::convert_to_c( f->type );
}

void ctidas_field_name_set ( ctidas_field * fld, char const * name ) {
	field * f = reinterpret_cast < field * > ( fld );
	f->name = string(name);
	return;
}

char * ctidas_field_name_get ( ctidas_field * fld ) {
	field * f = reinterpret_cast < field * > ( fld );
	char * ret = (char*)malloc( f->name.size() + 1 );
	if ( ! ret ) {
		fprintf(stderr, "failed to allocate field name string\n");
		exit(1);
	}
	strncpy( ret, f->name.c_str(), f->name.size() );
	return ret;
}

void ctidas_field_units_set ( ctidas_field * fld, char const * units ) {
	field * f = reinterpret_cast < field * > ( fld );
	f->units = string(units);
	return;
}

char * ctidas_field_units_get ( ctidas_field * fld ) {
	field * f = reinterpret_cast < field * > ( fld );
	char * ret = (char*)malloc( f->units.size() + 1 );
	if ( ! ret ) {
		fprintf(stderr, "failed to allocate field units string\n");
		exit(1);
	}
	strncpy( ret, f->units.c_str(), f->units.size() );
	return ret;
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

char ** ctidas_schema_fields ( ctidas_schema * schm, size_t * nfields ) {
	schema * s = reinterpret_cast < schema * > ( schm );
	size_t n = s->fields().size();
	char ** ret = (char**)malloc(n * sizeof(char*));
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

ctidas_field * ctidas_schema_field_get ( ctidas_schema * schm, char const * name ) {
	schema * s = reinterpret_cast < schema * > ( schm );
	ctidas_field * ret = ctidas_field_alloc();
	field * f = reinterpret_cast < field * > ( ret );
	(*f) = s->field_get( name );
	return ret;
}




