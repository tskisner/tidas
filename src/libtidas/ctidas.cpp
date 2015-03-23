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


data_type ctidas::convert_type ( ctidas_data_type in ) {
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


/*

// observation object


void ctoast_observation_free ( ctoast_observation * obs ) {
  ctoast::ptr_free < ctoast_observation, observation > ( obs );
  return;
}


void ctoast_observations_free ( ctoast_observation ** obs, size_t n ) {
  if ( n > 0 ) {
    for ( size_t i = 0; i < n; ++i ) {
      ctoast::ptr_free < ctoast_observation, observation > ( obs[i] );
    }
  }
  return;
}


void ctoast_observation_name ( ctoast_observation * obs, char * name ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  string namestr = ob->name();
  strcpy ( name, namestr.c_str() );
  return;
}


void ctoast_observation_flush ( ctoast_observation * obs ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  ob->flush();
  return;
}


double ctoast_observation_global_time ( ctoast_observation * obs, int64_t sample ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  return ob->global_time( sample );
}

void ctoast_observation_global_times ( ctoast_observation * obs, int64_t first, int64_t last, double * times ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  
  int64_t n = last - first + 1;
  time_vec tbuf;

  ob->global_times( first, n, tbuf );
  memcpy( (void*)times, &(tbuf[0]), n * sizeof(double) );

  return;
}

double ctoast_observation_rate ( ctoast_observation * obs ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  return ob->rate();
}

ctoast_chunk ** ctoast_observation_valid_chunks ( ctoast_observation * obs, double extension, size_t * n ) {
  observation_p ob = ctoast::ptr_extract < ctoast_observation, observation > ( obs );
  
  vector < chunk_p > chk = ob->valid_chunks(extension);
  
  (*n) = chk.size();

  if ( *n < 1 ) {
    return NULL;
  }
  
  ctoast_chunk ** ret = (ctoast_chunk **) malloc ( chk.size() * sizeof ( ctoast_chunk * ) );
  if ( ! ret ) {
    TOAST_THROW( "cannot allocate C bindings buffer for chunkset chunks list" );
  }

  for ( size_t i = 0; i < chk.size(); ++i ) {
    chunk_p * ck = new chunk_p ( chk[i] );
    ret[i] = ctoast::ptr_init < ctoast_chunk, chunk > ( ck );
  }
  
  return ret;
}

*/

