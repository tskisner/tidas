/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>


using namespace std;
using namespace tidas;

/*
group ();
			group ( schema const & schm, index_type nsamp );
			group ( schema const & schm, dict const & dictionary, index_type nsamp );
			group ( backend_path const & loc );
			group ( group const & orig );
			~group ();

			void read_meta ();

			void write_meta ();

			void relocate ( backend_path const & loc );

			backend_path location () const;

			void duplicate ( backend_path const & newloc, group_select const & selection );

			schema const & schema_get () const;

			template < class T >
			void dictionary_put ( std::string const & key, T const & val ) {
				dict_.put < T > ( key, val );
				return;
			}

			dict const & dictionary () const;

			index_type nsamp () const;

			void times ( std::vector < time_type > & data );
*/


TEST( grouptest, hdf5meta ) {

	field f_int8;
	field f_uint8;
	field f_int16;
	field f_uint16;
	field f_int32;
	field f_uint32;
	field f_int64;
	field f_uint64;
	field f_float32;
	field f_float64;

	size_t nf = 10;

	f_int8.type = TYPE_INT8;
	f_int8.name = "int8";
	f_int8.units = "int8";

	f_uint8.type = TYPE_UINT8;
	f_uint8.name = "uint8";
	f_uint8.units = "uint8";

	f_int16.type = TYPE_INT16;
	f_int16.name = "int16";
	f_int16.units = "int16";

	f_uint16.type = TYPE_UINT16;
	f_uint16.name = "uint16";
	f_uint16.units = "uint16";

	f_int32.type = TYPE_INT32;
	f_int32.name = "int32";
	f_int32.units = "int32";

	f_uint32.type = TYPE_UINT32;
	f_uint32.name = "uint32";
	f_uint32.units = "uint32";

	f_int64.type = TYPE_INT64;
	f_int64.name = "int64";
	f_int64.units = "int64";

	f_uint64.type = TYPE_UINT64;
	f_uint64.name = "uint64";
	f_uint64.units = "uint64";

	f_float32.type = TYPE_FLOAT32;
	f_float32.name = "float32";
	f_float32.units = "float32";

	f_float64.type = TYPE_FLOAT64;
	f_float64.name = "float64";
	f_float64.units = "float64";

	field_list flist;
	flist.clear();
	flist.push_back ( f_int8 );
	flist.push_back ( f_uint8 );
	flist.push_back ( f_int16 );
	flist.push_back ( f_uint16 );
	flist.push_back ( f_int32 );
	flist.push_back ( f_uint32 );
	flist.push_back ( f_int64 );
	flist.push_back ( f_uint64 );
	flist.push_back ( f_float32 );
	flist.push_back ( f_float64 );

	schema schm ( flist );

	dict d;
	string sval = "blahblahblah";
	double dval = 12345.6;
	int ival = 12345;
	long long lval = 1234567890123456L;

	dict test;

	test.put ( "string", sval );
	test.put ( "double", dval );
	test.put ( "int", ival );
	test.put ( "longlong", lval );

	index_type nsamp = 100;

	// instantiate group

	group grp1 ( schm, d, nsamp );

	backend_path loc;
	loc.type = BACKEND_HDF5;
	loc.path = ".";
	loc.name = "test_group_meta.hdf5.out";

	grp1.relocate ( loc );
	grp1.write_meta();

	loc.name = "test_group_data.hdf5.out";

	grp1.relocate ( loc );
	grp1.write_meta();

	// write all fields





}

