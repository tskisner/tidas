/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_test.hpp>

#include <fstream>


using namespace std;
using namespace tidas;


class cereal_test {

	public :

		cereal_test() {
			itest = 1;
			stest = "test";
			dtest = -1.0;
		}

		~cereal_test() {}

		int itest;
		string stest;
		double dtest;

		template < class Archive >
		void serialize ( Archive & ar ) {
			ar ( CEREAL_NVP( itest ) );
			ar ( CEREAL_NVP( stest ) );
			ar ( CEREAL_NVP( dtest ) );
			return;
		}

};


// recall EXPECT_EQ(expected, actual)


TEST( cerealtest, basic ) {

	cereal_test test;

	string path = "./cereal_test.out";

	{
		ofstream os( path, ios::binary );
  		cereal::PortableBinaryOutputArchive outarch ( os );
  		outarch ( test );
  		os.close();

		ifstream is( path, ios::binary );
  		cereal::PortableBinaryInputArchive inarch ( is );
  		inarch ( test );
  		is.close();
	}

	EXPECT_EQ( 1, test.itest );
	EXPECT_EQ( string("test"), test.stest );
	EXPECT_EQ( -1.0, test.dtest );

}


TEST( cerealtest, field ) {

	field test;

	string path = "./cereal_test_field.out";

	{
		ofstream os( path, ios::binary );
  		cereal::PortableBinaryOutputArchive outarch ( os );
  		outarch ( test );
  		os.close();

		ifstream is( path, ios::binary );
  		cereal::PortableBinaryInputArchive inarch ( is );
  		inarch ( test );
  		is.close();
	}

	EXPECT_EQ( data_type::none, test.type );
	EXPECT_EQ( string(""), test.name );
	EXPECT_EQ( string(""), test.units );

}

