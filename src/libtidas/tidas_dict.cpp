/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


tidas::dict::dict () {

}


tidas::dict::~dict () {

}


std::string tidas::dict::get_string ( std::string const & key ) const {
	return data_.at( key );
}


double tidas::dict::get_double ( std::string const & key ) const {
	return stod ( data_.at( key ) );
}


int tidas::dict::get_int ( std::string const & key ) const {
	return stoi ( data_.at( key ) );
}


long long tidas::dict::get_ll ( std::string const & key ) const {
	return stoll ( data_.at( key ) );
}


