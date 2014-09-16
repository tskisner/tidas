/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_DICT_HPP
#define TIDAS_DICT_HPP


namespace tidas {

	// dictionary class

	class dict {

		public :

			dict ();
			~dict ();

			template < class T >
			void put ( std::string const & key, T const & val ) {
				std::ostringstream o;
				o.precision(18);
				o.str("");
				if ( ! ( o << val ) ) {
					TIDAS_THROW( "cannot convert type to string for dict storage" );
				}
				data_[ key ] = o.str();
				return;
			}

			std::string get_string ( std::string const & key ) const;

			double get_double ( std::string const & key ) const;

			int get_int ( std::string const & key ) const;

			long long get_ll ( std::string const & key ) const;

			void clear();

		private :

			std::map < std::string, std::string > data_;

	};


}


#endif
