/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#ifndef TIDAS_INDEX_HPP
#define TIDAS_INDEX_HPP

#include <tidas/sqlite3.h>


namespace tidas {

	// class for object index

	class indexdb {

		public :

			indexdb ();
			~indexdb ();
			indexdb & operator= ( indexdb const & other );

			indexdb ( indexdb const & other );

			indexdb ( std::string const & path );


		private :

			std::string path_;
			std::unique_ptr < sqlite3 > db_;

	};


}


#endif
