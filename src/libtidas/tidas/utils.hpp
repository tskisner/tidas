/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#ifndef TIDAS_UTILS_HPP
#define TIDAS_UTILS_HPP


namespace tidas {

	// Exception handling

	class exception : public std::exception {

		public:
			
			exception ( char const * msg, char const * file, int line );
			~exception ( ) throw ();
			char const * what() const throw();

		private:

			// use C strings here for passing to what()
			static size_t const msg_len_ = 1024; 
			char msg_[ msg_len_ ];
	  
	};  

	typedef void (*TIDAS_EXCEPTION_HANDLER) ( tidas::exception & e );

	#define TIDAS_THROW(msg) \
	throw tidas::exception ( msg, __FILE__, __LINE__ )

	#define TIDAS_TRY \
	try {

	#define TIDAS_CATCH \
	} catch ( tidas::exception & e ) { \
	std::cerr << e.what() << std::endl; \
	throw; \
	}

	#define TIDAS_CATCH_CUSTOM(handler) \
	} catch ( tidas::exception & e ) { \
	(*handler) ( e ); \
	}


	// simple contiguous buffer allocation

	template < class T >
	T * mem_alloc ( size_t n ) {
		size_t size = n * sizeof(T);
		void * temp = ::malloc ( size );
		if ( ! temp ) {
			std::ostringstream o;
			o << "cannot allocate mem buffer of size " << size;
			TIDAS_THROW( o.str().c_str() );
		}
		return (T*)temp;
	}


	// common file operations

	int64_t fs_stat ( char const * path );

	void fs_rm ( char const * path );

	void fs_mkdir ( char const * path );


	// general regular expression filters

	std::string filter_default ( std::string const & filter );

	std::map < std::string, std::string > filter_split ( std::string const & filter );


}


#endif
