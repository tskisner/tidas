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


	// simple memory allocation

	double * double_alloc ( size_t n );

	float * float_alloc ( size_t n );

	int * int_alloc ( size_t n );

	long * long_alloc ( size_t n );

	size_t * sizet_alloc ( size_t n );

	int8_t * int8_alloc ( size_t n );

	uint8_t * uint8_alloc ( size_t n );

	int16_t * int16_alloc ( size_t n );

	uint16_t * uint16_alloc ( size_t n );

	int32_t * int32_alloc ( size_t n );

	uint32_t * uint32_alloc ( size_t n );

	int64_t * int64_alloc ( size_t n );

	uint64_t * uint64_alloc ( size_t n );


	// common file operations

	int64_t fs_stat ( char const * path );

	void fs_rm ( char const * path );

	void fs_mkdir ( char const * path );

}


#endif
