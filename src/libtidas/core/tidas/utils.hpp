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


    // simple, C-style contiguous buffer allocation

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

    // array of fixed-length C strings

    char ** c_string_alloc ( size_t nstring, size_t length );

    void c_string_free ( size_t nstring, char ** str );


    // common file operations

    ///
    /// Return the size of the specified file.
    ///
    int64_t fs_stat ( char const * path );

    ///
    /// Remove the specified file.
    ///
    void fs_rm ( char const * path );

    ///
    /// Recursively remove directories
    ///
    void fs_rm_r ( char const * path );

    /// Make the specified directory.
    void fs_mkdir ( char const * path );

    /// Make a link
    void fs_link ( char const * target, char const * path, bool hard = false );

    /// Recursively make directories and link files
    void fs_link_r ( char const * target, char const * path, bool hard );

    /// Get the full path of the specified file
    std::string fs_fullpath ( char const * relpath );


    // data types

    /// Return the data type based on the C++ typeid.
    data_type data_type_get ( std::type_info const & test );

    /// Return the string name of the data type.
    std::string data_type_to_string ( data_type type );

    /// Return the data type based on the string name.
    data_type data_type_from_string ( std::string const & name );

    /// Convert a string into the specified type.
    template < typename T >
    T data_convert ( std::string const & str ) {
        T ret;

        int8_t int8;
        uint8_t uint8;
        int16_t int16;
        uint16_t uint16;
        int32_t int32;
        uint32_t uint32;
        int64_t int64;
        uint64_t uint64;
        float f32;
        double f64;

        if ( typeid ( ret ) == typeid ( int8 ) ) {
            ret = static_cast < T > ( str[0] );
        } else if ( typeid ( ret ) == typeid ( uint8 ) ) {
            ret = static_cast < T > ( str[0] );
        } else if ( typeid ( ret ) == typeid ( int16 ) ) {
            ret = static_cast < T > ( stol ( str ) );
        } else if ( typeid ( ret ) == typeid ( uint16 ) ) {
            ret = static_cast < T > ( stoul ( str ) );
        } else if ( typeid ( ret ) == typeid ( int32 ) ) {
            ret = static_cast < T > ( stol ( str ) );
        } else if ( typeid ( ret ) == typeid ( uint32 ) ) {
            ret = static_cast < T > ( stoul ( str ) );
        } else if ( typeid ( ret ) == typeid ( int64 ) ) {
            ret = static_cast < T > ( stoll ( str ) );
        } else if ( typeid ( ret ) == typeid ( uint64 ) ) {
            ret = static_cast < T > ( stoull ( str ) );
        } else if ( typeid ( ret ) == typeid ( f32 ) ) {
            ret = stof ( str );
        } else if ( typeid ( ret ) == typeid ( f64 ) ) {
            ret = stod ( str );
        } else {
            TIDAS_THROW( "Unknown data type" );
        }
        return ret;
    }

    template <>
    std::string data_convert < std::string > ( std::string const & str );


    // general regular expression filters

    std::string filter_default ( std::string const & filter );

    void filter_sub ( std::string const & filter, std::string & root, std::string & subfilter );

    void filter_block ( std::string const & filter, std::string & local, std::string & subfilter, bool & stop );

    std::map < std::string, std::string > filter_split ( std::string const & filter );


}


#endif
