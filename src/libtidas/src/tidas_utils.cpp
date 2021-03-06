
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>

#include <limits>
#include <cstdlib>
#include <cstdio>

extern "C" {
    #include <ftw.h>
    #include <unistd.h>
    #include <sys/stat.h>
    #include <sys/types.h>
    #include <limits.h>
    #include <libgen.h>
}

using namespace std;
using namespace tidas;


tidas::exception::exception(const char * msg, const char * file,
                            int line) : std::exception() {
    snprintf(msg_, msg_len_, "Exeption at line %d of file %s:  %s", line, file, msg);
    return;
}

tidas::exception::~exception() throw() {
    return;
}

const char * tidas::exception::what() const throw() {
    return msg_;
}

char ** tidas::c_string_alloc(size_t nstring, size_t length) {
    // Allocate a contiguous buffer of chars, then set up pointers
    // into that memory.

    char * raw = (char *)malloc(nstring * length * sizeof(char));
    if (!raw) {
        std::ostringstream o;
        o << "failed to allocate buffer of " << nstring << " C strings";
        TIDAS_THROW(o.str().c_str());
    }
    memset((void *)raw, 0, nstring * length * sizeof(char));

    char ** ret = (char **)malloc(nstring * sizeof(char *));
    if (!ret) {
        std::ostringstream o;
        o << "failed to allocate array of " << nstring << " char *";
        TIDAS_THROW(o.str().c_str());
    }

    for (size_t i = 0; i < nstring; ++i) {
        ret[i] = &(raw[i * backend_string_size]);
        strcpy(ret[i], "");
    }

    return ret;
}

void tidas::c_string_free(size_t nstring, char ** str) {
    if (str != NULL) {
        // First, get the memory address of the underlying buffer.
        char * raw = str[0];

        // Now free the list of pointers
        free(str);

        // Then free the raw buffer
        free(raw);
    }
    return;
}

int64_t tidas::fs_stat(char const * path) {
    int64_t size = -1;
    struct stat filestat;
    int ret;

    ret = ::stat(path, &filestat);

    if (ret == 0) {
        /* we found the file, get props */
        size = static_cast <int64_t> (filestat.st_size);
    }
    return size;
}

void tidas::fs_rm(char const * path) {
    int ret;
    int64_t size;

    size = fs_stat(path);
    if (size >= 0) {
        ret = ::remove(path);
        if (ret) {
            ::perror(path);
        }
    }

    return;
}

int tidas_fs_rm_r_callback(char const * fpath, const struct stat * sb, int typeflag,
                           struct FTW * ftwbuf) {
    int ret = ::remove(fpath);

    if (ret) {
        ::perror(fpath);
    }

    return ret;
}

void tidas::fs_rm_r(char const * path) {
    int64_t size = fs_stat(path);

    if (size >= 0) {
        int ret = ::nftw(path, tidas_fs_rm_r_callback, 64, FTW_DEPTH | FTW_PHYS);
        if (ret) {
            ::perror(path);
        }
    }

    return;
}

void tidas::fs_mkdir(char const * path) {
    int ret;
    int64_t size;

    size = fs_stat(path);

    if (size < 0) {
        ret = ::mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (ret) {
            ::perror(path);
        }
    }

    return;
}

void tidas::fs_link(char const * target, char const * path, bool hard) {
    int ret;

    if (hard) {
        ret = ::link(target, path);
    } else {
        ret = ::symlink(target, path);
    }

    if (ret) {
        ::perror(path);
    }

    return;
}

int tidas_fs_link_r_callback(char const * fpath, const struct stat * sb, int typeflag,
                             struct FTW * ftwbuf) {
    // FIXME:  probably have to do this manually, in order to maintain state...

    /*
       string target_root = ftwbuf->base;

       static string link_root;
       bool hard;

       // check for permissions errors

       if ( ( typeflag & FTW_DNR ) || ( typeflag & FTW_NS ) ) {
        cerr << "WARNING:  permissions / access error on \"" << fpath << "\" prevents
           linking" << endl;
        return 1;
       } else {

        // construct path relative to root

        string path = fpath;
        string relpath = path.substr ( target_root.size() );
        string linkpath = link_root + relpath;

        if ( typeflag | FTW_D ) {
            // we have a directory, in that case, just make the directory
            fs_mkdir ( linkpath.c_str() );
        } else if ( typeflag | FTW_SL ) {
            // we have a symlink, just copy it
            ostringstream com;
            com << "cp -a " << path << " " << linkpath;
            int sret = ::system( com.str().c_str() );
        } else {
            // make a link
            fs_link ( path.c_str(), linkpath.c_str(), hard );
        }

       }
     */

    return 0;
}

void tidas::fs_link_r(char const * target, char const * path, bool hard) {
    int ret = ::nftw(target, tidas_fs_link_r_callback, 64, FTW_PHYS);

    if (ret) {
        ::perror(target);
    }
    return;
}

// NOTE: this function only works if the relative path given to realpath()
// already exists!

std::string tidas::fs_fullpath(char const * relpath) {
    char * ptr;
    string ret = "";
    size_t max = 10000;

    #ifdef PATH_MAX

    // use this constant from the C header
    max = PATH_MAX;
    #endif // ifdef PATH_MAX

    char path[max + 1];
    ptr = realpath(relpath, path);
    ret = path;

    return ret;
}

data_type tidas::data_type_get(type_info const & test) {
    data_type ret = data_type::none;

    int8_t type_int8;
    uint8_t type_uint8;
    int16_t type_int16;
    uint16_t type_uint16;
    int32_t type_int32;
    uint32_t type_uint32;
    int64_t type_int64;
    uint64_t type_uint64;
    float type_float;
    double type_double;
    char * type_string;
    char const * type_cstring;

    if (test == typeid(type_int8)) {
        ret = data_type::int8;
    } else if (test == typeid(type_uint8)) {
        ret = data_type::uint8;
    } else if (test == typeid(type_int16)) {
        ret = data_type::int16;
    } else if (test == typeid(type_uint16)) {
        ret = data_type::uint16;
    } else if (test == typeid(type_int32)) {
        ret = data_type::int32;
    } else if (test == typeid(type_uint32)) {
        ret = data_type::uint32;
    } else if (test == typeid(type_int64)) {
        ret = data_type::int64;
    } else if (test == typeid(type_uint64)) {
        ret = data_type::uint64;
    } else if (test == typeid(type_float)) {
        ret = data_type::float32;
    } else if (test == typeid(type_double)) {
        ret = data_type::float64;
    } else if (test == typeid(type_string)) {
        ret = data_type::string;
    } else if (test == typeid(type_cstring)) {
        ret = data_type::string;
    } else {
        ret = data_type::none;
    }

    return ret;
}

string tidas::data_type_to_string(data_type type) {
    string ret;
    switch (type) {
        case data_type::int8:
            ret = "int8";
            break;

        case data_type::uint8:
            ret = "uint8";
            break;

        case data_type::int16:
            ret = "int16";
            break;

        case data_type::uint16:
            ret = "uint16";
            break;

        case data_type::int32:
            ret = "int32";
            break;

        case data_type::uint32:
            ret = "uint32";
            break;

        case data_type::int64:
            ret = "int64";
            break;

        case data_type::uint64:
            ret = "uint64";
            break;

        case data_type::float32:
            ret = "float32";
            break;

        case data_type::float64:
            ret = "float64";
            break;

        case data_type::string:
            ret = "string";
            break;

        case data_type::none:
            ret = "none";
            break;

        default:
            TIDAS_THROW("data type not recognized");
            break;
    }
    return ret;
}

data_type tidas::data_type_from_string(string const & name) {
    data_type ret;

    if (name.compare("int8") == 0) {
        ret = data_type::int8;
    } else if (name.compare("uint8") == 0) {
        ret = data_type::uint8;
    } else if (name.compare("int16") == 0) {
        ret = data_type::int16;
    } else if (name.compare("uint16") == 0) {
        ret = data_type::uint16;
    } else if (name.compare("int32") == 0) {
        ret = data_type::int32;
    } else if (name.compare("uint32") == 0) {
        ret = data_type::uint32;
    } else if (name.compare("int64") == 0) {
        ret = data_type::int64;
    } else if (name.compare("uint64") == 0) {
        ret = data_type::uint64;
    } else if (name.compare("float32") == 0) {
        ret = data_type::float32;
    } else if (name.compare("float64") == 0) {
        ret = data_type::float64;
    } else if (name.compare("string") == 0) {
        ret = data_type::string;
    } else if (name.compare("none") == 0) {
        ret = data_type::none;
    } else {
        ostringstream o;
        o << "data type \"" << name << "\" not recognized";
        TIDAS_THROW(o.str().c_str());
    }
    return ret;
}

template <>
std::string tidas::data_from_string <std::string> (std::string const & str) {
    return std::string(str);
}

std::string tidas::data_to_string(double const & val) {
    std::ostringstream o;
    o.precision(18);
    o.str("");
    if (!(o << std::scientific << val)) {
        TIDAS_THROW("cannot convert double data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(float const & val) {
    std::ostringstream o;
    o.precision(8);
    o.str("");
    if (!(o << std::scientific << val)) {
        TIDAS_THROW("cannot convert float data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(int64_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert int64 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(uint64_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert uint64 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(int32_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert int32 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(uint32_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert uint32 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(int16_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert int16 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(uint16_t const & val) {
    std::ostringstream o;
    o.str("");
    if (!(o << val)) {
        TIDAS_THROW("cannot convert uint16 data to string");
    }
    return o.str();
}

std::string tidas::data_to_string(std::string const & val) {
    return std::string(val);
}

std::string tidas::data_to_string(int8_t const & val) {
    return tidas::data_to_string(static_cast <int16_t> (val));
}

std::string tidas::data_to_string(uint8_t const & val) {
    return tidas::data_to_string(static_cast <uint16_t> (val));
}

string tidas::filter_default(string const & filter) {
    string ret = filter;
    if (ret == "") {
        ret = ".*";
    }
    return ret;
}

// "/2012.*[grp=.*[schm=field.*,dict=prop.*],intr=base.*[dict=pr.*]]/13.*[grp=.*[schm=field.*,dict=prop.*],intr=base.*]/.*"
//
// block name match: "2012.*""
// block filter pass:
// "[grp=.*[schm=field.*,dict=prop.*],intr=base.*[dict=pr.*]]/13.*[grp=.*[schm=field.*,dict=prop.*],intr=base.*]/.*"
//   group name match: ".*"
//   group filter pass: "[schm=field.*,dict=prop.*]"
//   interval name match: "base.*"
//   interval filter pass: "[dict=pr.*]"
//   block name match: "13.*"
//   block filter pass: "[grp=.*[schm=field.*,dict=prop.*],intr=base.*]/.*"
//     group name match: ".*"
//     group filter pass: "[schm=field.*,dict=prop.*]"
//     interval name match: "base.*"
//     interval filter pass: ""
//     block name match: ".*"
//     block filter pass: ""


// This extracts the name and sub-filter:  XXXX/.... OR XXXX[]/.... OR XXXX,....

void tidas::filter_sub(string const & filter, string & name, string & subfilter) {
    name = "";
    subfilter = "";

    size_t subpos = filter.find(submatch_begin, 0);
    size_t pathpos = filter.find(path_sep, 0);
    size_t seppos = filter.find(submatch_sep, 0);

    if ((subpos == string::npos) && (pathpos == string::npos) &&
        (seppos == string::npos)) {
        // we have just a name match
        name = filter;
    } else {
        // find the first occurrence of any character that we want to split on

        size_t pos = std::numeric_limits <size_t>::max();
        if (subpos != string::npos) {
            if (subpos < pos) {
                pos = subpos;
            }
        }
        if (seppos != string::npos) {
            if (seppos < pos) {
                pos = seppos;
            }
        }
        if (pathpos != string::npos) {
            if (pathpos < pos) {
                pos = pathpos;
            }
        }

        name = filter.substr(0, pos);
        subfilter = filter.substr(pos, filter.size() - pos);
    }

    return;
}

// This splits filter of the form XXXX/XXXX

void tidas::filter_block(string const & filter, string & local, string & subfilter,
                         bool & stop) {
    local = "";
    subfilter = "";
    stop = false;

    size_t off = 0;

    size_t pos = filter.find(path_sep, off);

    if (pos != string::npos) {
        local = filter.substr(off, (pos - off));

        if (pos == filter.size() - 1) {
            stop = true;
        } else {
            subfilter = filter.substr((pos + 1), (filter.size() - pos));
        }
    } else {
        // we just have a local filter
        local = filter;
    }

    return;
}

// This splits a filter of form [XXX=XXX,XXX=XXX[XX=XX[X=X,X=X],XX=XX],XXX=XXX]
// into XXX : XXX, XXX : XXX[XX=XX[X=X,X=X],XX=XX], XXX : XXX

map <string, string> tidas::filter_split(string const & filter) {
    map <string, string> ret;

    if (filter != "") {
        // check enclosing chars

        if (filter.compare(0, 1, submatch_begin) != 0) {
            ostringstream o;
            o << "filter split string \"" << filter <<
                "\" does not start with correct character (" << submatch_begin << ")";
            TIDAS_THROW(o.str().c_str());
        }

        if (filter.compare((filter.size() - 1), 1, submatch_end) != 0) {
            ostringstream o;
            o << "filter split string \"" << filter <<
                "\" does not end with correct character (" << submatch_end << ")";
            TIDAS_THROW(o.str().c_str());
        }

        // search through string and build up sub filters

        size_t off = 0;

        size_t pos;
        size_t end;

        string keyval;
        string key;
        string val;

        size_t assign;

        while (off < (filter.size() - 1)) {
            ++off;

            size_t pos = filter.find(submatch_sep, off);

            if (pos == string::npos) {
                end = filter.size() - 1;
            } else {
                // we might have more than one key/value left.
                // do we have a nested filter ahead?

                size_t substart = filter.find(submatch_begin, off);

                if ((substart != string::npos) && (substart < pos)) {
                    // ok, we do, now find the separator we are looking for

                    size_t nest = 1;
                    size_t cur = substart + 1;
                    bool done = false;

                    while (!done) {
                        size_t nextstart = filter.find(submatch_begin, cur);
                        size_t nextstop = filter.find(submatch_end, cur);
                        if (nextstop == string::npos) {
                            ostringstream o;
                            o << "filter split string \"" << filter <<
                                "\" contains unmatched grouping";
                            TIDAS_THROW(o.str().c_str());
                        }
                        if ((nextstart != string::npos) && (nextstart < nextstop)) {
                            // we are descending
                            ++nest;
                            cur = nextstart + 1;
                        } else {
                            --nest;
                            cur = nextstop + 1;
                        }
                        if (nest == 0) {
                            done = true;
                        }
                    }

                    end = cur;
                } else {
                    end = pos;
                }
            }

            // extract the entry

            keyval = filter.substr(off, (end - off));

            assign = keyval.find(submatch_assign);

            if (assign == string::npos) {
                ostringstream o;
                o << "filter string sub-match must include the \"" << submatch_assign <<
                    "\" character";
                TIDAS_THROW(o.str().c_str());
            }

            key = keyval.substr(0, assign);
            val = keyval.substr((assign + 1), (keyval.size() - assign));

            ret[key] = val;

            off = end;
        }
    }
    return ret;
}

tidas::timer::timer() {
    start_ = std::chrono::high_resolution_clock::now();
    stop_ = start_;
    running_ = false;
}

void tidas::timer::start() {
    if (!running_) {
        start_ = std::chrono::high_resolution_clock::now();
        running_ = true;
    }
    return;
}

void tidas::timer::stop() {
    if (running_) {
        stop_ = std::chrono::high_resolution_clock::now();
        running_ = false;
    }
    return;
}

double tidas::timer::seconds() {
    if (running_) {
        std::cerr << "Timer is still running!" << std::endl;
        return -1.0;
    }
    std::chrono::duration <double> elapsed =
        std::chrono::duration_cast <std::chrono::duration <double> >
            (stop_ - start_);
    return elapsed.count();
}

void tidas::timer::report(char const * message) {
    double t = seconds();
    fprintf(stdout, "%s:  %.2f seconds\n", message, t);
    fflush(stdout);
    return;
}
