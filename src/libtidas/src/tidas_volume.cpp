/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <fstream>

extern "C" {
    #include <dirent.h>
}

using namespace std;
using namespace tidas;


tidas::volume::volume () {
    index_setup();
}


tidas::volume::volume ( string const & path, backend_type type, compression_type comp, std::map < std::string, std::string > extra ) {

    // set up location

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
    }

    if ( fs_stat ( relpath.c_str() ) >= 0 ) {
        ostringstream o;
        o << "cannot create volume \"" << relpath << "\", a file or directory already exists";
        TIDAS_THROW( o.str().c_str() );
    }

    // We have to make the directory before we can get the absolute path.
    fs_mkdir ( relpath.c_str() );

    string fspath = fs_fullpath ( relpath.c_str() );

    loc_.path = fspath;
    loc_.name = "";
    loc_.meta = "";
    loc_.type = type;
    loc_.comp = comp;
    loc_.mode = access_mode::write;
    loc_.backparams = extra;

    // write properties

    write_props( loc_ );

    // open index

    index_setup();

    // relocate root block

    root_.relocate ( root_loc ( loc_ ) );

    // write root block

    root_.flush();

}


tidas::volume::~volume () {
}


volume & tidas::volume::operator= ( volume const & other ) {
    if ( this != &other ) {
        copy ( other, "", other.loc_ );
    }
    return *this;
}


tidas::volume::volume ( volume const & other ) {
    copy ( other, "", other.loc_ );
}


tidas::volume::volume ( string const & path, access_mode mode ) {

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
    }

    if ( fs_stat ( relpath.c_str() ) == 0 ) {
        ostringstream o;
        o << "cannot open volume \"" << relpath << "\", directory does not exist";
        TIDAS_THROW( o.str().c_str() );
    }

    string fspath = fs_fullpath ( relpath.c_str() );

    // read properties

    loc_.path = fspath;
    read_props( loc_ );

    loc_.path = fspath;
    loc_.name = "";
    loc_.meta = "";
    loc_.mode = mode;

    // open index

    index_setup();

    // relocate root block

    root_.relocate ( root_loc ( loc_ ) );

    // read root block

    root_.sync();

}


tidas::volume::volume ( volume const & other, std::string const & filter, backend_path const & loc ) {
    copy ( other, filter, loc );
}


void tidas::volume::index_setup () {

    // create index

    if ( loc_.path != "" ) {
        string indxpath = loc_.path + path_sep + volume_fs_index;
        db_.reset ( new indexdb_sql( indxpath, loc_.path, loc_.mode ) );
    } else {
        db_.reset ( new indexdb_mem() );
    }

    loc_.idx = db_;

    return;
}


void tidas::volume::copy ( volume const & other, string const & filter, backend_path const & loc ) {

    string filt = filter_default ( filter );

    if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
        TIDAS_THROW( "copy of non-memory volume with a filter requires a new location" );
    }

    loc_ = loc;

    if ( loc_ != other.loc_ ) {

        if ( loc_.mode != access_mode::write ) {
            TIDAS_THROW( "cannot copy volume to read-only location" );
        }

        if ( loc_.path != "" ) {

            if ( fs_stat ( loc_.path.c_str() ) >= 0 ) {
                ostringstream o;
                o << "cannot create volume \"" << loc_.path << "\", a file or directory already exists";
                TIDAS_THROW( o.str().c_str() );
            }

            string relpath = loc_.path;
            if ( relpath[ relpath.size() - 1 ] == '/' ) {
                relpath[ relpath.size() - 1 ] = '\0';
            }

            // We have to make the directory before we can get the absolute path.
            fs_mkdir ( relpath.c_str() );

            string fspath = fs_fullpath ( relpath.c_str() );
            loc_.path = fspath;

            // write properties

            write_props( loc_ );

        }

        // open index

        index_setup();

    }

    // copy root

    root_.copy ( other.root_, filter, root_loc ( loc_ ) );

    if ( loc_ != other.loc_ ) {
        // write root block
        root_.flush();
    }

    return;
}


void tidas::volume::link ( std::string const & path, link_type const & ltype, std::string const & filter ) const {

    string lpath = path;

    if ( lpath[ loc_.path.size() - 1 ] == '/' ) {
        lpath[ loc_.path.size() - 1 ] = '\0';
    }

    if ( ltype == link_type::none ) {
        TIDAS_THROW( "you must specify a link type" );
    }

    if ( loc_.type == backend_type::none ) {
        TIDAS_THROW( "you cannot link a volume which has no backend assigned" );
    }

    bool hard = ( ltype == link_type::hard );

    // make the top level directory

    if ( fs_stat ( lpath.c_str() ) >= 0 ) {
        ostringstream o;
        o << "cannot create volume link \"" << lpath << "\", a file or directory already exists";
        TIDAS_THROW( o.str().c_str() );
    }

    fs_mkdir ( lpath.c_str() );

    // link properties file

    string propfile = loc_.path + path_sep + volume_fs_props;
    string newpropfile = lpath + path_sep + volume_fs_props;

    fs_link ( propfile.c_str(), newpropfile.c_str(), hard );

    string rootpath = lpath + path_sep + volume_fs_data_dir;

    root_.link ( ltype, rootpath );

    // make a copy of the index, so that we can add new objects

    string newdb = lpath + path_sep + volume_fs_index;
    indexdb_sql sqldb ( newdb, lpath, access_mode::write );

    if ( loc_.path != "" ) {
        indexdb_sql * orig = dynamic_cast < indexdb_sql * > ( db_.get() );
        backend_path rootloc = root_loc ( loc_ );
        std::deque < indexdb_transaction > result;
        orig->tree( rootloc, "", result );
        sqldb.commit ( result );
    } else {
        indexdb_mem * orig = dynamic_cast < indexdb_mem * > ( db_.get() );
        sqldb.commit ( orig->history() );
    }

    return;
}


void tidas::volume::duplicate ( std::string const & path, backend_type type, compression_type comp, std::string const & filter, std::map < std::string, std::string > extra ) const {

    // construct new volume and copy

    backend_path exploc;

    exploc.path = path;
    exploc.name = "";
    exploc.meta = "";
    exploc.type = type;
    exploc.comp = comp;
    exploc.mode = access_mode::write;
    exploc.backparams = extra;

    volume newvol;
    newvol.copy ( (*this), filter, exploc );

    data_copy ( (*this), newvol );

    return;
}


void tidas::volume::wipe ( string const & filter ) const {

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {


        } else {
            TIDAS_THROW( "cannot wipe volume in read-only mode" );
        }
    }

    return;
}


backend_path tidas::volume::location () const {
    return loc_;
}


backend_path tidas::volume::root_loc ( backend_path const & loc ) const {
    backend_path ret = loc;

    ret.name = volume_fs_data_dir;

    return ret;
}


block & tidas::volume::root () {
    return root_;
}


block const & tidas::volume::root () const {
    return root_;
}


void tidas::volume::read_props ( backend_path & loc ) {

    string propfile = loc.path + path_sep + volume_fs_props;

    {
        ifstream props ( propfile );
        cereal::XMLInputArchive inarch ( props );

        backend_path tmploc;
        inarch ( tmploc );
        loc = tmploc;
    }

    return;
}


void tidas::volume::write_props ( backend_path const & loc ) const {

    // make directory for the volume

    fs_mkdir ( loc.path.c_str() );

    // write properties to file

    string propfile = loc.path + path_sep + volume_fs_props;

    {
        ofstream props ( propfile );
        cereal::XMLOutputArchive outarch ( props );
        backend_path tmploc = loc;
        outarch ( cereal::make_nvp( "volume", tmploc ) );
    }

    return;
}


void tidas::volume::info ( std::ostream & out ) const {
    std::ostringstream ind;
    ind.str("");
    ind << "TIDAS:  ";
    out << ind.str() << "Volume \"" << loc_.path << "\"" << std::endl;
    root_.info(out, 2, true);
    return;
}
