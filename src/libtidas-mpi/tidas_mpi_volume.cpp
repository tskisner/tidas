/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_mpi_internal.hpp>

#include <fstream>

extern "C" {
    #include <dirent.h>
}

using namespace std;
using namespace tidas;


tidas::mpi_volume::mpi_volume () {
    comm_ = MPI_COMM_WORLD;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );
    index_setup();
}


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, string const & path, backend_type type,
    compression_type comp, std::map < std::string, std::string > extra ) {

    comm_ = MPI_COMM_WORLD;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    // set up location

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
    }

    string fspath = fs_fullpath ( relpath.c_str() );

    loc_.path = fspath;
    loc_.name = "";
    loc_.meta = "";
    loc_.type = type;
    loc_.comp = comp;
    loc_.mode = access_mode::write;
    loc_.backparams = extra;

    if ( fs_stat ( fspath.c_str() ) >= 0 ) {
        ostringstream o;
        o << "cannot create mpi_volume \"" << fspath << "\", a file or directory already exists";
        TIDAS_THROW( o.str().c_str() );
    }

    // write properties

    write_props( loc_ );

    // open index

    index_setup();

    // relocate root block

    root_.relocate ( root_loc ( loc_ ) );

    // write root block

    root_.flush();

}


tidas::mpi_volume::~mpi_volume () {
    meta_sync();
}


mpi_volume & tidas::mpi_volume::operator= ( mpi_volume const & other ) {
    if ( this != &other ) {
        copy ( other, "", other.loc_ );
    }
    return *this;
}


tidas::mpi_volume::mpi_volume ( mpi_volume const & other ) {
    copy ( other, "", other.loc_ );
}


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, string const & path, 
    access_mode mode, std::string const & dist ) {

    // FIXME: eventually the dist string will be used to distribute
    // metadata.

    comm_ = MPI_COMM_WORLD;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
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


tidas::mpi_volume::mpi_volume ( mpi_volume const & other, std::string const & filter, backend_path const & loc ) {
    copy ( other, filter, loc );
}


MPI_Comm tidas::mpi_volume::comm ( ) const {
    return comm_;
}


void tidas::mpi_volume::meta_sync ( ) {

    // all processes take turns replaying their index transactions
    // to the master DB.

    int ret;

    std::deque < indexdb_transaction > hist;

    for ( int p = 0; p < nproc_; ++p ) {
        if ( rank_ == 0 ) {
            if ( p == 0 ) {
                // replay our own transactions
                hist = localdb_->history();
            } else {
                // receive transactions from this process
                mpi_recv ( comm_, hist, p, p );
            }
            masterdb_->commit ( hist );
        } else {
            if ( rank_ == p ) {
                // send our transactions
                hist = localdb_->history();
                mpi_send ( comm_, hist, 0, p );
            }
        }
        ret = MPI_Barrier ( comm_ );
    }
    localdb_->history_clear();

    return;
}


void tidas::mpi_volume::index_setup () {

    // create index

    if ( rank_ == 0 ) {
        if ( loc_.path != "" ) {
            string indxpath = loc_.path + path_sep + volume_fs_index;
            masterdb_.reset ( new indexdb_sql( indxpath, loc_.path, loc_.mode ) );
        } else {
            masterdb_.reset();
        }
    }

    localdb_.reset ( new indexdb_mem() );

    loc_.idx = localdb_;

    return;
}


void tidas::mpi_volume::copy ( mpi_volume const & other, string const & filter, backend_path const & loc ) {

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
                o << "cannot create mpi_volume \"" << loc_.path << "\", a file or directory already exists";
                TIDAS_THROW( o.str().c_str() );
            }

            // write properties

            write_props( loc_ );

        }

        // open index

        index_setup();

    }

    // copy root

    root_.copy ( other.root_, filter, root_loc ( loc_ ) );

    if ( loc_ != other.loc_ ) {
        if ( rank_ == 0 ) {
            // write root block
            root_.flush();
        }
    }

    return;
}


void tidas::mpi_volume::link ( std::string const & path, link_type const & ltype, std::string const & filter ) const {

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

    if ( rank_ == 0 ) {

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
            indexdb_sql * orig = dynamic_cast < indexdb_sql * > ( masterdb_.get() );
            backend_path rootloc = root_loc ( loc_ );
            std::deque < indexdb_transaction > result;
            orig->tree( rootloc, "", result );
            sqldb.commit ( result );
        } else {
            indexdb_mem * orig = dynamic_cast < indexdb_mem * > ( masterdb_.get() );
            sqldb.commit ( orig->history() );
        }
    }

    return;
}


void tidas::mpi_volume::duplicate ( std::string const & path, backend_type type, compression_type comp, std::string const & filter, std::map < std::string, std::string > extra ) const {

    if ( fs_stat ( path.c_str() ) >= 0 ) {
        ostringstream o;
        o << "cannot export volume to \"" << path << "\", which already exists";
        TIDAS_THROW( o.str().c_str() );
    }

    // construct new volume

    backend_path exploc;

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
    }

    string fspath = fs_fullpath ( relpath.c_str() );

    exploc.path = fspath;
    exploc.name = "";
    exploc.meta = "";
    exploc.type = type;
    exploc.comp = comp;
    exploc.mode = access_mode::write;
    exploc.backparams = extra;

    mpi_volume newvol;
    newvol.copy ( (*this), filter, exploc );

    data_copy ( (*this), newvol );

    return;
}


void tidas::mpi_volume::wipe ( string const & filter ) const {

    if ( loc_.type != backend_type::none ) {

        if ( loc_.mode == access_mode::write ) {


        } else {
            TIDAS_THROW( "cannot wipe volume in read-only mode" );
        }
    }

    return;
}


backend_path tidas::mpi_volume::location () const {
    return loc_;
}


backend_path tidas::mpi_volume::root_loc ( backend_path const & loc ) const {
    backend_path ret = loc;

    ret.name = volume_fs_data_dir;

    return ret;
}


block & tidas::mpi_volume::root () {
    return root_;
}


block const & tidas::mpi_volume::root () const {
    return root_;
}


void tidas::mpi_volume::read_props ( backend_path & loc ) {

    if ( rank_ == 0 ) {

        string propfile = loc.path + path_sep + volume_fs_props;

        {
            ifstream props ( propfile );
            cereal::XMLInputArchive inarch ( props );

            backend_path tmploc;
            inarch ( tmploc );
            loc = tmploc;
        }

    }

    mpi_bcast ( comm_, loc, 0 );

    return;
}

            
void tidas::mpi_volume::write_props ( backend_path const & loc ) const {

    if ( rank_ == 0 ) {

        // make directory for the mpi_volume

        fs_mkdir ( loc.path.c_str() );

        // write properties to file

        string propfile = loc.path + path_sep + volume_fs_props;

        {
            ofstream props ( propfile );
            cereal::XMLOutputArchive outarch ( props );
            backend_path tmploc = loc;
            outarch ( cereal::make_nvp( "volume", tmploc ) );
        }

    }

    int ret = MPI_Barrier ( comm_ );

    return;
}


void tidas::data_copy ( mpi_volume const & in, mpi_volume & out ) {

    int rank;
    int nproc;
    MPI_Comm comm = in.comm();

    int ret = MPI_Comm_rank ( comm, &rank );
    ret = MPI_Comm_size ( comm, &nproc );

    // just copy the root block

    if ( rank == 0 ) {
        data_copy ( in.root(), out.root() );
    }

    return;
}

