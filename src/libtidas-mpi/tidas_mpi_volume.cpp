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

    comm_ = comm;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    // set up location

    string fspath;

    if ( rank_ == 0 ) {
        if ( fs_stat ( path.c_str() ) >= 0 ) {
            ostringstream o;
            o << "cannot create volume \"" << path << "\", which already exists";
            TIDAS_THROW( o.str().c_str() );
        }
        string relpath = path;
        if ( relpath[ relpath.size() - 1 ] == '/' ) {
            relpath[ relpath.size() - 1 ] = '\0';
        }

        // We have to make the directory before we can get the absolute path.
        fs_mkdir ( relpath.c_str() );

        fspath = fs_fullpath ( relpath.c_str() );
    }

    mpi_bcast ( comm_, fspath, 0 );

    std::cout << "DBG: mpi_volume create at " << fspath << std::endl;

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

    // flush root block
    if ( rank_ == 0 ) {
        loc_.idx = masterdb_;
        root_.relocate ( root_loc ( loc_ ) );
        root_.flush();
        loc_.idx = localdb_;
        root_.relocate ( root_loc ( loc_ ) );
    }

    // collectively get objects
    open();
}


tidas::mpi_volume::~mpi_volume () {
    close();
}


mpi_volume & tidas::mpi_volume::operator= ( mpi_volume const & other ) {
    if ( this != &other ) {
        copy ( other.comm(), other, "", other.loc_ );
    }
    return *this;
}


tidas::mpi_volume::mpi_volume ( mpi_volume const & other ) {
    copy ( other.comm(), other, "", other.loc_ );
}


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, string const & path, 
    access_mode mode, std::string const & dist ) {

    // FIXME: eventually the dist string will be used to distribute
    // metadata.

    comm_ = comm;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    string fspath;

    if ( rank_ == 0 ) {
        if ( fs_stat ( path.c_str() ) == 0 ) {
            ostringstream o;
            o << "cannot open volume \"" << path << "\", which does not exist";
            TIDAS_THROW( o.str().c_str() );
        }
        string relpath = path;
        if ( relpath[ relpath.size() - 1 ] == '/' ) {
            relpath[ relpath.size() - 1 ] = '\0';
        }

        fspath = fs_fullpath ( relpath.c_str() );
    }

    mpi_bcast ( comm_, fspath, 0 );

    std::cout << "DBG: mpi_volume open existing at " << fspath << std::endl;

    // read properties

    loc_.path = fspath;
    read_props( loc_ );

    loc_.path = fspath;
    loc_.name = "";
    loc_.meta = "";
    loc_.mode = mode;

    // open index
    index_setup();

    // collectively get objects
    open();
}


tidas::mpi_volume::mpi_volume ( mpi_volume const & other, std::string const & filter, backend_path const & loc ) {
    copy ( other.comm(), other, filter, loc );
}


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, mpi_volume const & other, std::string const & filter, backend_path const & loc ) {
    copy ( comm, other, filter, loc );
}


void tidas::mpi_volume::open ( ) {

    // Check that we have no outstanding transactions
    size_t histsize = localdb_->history().size();
    if ( histsize > 0 ) {
        ostringstream o;
        o << "process " << rank_ << " in mpi_volume has " << histsize << " outstanding transactions that would be lost during sync";
        TIDAS_THROW( o.str().c_str() );
    }

    // All processes clear their local in-memory DB
    localdb_.reset ( new indexdb_mem() );

    std::deque < indexdb_transaction > hist;

    // rank zero reads the master sql DB and replicates transactions to
    // local in-memory DB.
    if ( rank_ == 0 ) {
        // recursively set active DB to sql one.
        loc_.idx = masterdb_;
        root_.relocate ( root_loc ( loc_ ) );

        // recursively load objects and metadata from DB or filesystem
        root_.sync();

        // get transactions which replicate the full DB
        masterdb_->tree ( root_loc ( loc_ ), "", hist );
    }

    // broadcast all objects (does not send index)
    mpi_bcast ( comm_, root_, 0 );

    // broadcast transactions
    mpi_bcast ( comm_, hist, 0 );

    // replay transactions into local DB
    std::cout << "DBG: mpi_volume open replaying:" << std::endl;
    for ( auto const & h : hist ) {
        h.print ( std::cout );
    }

    localdb_->replay ( hist );

    // recursively set active DB to in-memory one
    loc_.idx = localdb_;
    root_.relocate ( root_loc ( loc_ ) );

    int ret = MPI_Barrier ( comm_ );

    return;
}


void tidas::mpi_volume::close ( ) {

    // First, ensure that all transactions in memory are gathered at the root
    // process.

    std::deque < indexdb_transaction > hist;
    hist = localdb_->history();
    localdb_->history_clear();

    for ( int p = 0; p < nproc_; ++p ) {
        if ( rank_ == p ) {
            std::cout << "DBG: mpi_volume close proc " << p << " local:" << std::endl;
            for ( auto const & h : hist ) {
                h.print ( std::cout );
            }
        }
        int blah = MPI_Barrier ( comm_ );
    }

    std::vector < std::deque < indexdb_transaction > > allhist;

    mpi_gather ( comm_, hist, allhist, 0 );

    // Root process replays to master DB

    if ( rank_ == 0 ) {
        for ( size_t i = 0; i < nproc_; ++i ) {
            std::cout << "DBG: mpi_volume close replaying from proc " << i << ":" << std::endl;
            for ( auto const & h : allhist[i] ) {
                h.print ( std::cout );
            }
            masterdb_->commit ( allhist[i] );
        }
    }

    int ret = MPI_Barrier ( comm_ );

    return;
}


void tidas::mpi_volume::meta_sync ( ) {
    close();
    open();
    return;
}


MPI_Comm tidas::mpi_volume::comm ( ) const {
    return comm_;
}


int tidas::mpi_volume::comm_rank ( ) const {
    return rank_;
}


int tidas::mpi_volume::comm_size ( ) const {
    return nproc_;
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


void tidas::mpi_volume::copy ( MPI_Comm comm, mpi_volume const & other, string const & filter, backend_path const & loc ) {

    // Check that we have no outstanding transactions
    size_t histsize = other.localdb_->history().size();
    if ( histsize > 0 ) {
        ostringstream o;
        o << "process " << rank_ << " in mpi_volume copy has " << histsize << " outstanding transactions";
        TIDAS_THROW( o.str().c_str() );
    }

    comm_ = comm;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    string filt = filter_default ( filter );

    if ( ( filt != ".*" ) && ( loc.type != backend_type::none ) && ( loc == other.loc_ ) ) {
        TIDAS_THROW( "copy of non-memory volume with a filter requires a new location" );
    }

    loc_ = loc;

    bool newloc = true;
    if ( loc_ == other.loc_ ) {
        newloc = false;
    }

    if ( newloc ) {

        if ( loc_.mode != access_mode::write ) {
            TIDAS_THROW( "cannot copy volume to read-only location" );
        }

        if ( loc_.path != "" ) {

            string fspath;

            if ( rank_ == 0 ) {
                if ( fs_stat ( loc_.path.c_str() ) >= 0 ) {
                    ostringstream o;
                    o << "cannot create mpi_volume \"" << loc_.path << "\", a file or directory already exists";
                    TIDAS_THROW( o.str().c_str() );
                }
                string relpath = loc_.path;
                if ( relpath[ relpath.size() - 1 ] == '/' ) {
                    relpath[ relpath.size() - 1 ] = '\0';
                }

                // We have to make the directory before we can get the absolute path.
                fs_mkdir ( relpath.c_str() );

                fspath = fs_fullpath ( relpath.c_str() );
            }

            mpi_bcast ( comm_, fspath, 0 );
            loc_.path = fspath;

            // write properties

            write_props( loc_ );

        }

        // open index

        index_setup();

    }

    // copy root

    root_.copy ( other.root_, filter, root_loc ( loc_ ) );

    if ( newloc ) {
        // flush root block
        if ( rank_ == 0 ) {
            loc_.idx = masterdb_;
            root_.relocate ( root_loc ( loc_ ) );
            root_.flush();
            loc_.idx = localdb_;
            root_.relocate ( root_loc ( loc_ ) );
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

    int ret = MPI_Barrier ( comm_ );

    return;
}


void tidas::mpi_volume::duplicate ( std::string const & path, backend_type type, compression_type comp, std::string const & filter, std::map < std::string, std::string > extra ) const {

    // construct new volume and copy

    backend_path exploc;

    exploc.path = path;
    exploc.name = "";
    exploc.meta = "";
    exploc.type = type;
    exploc.comp = comp;
    exploc.mode = access_mode::write;
    exploc.backparams = extra;

    mpi_volume newvol;
    newvol.copy ( comm_, (*this), filter, exploc );

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

    int ret = MPI_Barrier ( comm_ );

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

    // Just copy the root block.  This could be parallelized in the future.

    if ( rank == 0 ) {
        data_copy ( in.root(), out.root() );
    }

    ret = MPI_Barrier ( comm );

    return;
}

