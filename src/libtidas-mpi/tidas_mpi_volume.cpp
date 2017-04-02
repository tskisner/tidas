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


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, string const & path, backend_type type, compression_type comp ) {

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


tidas::mpi_volume::mpi_volume ( MPI_Comm comm, string const & path, access_mode mode ) {

    comm_ = MPI_COMM_WORLD;
    int ret = MPI_Comm_rank ( comm_, &rank_ );
    ret = MPI_Comm_size ( comm_, &nproc_ );

    string relpath = path;
    if ( relpath[ relpath.size() - 1 ] == '/' ) {
        relpath[ relpath.size() - 1 ] = '\0';
    }

    string fspath = fs_fullpath ( relpath.c_str() );

    loc_.path = fspath;
    loc_.name = "";
    loc_.meta = "";
    loc_.mode = mode;

    // read properties

    read_props( loc_ );

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

        string propfile = loc.path + path_sep + mpi_volume_fs_props;

        ifstream props ( propfile );

        if ( ! props.is_open() ) {
            ostringstream o;
            o << "cannot read mpi_volume properties from " << propfile;
            TIDAS_THROW( o.str().c_str() );
        }

        string line;

        getline ( props, line );

        if ( line == "hdf5" ) {
            loc.type = backend_type::hdf5;
        } else if ( line == "getdata" ) {
            loc.type = backend_type::getdata;
        } else {
            ostringstream o;
            o << "mpi_volume has unsupported backend \"" << line << "\"";
            TIDAS_THROW( o.str().c_str() );
        }

        getline ( props, line );

        if ( line == "gzip" ) {
            loc.comp = compression_type::gzip;
        } else if ( line == "bzip2" ) {
            loc.comp = compression_type::bzip2;
        } else {
            loc.comp = compression_type::none;
        }

        props.close();

    }

    mpi_bcast ( comm_, loc, 0 );

    return;
}

            
void tidas::mpi_volume::write_props ( backend_path const & loc ) const {

    if ( rank_ == 0 ) {

        // make directory for the mpi_volume

        fs_mkdir ( loc.path.c_str() );

        // write properties to file

        string propfile = loc.path + path_sep + mpi_volume_fs_props;

        ofstream props ( propfile );

        if ( ! props.is_open() ) {
            ostringstream o;
            o << "cannot write mpi_volume properties to " << propfile;
            TIDAS_THROW( o.str().c_str() );
        }

        if ( loc.type == backend_type::hdf5 ) {
            props << "hdf5" << endl;
        } else if ( loc.type == backend_type::getdata ) {
            props << "getdata" << endl;
        } else {
            props << "none" << endl;
        }

        if ( loc.comp == compression_type::gzip ) {
            props << "gzip" << endl;
        } else if ( loc.comp == compression_type::bzip2 ) {
            props << "bzip2" << endl;
        } else {
            props << "none" << endl;
        }

        props.close();

    }

    int ret = MPI_Barrier ( comm_ );

    return;
}


