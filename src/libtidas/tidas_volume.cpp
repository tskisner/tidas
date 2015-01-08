/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/

#include <tidas_internal.hpp>

#include <fstream>

extern "C" {
	#include <dirent.h>
}

using namespace std;
using namespace tidas;


tidas::volume::volume () {
	relocate ( loc_ );
}


tidas::volume::volume ( string const & path, backend_type type, compression_type comp ) {

	if ( fs_stat ( path.c_str() ) >= 0 ) {
		ostringstream o;
		o << "cannot create volume \"" << path << "\", a file or directory already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	loc_.path = path;
	loc_.name = "";
	loc_.meta = "";
	loc_.type = type;
	loc_.comp = comp;
	loc_.mode = access_mode::readwrite;

	relocate ( loc_ );

	flush();
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
	loc_.path = path;
	loc_.name = "";
	loc_.meta = "";

	relocate ( loc_ );

	sync();
}


tidas::volume::volume ( volume const & other, std::string const & filter, backend_path const & loc ) {
	copy ( other, filter, loc );
}


void tidas::volume::relocate ( backend_path const & loc ) {

	loc_ = loc;

	db_.reset ( new indexdb() );

	loc_.idx = db_;

	root_.relocate ( root_loc ( loc_ ) );

	return;
}


void tidas::volume::sync () {

	read_props ( loc_ );

	// check that root block exists

	string fspath = loc_.path;

	struct dirent * entry;
	DIR * dp;

	dp = opendir ( fspath.c_str() );

	if ( dp == NULL ) {
		ostringstream o;
		o << "cannot open volume \"" << fspath << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	bool found_data;
	bool found_index;

	while ( ( entry = readdir ( dp ) ) ) {
		string item = entry->d_name;

		if ( item == volume_fs_data_dir ) {
			found_data = true;
		} else if ( item == volume_fs_index ) {
			found_index = true;
		}
	}

	closedir ( dp );

	if ( ! found_data ) {
		ostringstream o;
		o << "volume \"" << fspath << "\" is missing the data subdirectory!";
		TIDAS_THROW( o.str().c_str() );
	}

	// sync root block

	root_.sync();

	// load index if it exists

	if ( found_index ) {


	}

	return;
}


void tidas::volume::flush () const {

	write_props ( loc_ );

	if ( loc_.mode == access_mode::readwrite ) {
		root_.flush();
	}

	// write index




	return;
}


void tidas::volume::copy ( volume const & other, string const & filter, backend_path const & loc ) {

	loc_ = loc;

	if ( loc_.type != backend_type::none ) {
		write_props ( loc_ );
	}

	root_.copy ( other.root_, filter, root_loc ( loc_ ) );

	// copy index or reindex

	if ( filter == "" ) {
		// just copy index

	} else {
		// regenerate
		reindex();
	}

	return;
}


void tidas::volume::link ( link_type const & type, string const & path, string const & filter ) const {

	if ( type != link_type::none ) {

		if ( loc_.type != backend_type::none ) {

			bool hard = ( type == link_type::hard );

			// make the top level directory

			if ( fs_stat ( path.c_str() ) >= 0 ) {
				ostringstream o;
				o << "cannot create volume link \"" << path << "\", a file or directory already exists";
				TIDAS_THROW( o.str().c_str() );
			}

			fs_mkdir ( path.c_str() );

			// link properties file



			// link root block

			//root_.link ();

		}

	}

	return;
}


void tidas::volume::wipe ( string const & filter ) const {

	if ( loc_.type != backend_type::none ) {

		if ( loc_.mode == access_mode::readwrite ) {


		} else {
			TIDAS_THROW( "cannot wipe volume in read-only mode" );
		}
	}

	return;
}


backend_path tidas::volume::location () const {
	return loc_;
}


backend_path tidas::volume::root_loc ( backend_path const & loc ) {
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


void tidas::volume::reindex () {

	return;
}


void tidas::volume::read_props ( backend_path & loc ) {

	string propfile = loc.path + path_sep + volume_fs_props;

	ifstream props ( propfile );

	if ( ! props.is_open() ) {
		ostringstream o;
		o << "cannot read volume properties from " << propfile;
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
		o << "volume has unsupported backend \"" << line << "\"";
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

	return;
}

			
void tidas::volume::write_props ( backend_path const & loc ) const {

	// make directory for the volume

	fs_mkdir ( loc.path.c_str() );

	// write properties to file

	string propfile = loc.path + path_sep + volume_fs_props;

	ofstream props ( propfile );

	if ( ! props.is_open() ) {
		ostringstream o;
		o << "cannot write volume properties to " << propfile;
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

	return;
}


