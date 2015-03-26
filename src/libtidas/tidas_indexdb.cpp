/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#include <tidas_internal.hpp>

#include <cstdio>


using namespace std;
using namespace tidas;


indexdb_object * tidas::indexdb_dict::clone () {
	return new indexdb_dict ( *this );
}


indexdb_object * tidas::indexdb_schema::clone () {
	return new indexdb_schema ( *this );
}


indexdb_object * tidas::indexdb_group::clone () {
	return new indexdb_group ( *this );
}


indexdb_object * tidas::indexdb_intervals::clone () {
	return new indexdb_intervals ( *this );
}


indexdb_object * tidas::indexdb_block::clone () {
	return new indexdb_block ( *this );
}



tidas::indexdb_transaction::indexdb_transaction () {

}


tidas::indexdb_transaction::~indexdb_transaction () {
}


indexdb_transaction & tidas::indexdb_transaction::operator= ( indexdb_transaction const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb_transaction::indexdb_transaction ( indexdb_transaction const & other ) {
	copy ( other );
}


void tidas::indexdb_transaction::copy ( indexdb_transaction const & other ) {
	op = other.op;
	obj.reset ( other.obj->clone() );
	return;
}



#define SQLERR(err, msg) \
sql_err( err, msg, __FILE__, __LINE__ )


tidas::indexdb::indexdb () {
	path_ = "";
	mode_ = access_mode::readwrite;
	sql_ = NULL;
	sql_open();
}


tidas::indexdb::~indexdb () {
	sql_close();
}


indexdb & tidas::indexdb::operator= ( indexdb const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb::indexdb ( indexdb const & other ) {
	sql_ = NULL;
	copy ( other );
}


void tidas::indexdb::copy ( indexdb const & other ) {
	path_ = other.path_;
	mode_ = other.mode_;
	history_ = other.history_;

	data_dict_ = other.data_dict_;
	data_schema_ = other.data_schema_;
	data_group_ = other.data_group_;
	data_intervals_ = other.data_intervals_;
	data_block_ = other.data_block_;

	sql_close();
	sql_open();

	return;
}


tidas::indexdb::indexdb ( string const & path, access_mode mode ) {
	path_ = path;
	mode_ = mode;
	sql_ = NULL;
	sql_open();
}


void tidas::indexdb::sql_init ( string const & path ) {

	if ( path != "" ) {

		int64_t size = fs_stat ( path.c_str() );

		int flags = 0;

		if ( size > 0 ) {

			ostringstream o;
			o << "sqlite DB \"" << path << "\" already exists";
			TIDAS_THROW( o.str().c_str() );

		} else {

			// create and initialize schema

			flags = flags | SQLITE_OPEN_READWRITE;
			flags = flags | SQLITE_OPEN_CREATE;

			sqlite3 * sql;

			int ret = sqlite3_open_v2 ( path.c_str(), &sql, flags, NULL );
			SQLERR( ret != SQLITE_OK, "open" );

			string command;
			char * sqlerr;

			command = "CREATE TABLE blk ( path TEXT PRIMARY KEY, parent TEXT, FOREIGN KEY(parent) REFERENCES blk(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create block table" );

			command = "CREATE TABLE grp ( path TEXT PRIMARY KEY, parent TEXT, nsamp INTEGER, start REAL, stop REAL, counts BLOB, FOREIGN KEY(parent) REFERENCES blk(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create group table" );

			command = "CREATE TABLE intrvl ( path TEXT PRIMARY KEY, parent TEXT, size INTEGER, FOREIGN KEY(parent) REFERENCES blk(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create intervals table" );

			command = "CREATE TABLE schm ( parent TEXT UNIQUE, fields BLOB, FOREIGN KEY(parent) REFERENCES grp(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create schema table" );

			command = "CREATE TABLE dct_grp ( parent TEXT UNIQUE, data BLOB, types BLOB, FOREIGN KEY(parent) REFERENCES grp(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create group dict table" );

			command = "CREATE TABLE dct_intrvl ( parent TEXT UNIQUE, data BLOB, types BLOB, FOREIGN KEY(parent) REFERENCES intrvl(path) );";

			ret = sqlite3_exec ( sql, command.c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "create intervals dict table" );

			// close

			ret = sqlite3_close_v2 ( sql );
			SQLERR( ret != SQLITE_OK, "close" );

		}
			
	}
	return;
}


void tidas::indexdb::sql_open () {
	// close DB if it is open
	if ( path_ != "" ) {
		if ( sql_ ) {
			TIDAS_THROW( "sqlite DB already open" );
		} else {

			int64_t size = fs_stat ( path_.c_str() );

			int flags = 0;

			if ( size > 0 ) {
				// just open
				
				if ( mode_ == access_mode::readwrite ) {
					flags = flags | SQLITE_OPEN_READWRITE;
				} else {
					flags = flags | SQLITE_OPEN_READONLY;
				}

				int ret = sqlite3_open_v2 ( path_.c_str(), &sql_, flags, NULL );
				SQLERR( ret != SQLITE_OK, "open" );

			} else {
				// create and initialize schema

				if ( mode_ == access_mode::readwrite ) {

					sql_init ( path_ );

					flags = flags | SQLITE_OPEN_READWRITE;

					int ret = sqlite3_open_v2 ( path_.c_str(), &sql_, flags, NULL );
					SQLERR( ret != SQLITE_OK, "open" );

				} else {
					ostringstream o;
					o << "cannot create sqlite DB \"" << path_ << "\" in read-only mode";
					TIDAS_THROW( o.str().c_str() );
				}

			}
			
		}
	}
	return;
}


void tidas::indexdb::sql_close () {
	// close DB if it is open
	if ( path_ != "" ) {
		if ( sql_ ) {
			int ret = sqlite3_close_v2 ( sql_ );
			SQLERR( ret != SQLITE_OK, "close" );
			sql_ = NULL;
		}
	}
	return;
}


void tidas::indexdb::sql_err ( bool err, char const * msg, char const * file, int line ) {

	if ( err ) {
		ostringstream o;
		o << "sqlite DB error at " << msg << " (" << file << ", line " << line << "):  \"" << sqlite3_errmsg( sql_ ) << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	return;
}


void tidas::indexdb::duplicate ( std::string const & path ) const {

	int64_t size = fs_stat ( path.c_str() );
	if ( size > 0 ) {
		ostringstream o;
		o << "cannot duplicate index to \"" << path << "\", file already exists";
		TIDAS_THROW( o.str().c_str() );
	}

	indexdb dup ( path, access_mode::readwrite );

	for ( auto const & b : data_block_ ) {
		dup.ins_block ( b.first, b.second );
	}

	for ( auto const & g : data_group_ ) {
		dup.ins_group ( g.first, g.second );
	}

	for ( auto const & t : data_intervals_ ) {
		dup.ins_intervals ( t.first, t.second );
	}

	for ( auto const & s : data_schema_ ) {
		dup.ins_schema ( s.first, s.second );
	}

	for ( auto const & d : data_dict_ ) {
		dup.ins_dict ( d.first, d.second );
	}

	return;
}


void tidas::indexdb::ins_dict ( string const & path, indexdb_dict const & d ) {

	data_dict_[ path ] = d;

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			size_t pos = path.find( block_fs_group_dir );
			if ( pos != string::npos ) {
				command.str("");
				command << "INSERT OR REPLACE INTO dct_grp ( parent, data, types ) VALUES ( \"" << path << "\", ?, ? );";
			} else {
				pos = path.find( block_fs_intervals_dir );
				if ( pos != string::npos ) {
					command.str("");
					command << "INSERT OR REPLACE INTO dct_intrvl ( parent, data, types ) VALUES ( \"" << path << "\", ?, ? );";
				} else {
					TIDAS_THROW( "dictionary path not associated with group or intervals" );
				}
			}

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "dict prepare" );

			ostringstream datastr;
			{
				cereal::PortableBinaryOutputArchive outdata ( datastr );
  				outdata ( d.data );
  			}

  			ret = sqlite3_bind_blob ( stmt, 1, (void*)datastr.str().c_str(), datastr.str().size() + 1, SQLITE_TRANSIENT );
  			SQLERR( ret != SQLITE_OK, "dict bind data" );

  			ostringstream typestr;
  			{
  				cereal::PortableBinaryOutputArchive outtype ( typestr );
  				outtype ( d.types );
  			}

  			ret = sqlite3_bind_blob ( stmt, 2, (void*)typestr.str().c_str(), typestr.str().size() + 1, SQLITE_TRANSIENT );
  			SQLERR( ret != SQLITE_OK, "dict bind types" );

			ret = sqlite3_step ( stmt );
			SQLERR( ret != SQLITE_DONE, "dict step" );

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "dict finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::rm_dict ( string const & path ) {

	if ( data_dict_.count ( path ) == 0 ) {
		ostringstream o;
		o << "dictionary at \"" << path << "\" does not exist in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_dict_.erase ( path );

	// modify DB if path is set
	if ( path_ != "" ) {

		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			size_t pos = path.find( block_fs_group_dir );
			if ( pos != string::npos ) {
				command.str("");
				command << "DELETE FROM dct_grp WHERE parent = \"" << path << "\";";
			} else {
				pos = path.find( block_fs_intervals_dir );
				if ( pos != string::npos ) {
					command.str("");
					command << "DELETE FROM dct_intrvl WHERE parent = \"" << path << "\";";
				} else {
					TIDAS_THROW( "dictionary path not associated with group or intervals" );
				}
			}

			char * sqlerr;

			int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "dict delete" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}

	}

	return;
}


void tidas::indexdb::ins_schema ( string const & path, indexdb_schema const & s ) {

	if ( data_schema_.count ( path ) > 0 ) {
		ostringstream o;
		o << "schema at \"" << path << "\" already exists in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_schema_[ path ] = s;

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "INSERT OR REPLACE INTO schm ( parent, fields ) VALUES ( \"" << path << "\", ? );";

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "schema prepare" );

			ostringstream fieldstr;
			{
				cereal::PortableBinaryOutputArchive fielddata ( fieldstr );
  				fielddata ( s.fields );
  			}

  			ret = sqlite3_bind_blob ( stmt, 1, (void*)fieldstr.str().c_str(), fieldstr.str().size() + 1, SQLITE_TRANSIENT );
  			SQLERR( ret != SQLITE_OK, "schema bind fields" );

			ret = sqlite3_step ( stmt );
			SQLERR( ret != SQLITE_DONE, "schema step" );

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "schema finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}

	}

	return;
}


void tidas::indexdb::rm_schema ( string const & path ) {

	if ( data_schema_.count ( path ) == 0 ) {
		ostringstream o;
		o << "schema at \"" << path << "\" does not exist in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_schema_.erase ( path );

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "DELETE FROM schm WHERE parent = \"" << path << "\";";

			char * sqlerr;

			int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "schema delete" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::ins_group ( string const & path, indexdb_group const & g ) {

	if ( data_group_.count ( path ) > 0 ) {
		ostringstream o;
		o << "group at \"" << path << "\" already exists in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_group_[ path ] = g;

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			size_t plen = path.find ( path_sep + block_fs_group_dir + path_sep );
			if ( plen == string::npos ) {
				ostringstream o;
				o << "\"" << path << "\" is not a valid group path";
				TIDAS_THROW( o.str().c_str() );
			}

			string parent = path.substr ( 0, plen );

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "INSERT OR REPLACE INTO grp ( path, parent, nsamp, start, stop, counts ) VALUES ( \"" << path << "\", \"" << parent << "\", ?, ?, ?, ? );";

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "group prepare" );

		    ret = sqlite3_bind_int64 ( stmt, 1, (sqlite3_int64)g.nsamp );
		    SQLERR( ret != SQLITE_OK, "group bind nsamp" );

			ret = sqlite3_bind_double ( stmt, 2, g.start );
			SQLERR( ret != SQLITE_OK, "group bind start" );

			ret = sqlite3_bind_double ( stmt, 3, g.stop );
			SQLERR( ret != SQLITE_OK, "group bind stop" );

			ostringstream countstr;
			{
				cereal::PortableBinaryOutputArchive countdata ( countstr );
  				countdata ( g.counts );
  			}

  			ret = sqlite3_bind_blob ( stmt, 4, (void*)countstr.str().c_str(), countstr.str().size() + 1, SQLITE_TRANSIENT );
  			SQLERR( ret != SQLITE_OK, "group bind counts" );

			ret = sqlite3_step ( stmt );
			SQLERR( ret != SQLITE_DONE, "group step" );

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "group finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::rm_group ( string const & path ) {

	if ( data_group_.count ( path ) == 0 ) {
		ostringstream o;
		o << "group at \"" << path << "\" does not exist in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_group_.erase ( path );

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "DELETE FROM grp WHERE path = \"" << path << "\";";

			char * sqlerr;

			int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "group delete" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::ins_intervals ( string const & path, indexdb_intervals const & t ) {

	if ( data_intervals_.count ( path ) > 0 ) {
		ostringstream o;
		o << "intervals at \"" << path << "\" already exists in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_intervals_[ path ] = t;

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			size_t plen = path.find ( path_sep + block_fs_intervals_dir + path_sep );
			if ( plen == string::npos ) {
				ostringstream o;
				o << "\"" << path << "\" is not a valid intervals path";
				TIDAS_THROW( o.str().c_str() );
			}

			string parent = path.substr ( 0, plen );

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "INSERT OR REPLACE INTO intrvl ( path, parent, size ) VALUES ( \"" << path << "\", \"" << parent << "\", ? );";

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "intervals prepare" );

		    ret = sqlite3_bind_int64 ( stmt, 1, (sqlite3_int64)t.size );
		    SQLERR( ret != SQLITE_OK, "intervals bind size" );

			ret = sqlite3_step ( stmt );
			SQLERR( ret != SQLITE_DONE, "intervals step" );

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "intervals finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::rm_intervals ( string const & path ) {

	if ( data_intervals_.count ( path ) == 0 ) {
		ostringstream o;
		o << "intervals at \"" << path << "\" does not exist in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_intervals_.erase ( path );

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "DELETE FROM intrvl WHERE path = \"" << path << "\";";

			char * sqlerr;

			int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "intervals delete" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::ins_block ( string const & path, indexdb_block const & b ) {

	if ( data_block_.count ( path ) > 0 ) {
		ostringstream o;
		o << "block at \"" << path << "\" already exists in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_block_[ path ] = b;

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			size_t plen = path.rfind ( path_sep );
			if ( plen == string::npos ) {
				ostringstream o;
				o << "\"" << path << "\" is not a valid group path";
				TIDAS_THROW( o.str().c_str() );
			}

			string parent = path.substr ( 0, plen );

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "INSERT OR REPLACE INTO blk ( path, parent ) VALUES ( \"" << path << "\", \"" << parent << "\" );";

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "block prepare" );

			ret = sqlite3_step ( stmt );
			SQLERR( ret != SQLITE_DONE, "block step" );

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "block finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::rm_block ( string const & path ) {

	if ( data_block_.count ( path ) == 0 ) {
		ostringstream o;
		o << "block at \"" << path << "\" does not exist in index";
		TIDAS_THROW( o.str().c_str() );
	}

	data_block_.erase ( path );

	// modify DB if path is set
	if ( path_ != "" ) {
		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "DELETE FROM blk WHERE path = \"" << path << "\";";

			char * sqlerr;

			int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
			SQLERR( ret != SQLITE_OK, "block delete" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	return;
}


void tidas::indexdb::add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_dict d;
	d.type = indexdb_object_type::dict;
	d.path = path;
	d.data = data;
	d.types = types;

	ins_dict ( path, d );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::add;
		tr.obj.reset ( d.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::update_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_dict d;
	d.type = indexdb_object_type::dict;
	d.path = path;
	d.data = data;
	d.types = types;

	ins_dict ( path, d );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::update;
		tr.obj.reset ( d.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::del_dict ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_dict d;
	d.type = indexdb_object_type::dict;
	d.path = path;

	rm_dict ( path );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::del;
		tr.obj.reset ( d.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) {

	string path = loc.path + path_sep + loc.name;

	if ( data_dict_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB

			if ( sql_ ) {

				ostringstream command;
				command.precision(20);

				size_t pos = path.find( block_fs_group_dir );
				if ( pos != string::npos ) {
					command.str("");
					command << "SELECT * FROM dct_grp WHERE parent = \"" << path << "\";";
				} else {
					pos = path.find( block_fs_intervals_dir );
					if ( pos != string::npos ) {
						command.str("");
						command << "SELECT * FROM dct_intrvl WHERE parent = \"" << path << "\";";
					} else {
						TIDAS_THROW( "dictionary path not associated with group or intervals" );
					}
				}

				sqlite3_stmt * stmt;
				int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
				SQLERR( ret != SQLITE_OK, "dict query prepare" );

				ret = sqlite3_step ( stmt );
				SQLERR( ret != SQLITE_ROW, "dict query step" );

				indexdb_dict d;
				d.type = indexdb_object_type::dict;
				d.path = path;

				char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 1 ) );

				size_t bytes = sqlite3_column_bytes ( stmt, 1 );

				string datablobstr ( rawstr, bytes );

				istringstream datastr ( datablobstr );
				{
					cereal::PortableBinaryInputArchive indata ( datastr );
					indata ( d.data );
				}

				rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 2 ) );

				bytes = sqlite3_column_bytes ( stmt, 2 );

				string typeblobstr ( rawstr, bytes );

				istringstream typestr ( typeblobstr );
				{
					cereal::PortableBinaryInputArchive intypes ( typestr );
					intypes ( d.types );
				}

				ret = sqlite3_finalize ( stmt );
				SQLERR( ret != SQLITE_OK, "dict query finalize" );

				data_dict_[ path ] = d;

			} else {
				ostringstream o;
				o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
				TIDAS_THROW( o.str().c_str() );
			}

		} else {
			ostringstream o;
			o << "dictionary at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	data = data_dict_.at( path ).data;
	types = data_dict_.at( path ).types;

	return;
}


void tidas::indexdb::add_schema ( backend_path loc, field_list const & fields ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_schema s;
	s.type = indexdb_object_type::schema;
	s.path = path;
	s.fields = fields;

	ins_schema ( path, s );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::add;
		tr.obj.reset ( s.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::update_schema ( backend_path loc, field_list const & fields ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_schema s;
	s.type = indexdb_object_type::schema;
	s.path = path;
	s.fields = fields;

	ins_schema ( path, s );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::update;
		tr.obj.reset ( s.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::del_schema ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_schema s;
	s.type = indexdb_object_type::schema;
	s.path = path;

	rm_schema ( path );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::del;
		tr.obj.reset ( s.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::query_schema ( backend_path loc, field_list & fields ) {

	string path = loc.path + path_sep + loc.name;

	if ( data_schema_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB

			if ( sql_ ) {

				ostringstream command;
				command.precision(20);

				command.str("");
				command << "SELECT * FROM schm WHERE parent = \"" << path << "\";";

				sqlite3_stmt * stmt;
				int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
				SQLERR( ret != SQLITE_OK, "schema query prepare" );

				ret = sqlite3_step ( stmt );
				SQLERR( ret != SQLITE_ROW, "schema query step" );

				indexdb_schema s;
				s.type = indexdb_object_type::schema;
				s.path = path;

				char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 1 ) );

				size_t bytes = sqlite3_column_bytes ( stmt, 1 );

				string blobstr ( rawstr, bytes );

				istringstream fieldstr ( blobstr );
				{
					cereal::PortableBinaryInputArchive infields ( fieldstr );
					infields ( s.fields );
				}

				ret = sqlite3_finalize ( stmt );
				SQLERR( ret != SQLITE_OK, "schema query finalize" );

				data_schema_[ path ] = s;

			} else {
				ostringstream o;
				o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
				TIDAS_THROW( o.str().c_str() );
			}

		} else {
			ostringstream o;
			o << "schema at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	fields = data_schema_.at( path ).fields;

	return;
}


void tidas::indexdb::add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_group g;
	g.type = indexdb_object_type::group;
	g.path = path;
	g.nsamp = nsamp;
	g.counts = counts;
	g.start = start;
	g.stop = stop;

	ins_group ( path, g );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::add;
		tr.obj.reset ( g.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, std::map < data_type, size_t > const & counts ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_group g;
	g.type = indexdb_object_type::group;
	g.path = path;
	g.nsamp = nsamp;
	g.counts = counts;
	g.start = start;
	g.stop = stop;

	ins_group ( path, g );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::update;
		tr.obj.reset ( g.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::del_group ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_group g;
	g.type = indexdb_object_type::group;
	g.path = path;

	rm_group ( path );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::del;
		tr.obj.reset ( g.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts ) {

	string path = loc.path + path_sep + loc.name;

	if ( data_group_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB
			if ( sql_ ) {

				ostringstream command;
				command.precision(20);

				command.str("");
				command << "SELECT * FROM grp WHERE path = \"" << path << "\";";

				sqlite3_stmt * stmt;
				int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
				SQLERR( ret != SQLITE_OK, "group query prepare" );

				ret = sqlite3_step ( stmt );
				SQLERR( ret != SQLITE_ROW, "group query step" );

				indexdb_group g;
				g.type = indexdb_object_type::group;
				g.path = path;

				g.nsamp = (index_type) sqlite3_column_int64 ( stmt, 2 );
				g.start = sqlite3_column_double ( stmt, 3 );
				g.stop = sqlite3_column_double ( stmt, 4 );

				char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 5 ) );

				size_t bytes = sqlite3_column_bytes ( stmt, 5 );

				string blobstr ( rawstr, bytes );

				istringstream countstr( blobstr );
				{
					cereal::PortableBinaryInputArchive incounts ( countstr );
					incounts ( g.counts );
				}

				ret = sqlite3_finalize ( stmt );
				SQLERR( ret != SQLITE_OK, "group query finalize" );

				data_group_[ path ] = g;

			} else {
				ostringstream o;
				o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
				TIDAS_THROW( o.str().c_str() );
			}

		} else {
			ostringstream o;
			o << "group at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	nsamp = data_group_.at( path ).nsamp;
	start = data_group_.at( path ).start;
	stop = data_group_.at( path ).stop;
	counts = data_group_.at( path ).counts;

	return;
}


void tidas::indexdb::add_intervals ( backend_path loc, size_t const & size ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_intervals t;
	t.type = indexdb_object_type::intervals;
	t.path = path;
	t.size = size;

	ins_intervals ( path, t );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::add;
		tr.obj.reset ( t.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::update_intervals ( backend_path loc, size_t const & size ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_intervals t;
	t.type = indexdb_object_type::intervals;
	t.path = path;
	t.size = size;

	ins_intervals ( path, t );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::update;
		tr.obj.reset ( t.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::del_intervals ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_intervals t;
	t.type = indexdb_object_type::intervals;
	t.path = path;

	rm_intervals ( path );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::del;
		tr.obj.reset ( t.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::query_intervals ( backend_path loc, size_t & size ) {

	string path = loc.path + path_sep + loc.name;

	if ( data_intervals_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB
			if ( sql_ ) {

				ostringstream command;
				command.precision(20);

				command.str("");
				command << "SELECT * FROM intrvl WHERE path = \"" << path << "\";";

				sqlite3_stmt * stmt;
				int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
				SQLERR( ret != SQLITE_OK, "intervals query prepare" );

				ret = sqlite3_step ( stmt );
				SQLERR( ret != SQLITE_ROW, "intervals query step" );

				indexdb_intervals t;
				t.type = indexdb_object_type::intervals;
				t.path = path;

				t.size = (index_type) sqlite3_column_int64 ( stmt, 2 );

				ret = sqlite3_finalize ( stmt );
				SQLERR( ret != SQLITE_OK, "intervals query finalize" );

				data_intervals_[ path ] = t;

			} else {
				ostringstream o;
				o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
				TIDAS_THROW( o.str().c_str() );
			}

		} else {
			ostringstream o;
			o << "intervals at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	size = data_intervals_.at( path ).size;

	return;
}



void tidas::indexdb::add_block ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_block b;
	b.type = indexdb_object_type::block;
	b.path = path;

	ins_block ( path, b );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::add;
		tr.obj.reset ( b.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::update_block ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_block b;
	b.type = indexdb_object_type::block;
	b.path = path;

	ins_block ( path, b );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::update;
		tr.obj.reset ( b.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::del_block ( backend_path loc ) {

	string path = loc.path + path_sep + loc.name;

	indexdb_block b;
	b.type = indexdb_object_type::block;
	b.path = path;

	rm_block ( path );

	if ( path_ == "" ) {
		// save transaction

		indexdb_transaction tr;
		tr.op = indexdb_op::del;
		tr.obj.reset ( b.clone() );

		history_.push_back ( tr );
	}

	return;
}


void tidas::indexdb::query_block ( backend_path loc, std::vector < std::string > & child_blocks, std::vector < std::string > & child_groups, std::vector < std::string > & child_intervals ) {

	string path = loc.path + path_sep + loc.name;

	if ( data_block_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB
			if ( sql_ ) {

				ostringstream command;
				command.precision(20);

				command.str("");
				command << "SELECT * FROM blk WHERE path = \"" << path << "\";";

				sqlite3_stmt * stmt;
				int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
				SQLERR( ret != SQLITE_OK, "block query prepare" );

				ret = sqlite3_step ( stmt );
				SQLERR( ret != SQLITE_ROW, "block query step" );

				indexdb_block b;
				b.type = indexdb_object_type::block;
				b.path = path;

				ret = sqlite3_finalize ( stmt );
				SQLERR( ret != SQLITE_OK, "block query finalize" );

				data_block_[ path ] = b;

			} else {
				ostringstream o;
				o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
				TIDAS_THROW( o.str().c_str() );
			}

		} else {
			ostringstream o;
			o << "block at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	// Find all direct descendants.

	child_blocks.clear();
	child_groups.clear();
	child_intervals.clear();

	if ( path_ != "" ) {

		if ( sql_ ) {

			ostringstream command;
			command.precision(20);

			command.str("");
			command << "SELECT * FROM blk WHERE parent = \"" << path << "\";";

			sqlite3_stmt * stmt;
			int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "block child query prepare" );

			ret = sqlite3_step ( stmt );
			while ( ret == SQLITE_ROW ) {
				string result = (char*)sqlite3_column_text ( stmt, 1 );
				child_blocks.push_back ( result );
				ret = sqlite3_step ( stmt );
			}

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "block child query finalize" );

			command.str("");
			command << "SELECT * FROM grp WHERE parent = \"" << path << "\";";

			ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "group child query prepare" );

			ret = sqlite3_step ( stmt );
			while ( ret == SQLITE_ROW ) {
				string result = (char*)sqlite3_column_text ( stmt, 1 );
				child_groups.push_back ( result );
				ret = sqlite3_step ( stmt );
			}

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "group child query finalize" );

			command.str("");
			command << "SELECT * FROM intrvl WHERE parent = \"" << path << "\";";

			ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
			SQLERR( ret != SQLITE_OK, "intervals child query prepare" );

			ret = sqlite3_step ( stmt );
			while ( ret == SQLITE_ROW ) {
				string result = (char*)sqlite3_column_text ( stmt, 1 );
				child_intervals.push_back ( result );
				ret = sqlite3_step ( stmt );
			}

			ret = sqlite3_finalize ( stmt );
			SQLERR( ret != SQLITE_OK, "intervals child query finalize" );

		} else {
			ostringstream o;
			o << "indexdb path = \"" << path_ << "\", but sqlite DB is not open!";
			TIDAS_THROW( o.str().c_str() );
		}


	} else {

		std::map < std::string, indexdb_block > :: const_iterator bit = data_block_.lower_bound ( path );

		while ( ( bit != data_block_.end() ) && ( bit->first.compare ( 0, path.size(), path ) == 0 ) ) {
			if ( bit->first.size() > path.size() ) {
				//(we don't want the parent itself)

				size_t off = path.size() + 1;
				size_t pos = bit->first.find ( path_sep, off );

				if ( pos == string::npos ) {
					// direct descendant
					child_blocks.push_back( bit->first );
				}
			}
			++bit;
		}

		string grpdir = path + path_sep + block_fs_group_dir;

		std::map < std::string, indexdb_group > :: const_iterator git = data_group_.lower_bound ( grpdir );

		while ( ( git != data_group_.end() ) && ( git->first.compare ( 0, grpdir.size(), grpdir ) == 0 ) ) {
			child_groups.push_back ( git->first );
			++git;
		}

		string intdir = path + path_sep + block_fs_intervals_dir;

		std::map < std::string, indexdb_intervals > :: const_iterator it = data_intervals_.lower_bound ( intdir );

		while ( ( it != data_intervals_.end() ) && ( it->first.compare ( 0, intdir.size(), intdir ) == 0 ) ) {
			child_intervals.push_back ( it->first );
			++it;
		}

	}

	return;
}


std::deque < indexdb_transaction > const & tidas::indexdb::history () const {
	return history_;
}


void tidas::indexdb::history_clear() {
	history_.clear();
	return;
}


void tidas::indexdb::replay ( std::deque < indexdb_transaction > const & trans ) {

	for ( auto tr : trans ) {

		switch ( tr.op ) {
			case indexdb_op::add :

				switch ( tr.obj->type ) {
					case indexdb_object_type::dict :
						ins_dict ( tr.obj->path, *( dynamic_cast < indexdb_dict * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::schema :
						ins_schema ( tr.obj->path, *( dynamic_cast < indexdb_schema * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::group :
						ins_group ( tr.obj->path, *( dynamic_cast < indexdb_group * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::intervals :
						ins_intervals ( tr.obj->path, *( dynamic_cast < indexdb_intervals * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::block :
						ins_block ( tr.obj->path, *( dynamic_cast < indexdb_block * > ( tr.obj.get() ) ) );
						break;
					default :
						TIDAS_THROW( "unknown indexdb object type" );
						break;
				}

				break;
			case indexdb_op::del :

				switch ( tr.obj->type ) {
					case indexdb_object_type::dict :
						rm_dict ( tr.obj->path );
						break;
					case indexdb_object_type::schema :
						rm_schema ( tr.obj->path );
						break;
					case indexdb_object_type::group :
						rm_group ( tr.obj->path );
						break;
					case indexdb_object_type::intervals :
						rm_intervals ( tr.obj->path );
						break;
					case indexdb_object_type::block :
						rm_block ( tr.obj->path );
						break;
					default :
						TIDAS_THROW( "unknown indexdb object type" );
						break;
				}

				break;
			case indexdb_op::update :

				switch ( tr.obj->type ) {
					case indexdb_object_type::dict :
						ins_dict ( tr.obj->path, *( dynamic_cast < indexdb_dict * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::schema :
						ins_schema ( tr.obj->path, *( dynamic_cast < indexdb_schema * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::group :
						ins_group ( tr.obj->path, *( dynamic_cast < indexdb_group * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::intervals :
						ins_intervals ( tr.obj->path, *( dynamic_cast < indexdb_intervals * > ( tr.obj.get() ) ) );
						break;
					case indexdb_object_type::block :
						ins_block ( tr.obj->path, *( dynamic_cast < indexdb_block * > ( tr.obj.get() ) ) );
						break;
					default :
						TIDAS_THROW( "unknown indexdb object type" );
						break;
				}

				break;
			default :
				TIDAS_THROW( "unknown indexdb transaction operator" );
				break;
		}

	}

	return;
}



CEREAL_REGISTER_TYPE( tidas::indexdb_dict );
CEREAL_REGISTER_TYPE( tidas::indexdb_schema );
CEREAL_REGISTER_TYPE( tidas::indexdb_group );
CEREAL_REGISTER_TYPE( tidas::indexdb_intervals );
CEREAL_REGISTER_TYPE( tidas::indexdb_block );




