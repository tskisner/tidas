/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#include <tidas_internal.hpp>

#include <cstdio>

#include <regex>


using namespace std;
using namespace tidas;


#define SQLERR(err, msg) \
sql_err( err, msg, __FILE__, __LINE__ )


tidas::indexdb_sql::indexdb_sql () : indexdb() {
	path_ = "";
	volpath_ = "";
	mode_ = access_mode::readwrite;
	sql_ = NULL;
}


tidas::indexdb_sql::~indexdb_sql () {
	close();
}


indexdb_sql & tidas::indexdb_sql::operator= ( indexdb_sql const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb_sql::indexdb_sql ( indexdb_sql const & other ) : indexdb( other ) {
	sql_ = NULL;
	copy ( other );
}


void tidas::indexdb_sql::copy ( indexdb_sql const & other ) {

	close();
	sql_ = NULL;

	path_ = other.path_;
	volpath_ = other.volpath_;
	mode_ = other.mode_;

	open();

	return;
}


tidas::indexdb_sql::indexdb_sql ( string const & path, string const & volpath, access_mode mode ) {
	path_ = path;
	volpath_ = volpath;
	mode_ = mode;
	sql_ = NULL;
	open();
}


string tidas::indexdb_sql::dbpath ( string const & fullpath ) {
	string ret;
	if ( volpath_ != "" ) {
		size_t pos = fullpath.find ( volpath_ );
		ret = fullpath.substr ( pos + volpath_.size() + 1 );
	} else {
		ret = fullpath;
	}
	return ret;
}


void tidas::indexdb_sql::init ( string const & path ) {

	if ( path == "" ) {
		TIDAS_THROW( "cannot initialize sqlite db with empty path" );
	}

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

	return;
}


void tidas::indexdb_sql::open () {

	if ( path_ == "" ) {
		TIDAS_THROW( "cannot open sqlite db with empty path" );
	}

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

				init ( path_ );

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

	return;
}


void tidas::indexdb_sql::close () {

	if ( sql_ ) {
		int ret = sqlite3_close_v2 ( sql_ );
		SQLERR( ret != SQLITE_OK, "close" );
		sql_ = NULL;
	}

	return;
}


void tidas::indexdb_sql::sql_err ( bool err, char const * msg, char const * file, int line ) {

	if ( err ) {
		ostringstream o;
		o << "sqlite DB error at " << msg << " (" << file << ", line " << line << "):  \"" << sqlite3_errmsg( sql_ ) << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	return;
}


void tidas::indexdb_sql::ops_dict ( backend_path loc, indexdb_op op, map < string, string > const & data, map < string, data_type > const & types ) {

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	if ( mode_ != access_mode::readwrite ) {
		ostringstream o;
		o << "cannot modify read-only db \"" << path_ << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {

		size_t pos = dpath.find( block_fs_group_dir );
		if ( pos != string::npos ) {
			command.str("");
			command << "INSERT OR REPLACE INTO dct_grp ( parent, data, types ) VALUES ( \"" << dpath << "\", ?, ? );";
		} else {
			pos = dpath.find( block_fs_intervals_dir );
			if ( pos != string::npos ) {
				command.str("");
				command << "INSERT OR REPLACE INTO dct_intrvl ( parent, data, types ) VALUES ( \"" << dpath << "\", ?, ? );";
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
			outdata ( data );
		}

		ret = sqlite3_bind_blob ( stmt, 1, (void*)datastr.str().c_str(), datastr.str().size() + 1, SQLITE_TRANSIENT );
		SQLERR( ret != SQLITE_OK, "dict bind data" );

		ostringstream typestr;
		{
			cereal::PortableBinaryOutputArchive outtype ( typestr );
			outtype ( types );
		}

		ret = sqlite3_bind_blob ( stmt, 2, (void*)typestr.str().c_str(), typestr.str().size() + 1, SQLITE_TRANSIENT );
		SQLERR( ret != SQLITE_OK, "dict bind types" );

		ret = sqlite3_step ( stmt );
		SQLERR( ret != SQLITE_DONE, "dict step" );

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "dict finalize" );

	} else if ( op == indexdb_op::del ) {

		size_t pos = dpath.find( block_fs_group_dir );
		if ( pos != string::npos ) {
			command.str("");
			command << "DELETE FROM dct_grp WHERE parent = \"" << dpath << "\";";
		} else {
			pos = dpath.find( block_fs_intervals_dir );
			if ( pos != string::npos ) {
				command.str("");
				command << "DELETE FROM dct_intrvl WHERE parent = \"" << dpath << "\";";
			} else {
				TIDAS_THROW( "dictionary path not associated with group or intervals" );
			}
		}

		char * sqlerr;

		int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
		SQLERR( ret != SQLITE_OK, "dict delete" );

	} else {
		TIDAS_THROW( "unknown indexdb dict operation" );
	}

	return;
}


void tidas::indexdb_sql::ops_schema ( backend_path loc, indexdb_op op, field_list const & fields ) {

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	if ( mode_ != access_mode::readwrite ) {
		ostringstream o;
		o << "cannot modify read-only db \"" << path_ << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {

		command.str("");
		command << "INSERT OR REPLACE INTO schm ( parent, fields ) VALUES ( \"" << dpath << "\", ? );";

		sqlite3_stmt * stmt;
		int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "schema prepare" );

		ostringstream fieldstr;
		{
			cereal::PortableBinaryOutputArchive fielddata ( fieldstr );
			fielddata ( fields );
		}

		ret = sqlite3_bind_blob ( stmt, 1, (void*)fieldstr.str().c_str(), fieldstr.str().size() + 1, SQLITE_TRANSIENT );
		SQLERR( ret != SQLITE_OK, "schema bind fields" );

		ret = sqlite3_step ( stmt );
		SQLERR( ret != SQLITE_DONE, "schema step" );

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "schema finalize" );

	} else if ( op == indexdb_op::del ) {

		command.str("");
		command << "DELETE FROM schm WHERE parent = \"" << dpath << "\";";

		char * sqlerr;

		int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
		SQLERR( ret != SQLITE_OK, "schema delete" );

	} else {
		TIDAS_THROW( "unknown indexdb schema operation" );
	}

	return;
}


void tidas::indexdb_sql::ops_group ( backend_path loc, indexdb_op op, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	if ( mode_ != access_mode::readwrite ) {
		ostringstream o;
		o << "cannot modify read-only db \"" << path_ << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {

		size_t plen = dpath.find ( path_sep + block_fs_group_dir + path_sep );
		if ( plen == string::npos ) {
			ostringstream o;
			o << "\"" << dpath << "\" is not a valid group path";
			TIDAS_THROW( o.str().c_str() );
		}

		string parent = dpath.substr ( 0, plen );

		command.str("");
		command << "INSERT OR REPLACE INTO grp ( path, parent, nsamp, start, stop, counts ) VALUES ( \"" << dpath << "\", \"" << parent << "\", ?, ?, ?, ? );";

		sqlite3_stmt * stmt;
		int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "group prepare" );

	    ret = sqlite3_bind_int64 ( stmt, 1, (sqlite3_int64)nsamp );
	    SQLERR( ret != SQLITE_OK, "group bind nsamp" );

		ret = sqlite3_bind_double ( stmt, 2, start );
		SQLERR( ret != SQLITE_OK, "group bind start" );

		ret = sqlite3_bind_double ( stmt, 3, stop );
		SQLERR( ret != SQLITE_OK, "group bind stop" );

		ostringstream countstr;
		{
			cereal::PortableBinaryOutputArchive countdata ( countstr );
			countdata ( counts );
		}

		ret = sqlite3_bind_blob ( stmt, 4, (void*)countstr.str().c_str(), countstr.str().size() + 1, SQLITE_TRANSIENT );
		SQLERR( ret != SQLITE_OK, "group bind counts" );

		ret = sqlite3_step ( stmt );
		SQLERR( ret != SQLITE_DONE, "group step" );

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "group finalize" );

	} else if ( op == indexdb_op::del ) {
		
		command.str("");
		command << "DELETE FROM grp WHERE path = \"" << dpath << "\";";

		char * sqlerr;

		int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
		SQLERR( ret != SQLITE_OK, "group delete" );

	} else {
		TIDAS_THROW( "unknown indexdb group operation" );
	}

	return;
}


void tidas::indexdb_sql::ops_intervals ( backend_path loc, indexdb_op op, size_t const & size ) {

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	if ( mode_ != access_mode::readwrite ) {
		ostringstream o;
		o << "cannot modify read-only db \"" << path_ << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {

		size_t plen = dpath.find ( path_sep + block_fs_intervals_dir + path_sep );
		if ( plen == string::npos ) {
			ostringstream o;
			o << "\"" << dpath << "\" is not a valid intervals path";
			TIDAS_THROW( o.str().c_str() );
		}

		string parent = dpath.substr ( 0, plen );

		command.str("");
		command << "INSERT OR REPLACE INTO intrvl ( path, parent, size ) VALUES ( \"" << dpath << "\", \"" << parent << "\", ? );";

		sqlite3_stmt * stmt;
		int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "intervals prepare" );

	    ret = sqlite3_bind_int64 ( stmt, 1, (sqlite3_int64)size );
	    SQLERR( ret != SQLITE_OK, "intervals bind size" );

		ret = sqlite3_step ( stmt );
		SQLERR( ret != SQLITE_DONE, "intervals step" );

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "intervals finalize" );

	} else if ( op == indexdb_op::del ) {

		command.str("");
		command << "DELETE FROM intrvl WHERE path = \"" << dpath << "\";";

		char * sqlerr;

		int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
		SQLERR( ret != SQLITE_OK, "intervals delete" );

	} else {
		TIDAS_THROW( "unknown indexdb intervals operation" );
	}

	return;
}


void tidas::indexdb_sql::ops_block ( backend_path loc, indexdb_op op ) {

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	if ( mode_ != access_mode::readwrite ) {
		ostringstream o;
		o << "cannot modify read-only db \"" << path_ << "\"";
		TIDAS_THROW( o.str().c_str() );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {

		string parent = "";
		size_t plen = dpath.rfind ( path_sep );
		if ( plen != string::npos ) {
			parent = dpath.substr ( 0, plen );
		}

		command.str("");
		command << "INSERT OR REPLACE INTO blk ( path, parent ) VALUES ( \"" << dpath << "\", \"" << parent << "\" );";

		sqlite3_stmt * stmt;
		int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "block prepare" );

		ret = sqlite3_step ( stmt );
		SQLERR( ret != SQLITE_DONE, "block step" );

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "block finalize" );

	} else if ( op == indexdb_op::del ) {
		
		command.str("");
		command << "DELETE FROM blk WHERE path = \"" << dpath << "\";";

		char * sqlerr;

		int ret = sqlite3_exec ( sql_, command.str().c_str(), NULL, NULL, &sqlerr );
		SQLERR( ret != SQLITE_OK, "block delete" );

	} else {
		TIDAS_THROW( "unknown indexdb block operation" );
	}

	return;
}


void tidas::indexdb_sql::add_dict ( backend_path loc, map < string, string > const & data, map < string, data_type > const & types ) {
	ops_dict ( loc, indexdb_op::add, data, types );
	return;
}


void tidas::indexdb_sql::update_dict ( backend_path loc, map < string, string > const & data, map < string, data_type > const & types ) {
	ops_dict ( loc, indexdb_op::update, data, types );
	return;
}


void tidas::indexdb_sql::del_dict ( backend_path loc ) {
	map < string, string > fakedata;
	map < string, data_type > faketypes;
	ops_dict ( loc, indexdb_op::del, fakedata, faketypes );
	return;
}


void tidas::indexdb_sql::add_schema ( backend_path loc, field_list const & fields ) {
	ops_schema ( loc, indexdb_op::add, fields );
	return;
}


void tidas::indexdb_sql::update_schema ( backend_path loc, field_list const & fields ) {
	ops_schema ( loc, indexdb_op::update, fields );
	return;
}


void tidas::indexdb_sql::del_schema ( backend_path loc ) {
	field_list fakefields;
	ops_schema ( loc, indexdb_op::del, fakefields );
	return;
}


void tidas::indexdb_sql::add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {
	ops_group ( loc, indexdb_op::add, nsamp, start, stop, counts );
	return;
}


void tidas::indexdb_sql::update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {
	ops_group ( loc, indexdb_op::update, nsamp, start, stop, counts );
	return;
}


void tidas::indexdb_sql::del_group ( backend_path loc ) {
	map < data_type, size_t > fakecounts;
	ops_group ( loc, indexdb_op::del, 0, 0.0, 0.0, fakecounts );
	return;
}


void tidas::indexdb_sql::add_intervals ( backend_path loc, size_t const & size ) {
	ops_intervals ( loc, indexdb_op::add, size );
	return;
}


void tidas::indexdb_sql::update_intervals ( backend_path loc, size_t const & size ) {
	ops_intervals ( loc, indexdb_op::update, size );
	return;
}


void tidas::indexdb_sql::del_intervals ( backend_path loc ) {
	ops_intervals ( loc, indexdb_op::del, 0 );
	return;
}


void tidas::indexdb_sql::add_block ( backend_path loc ) {
	ops_block ( loc, indexdb_op::add );
	return;
}


void tidas::indexdb_sql::update_block ( backend_path loc ) {
	ops_block ( loc, indexdb_op::update );
	return;
}


void tidas::indexdb_sql::del_block ( backend_path loc ) {
	ops_block ( loc, indexdb_op::del );
	return;
}


bool tidas::indexdb_sql::query_dict ( backend_path loc, map < string, string > & data, map < string, data_type > & types ) {

	bool found = false;

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	size_t pos = dpath.find( block_fs_group_dir );
	if ( pos != string::npos ) {
		command.str("");
		command << "SELECT * FROM dct_grp WHERE parent = \"" << dpath << "\";";
	} else {
		pos = dpath.find( block_fs_intervals_dir );
		if ( pos != string::npos ) {
			command.str("");
			command << "SELECT * FROM dct_intrvl WHERE parent = \"" << dpath << "\";";
		} else {
			TIDAS_THROW( "dictionary path not associated with group or intervals" );
		}
	}

	sqlite3_stmt * stmt;
	int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
	SQLERR( ret != SQLITE_OK, "dict query prepare" );

	ret = sqlite3_step ( stmt );

	if ( ret == SQLITE_ROW ) {

		char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 1 ) );

		size_t bytes = sqlite3_column_bytes ( stmt, 1 );

		string datablobstr ( rawstr, bytes );

		istringstream datastr ( datablobstr );
		{
			cereal::PortableBinaryInputArchive indata ( datastr );
			indata ( data );
		}

		rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 2 ) );

		bytes = sqlite3_column_bytes ( stmt, 2 );

		string typeblobstr ( rawstr, bytes );

		istringstream typestr ( typeblobstr );
		{
			cereal::PortableBinaryInputArchive intypes ( typestr );
			intypes ( types );
		}

		found = true;
	}

	ret = sqlite3_finalize ( stmt );
	SQLERR( ret != SQLITE_OK, "dict query finalize" );

	return found;
}


bool tidas::indexdb_sql::query_schema ( backend_path loc, field_list & fields ) {

	bool found = false;

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	command.str("");
	command << "SELECT * FROM schm WHERE parent = \"" << dpath << "\";";

	sqlite3_stmt * stmt;
	int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
	SQLERR( ret != SQLITE_OK, "schema query prepare" );

	ret = sqlite3_step ( stmt );

	if ( ret == SQLITE_ROW ) {

		char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 1 ) );

		size_t bytes = sqlite3_column_bytes ( stmt, 1 );

		string blobstr ( rawstr, bytes );

		istringstream fieldstr ( blobstr );
		{
			cereal::PortableBinaryInputArchive infields ( fieldstr );
			infields ( fields );
		}

		found = true;
	}

	ret = sqlite3_finalize ( stmt );
	SQLERR( ret != SQLITE_OK, "schema query finalize" );

	return found;
}


bool tidas::indexdb_sql::query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, map < data_type, size_t > & counts ) {

	bool found = false;

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	command.str("");
	command << "SELECT * FROM grp WHERE path = \"" << dpath << "\";";

	sqlite3_stmt * stmt;
	int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
	SQLERR( ret != SQLITE_OK, "group query prepare" );

	ret = sqlite3_step ( stmt );
	if ( ret == SQLITE_ROW ) {

		nsamp = (index_type) sqlite3_column_int64 ( stmt, 2 );
		start = sqlite3_column_double ( stmt, 3 );
		stop = sqlite3_column_double ( stmt, 4 );

		char const * rawstr = reinterpret_cast < char const * > ( sqlite3_column_blob ( stmt, 5 ) );

		size_t bytes = sqlite3_column_bytes ( stmt, 5 );

		string blobstr ( rawstr, bytes );

		istringstream countstr( blobstr );
		{
			cereal::PortableBinaryInputArchive incounts ( countstr );
			incounts ( counts );
		}

		found = true;
	}

	ret = sqlite3_finalize ( stmt );
	SQLERR( ret != SQLITE_OK, "group query finalize" );

	return found;
}


bool tidas::indexdb_sql::query_intervals ( backend_path loc, size_t & size ) {

	bool found = false;

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	command.str("");
	command << "SELECT * FROM intrvl WHERE path = \"" << dpath << "\";";

	sqlite3_stmt * stmt;
	int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
	SQLERR( ret != SQLITE_OK, "intervals query prepare" );

	ret = sqlite3_step ( stmt );

	if ( ret == SQLITE_ROW ) {
		size = (index_type) sqlite3_column_int64 ( stmt, 2 );
		found = true;
	}

	ret = sqlite3_finalize ( stmt );
	SQLERR( ret != SQLITE_OK, "intervals query finalize" );

	return found;
}


bool tidas::indexdb_sql::query_block ( backend_path loc, vector < string > & child_blocks, vector < string > & child_groups, vector < string > & child_intervals ) {

	bool found = false;

	if ( ! sql_ ) {
		TIDAS_THROW( "sqlite DB is not open" );
	}

	child_blocks.clear();
	child_groups.clear();
	child_intervals.clear();

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	ostringstream command;
	command.precision(20);

	command.str("");
	command << "SELECT * FROM blk WHERE path = \"" << dpath << "\";";

	sqlite3_stmt * bstmt;
	int ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &bstmt, NULL );
	SQLERR( ret != SQLITE_OK, "block query prepare" );

	ret = sqlite3_step ( bstmt );

	if ( ret == SQLITE_ROW ) {

		string dir;
		string base;

		// query direct descendents

		command.str("");
		command << "SELECT * FROM intrvl WHERE parent = \"" << dpath << "\";";

		sqlite3_stmt * stmt;
		ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "intervals child query prepare" );

		ret = sqlite3_step ( stmt );
		while ( ret == SQLITE_ROW ) {
			string result = (char*)sqlite3_column_text ( stmt, 0 );
			indexdb_path_split ( result, dir, base );
			child_intervals.push_back ( base );
			ret = sqlite3_step ( stmt );
		}

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "intervals child query finalize" );

		command.str("");
		command << "SELECT * FROM grp WHERE parent = \"" << dpath << "\";";

		ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "group child query prepare" );

		ret = sqlite3_step ( stmt );
		while ( ret == SQLITE_ROW ) {
			string result = (char*)sqlite3_column_text ( stmt, 0 );
			indexdb_path_split ( result, dir, base );
			child_groups.push_back ( base );
			ret = sqlite3_step ( stmt );
		}

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "group child query finalize" );

		command.str("");
		command << "SELECT * FROM blk WHERE parent = \"" << dpath << "\";";

		ret = sqlite3_prepare_v2 ( sql_, command.str().c_str(), command.str().size() + 1, &stmt, NULL );
		SQLERR( ret != SQLITE_OK, "block child query prepare" );

		ret = sqlite3_step ( stmt );
		while ( ret == SQLITE_ROW ) {
			string result = (char*)sqlite3_column_text ( stmt, 0 );
			indexdb_path_split ( result, dir, base );
			child_blocks.push_back ( base );
			ret = sqlite3_step ( stmt );
		}

		ret = sqlite3_finalize ( stmt );
		SQLERR( ret != SQLITE_OK, "block child query finalize" );

		found = true;
	}

	ret = sqlite3_finalize ( bstmt );
	SQLERR( ret != SQLITE_OK, "block query finalize" );

	return found;
}


void tidas::indexdb_sql::commit ( deque < indexdb_transaction > const & trans ) {

	for ( auto tr : trans ) {

		// we need to add our full volume path before calling the ops methods.

		string fullpath;
		if ( volpath_ == "" ) {
			fullpath = tr.obj->path;
		} else {
			fullpath = volpath_ + path_sep + tr.obj->path;
		}

		backend_path loc;

		size_t pos = fullpath.rfind ( path_sep );

		loc.path = fullpath.substr ( 0, pos );
		loc.name = fullpath.substr ( pos + 1 );

		indexdb_dict * dp;
		indexdb_schema * sp;
		indexdb_group * gp;
		indexdb_intervals * tp;
		indexdb_block * bp;

		switch ( tr.obj->type ) {
			case indexdb_object_type::dict :
				dp = dynamic_cast < indexdb_dict * > ( tr.obj.get() );
				ops_dict ( loc, tr.op, dp->data, dp->types );
				break;
			case indexdb_object_type::schema :
				sp = dynamic_cast < indexdb_schema * > ( tr.obj.get() );
				ops_schema ( loc, tr.op, sp->fields );
				break;
			case indexdb_object_type::group :
				gp = dynamic_cast < indexdb_group * > ( tr.obj.get() );
				ops_group ( loc, tr.op, gp->nsamp, gp->start, gp->stop, gp->counts );
				break;
			case indexdb_object_type::intervals :
				tp = dynamic_cast < indexdb_intervals * > ( tr.obj.get() );
				ops_intervals ( loc, tr.op, tp->size );
				break;
			case indexdb_object_type::block :
				bp = dynamic_cast < indexdb_block * > ( tr.obj.get() );
				ops_block ( loc, tr.op );
				break;
			default :
				TIDAS_THROW( "unknown indexdb object type" );
				break;
		}

	}

	return;
}


void tidas::indexdb_sql::tree_node ( backend_path loc, std::string const & filter, std::deque < indexdb_transaction > & trans ) {

	string path = loc.path + path_sep + loc.name;

	string dpath = dbpath ( path );

	vector < string > child_blocks;
	vector < string > child_groups;
	vector < string > child_intervals;

	bool bfound = query_block ( loc, child_blocks, child_groups, child_intervals );

	if ( ! bfound ) {
		ostringstream o;
		o << "cannot find block \"" << (loc.path + path_sep + loc.name) << "\" when exporting tree";
		TIDAS_THROW( o.str().c_str() );
	}

	// add transaction for this block

	indexdb_block b;
	b.type = indexdb_object_type::block;
	b.path = dpath;

	indexdb_transaction tr;
	tr.op = indexdb_op::add;
	tr.obj.reset ( b.clone() );

	trans.push_back ( tr );

	//cerr << "tree_node filter = \"" << filter << "\"" << endl;

	// split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX[X=X]/ ---> [XX=XX,XX=XX] XXXX[XX=XX]/XXX[X=X]/

	string filt_local;
	string filt_sub;
	bool stop;

	filter_block ( filter, filt_local, filt_sub, stop );

	// split local filter string:  [XX=XX,XX=XX]

	map < string, string > filts = filter_split ( filt_local );

	// select groups.  Since we are not copying objects to a
	// new location, we strip off any sub-object filters.

	string filt_name;
	string filt_pass;

	// XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]

	filter_sub ( filts[ group_submatch_key ], filt_name, filt_pass );

	//cerr << "tree_node group filter = \"" << filt_name << "\"" << endl;

	regex groupre ( filter_default ( filt_name ), std::regex::extended );

	backend_path chloc;

	for ( auto const & c : child_groups ) {
		if ( regex_match ( c, groupre ) ) {

			//cerr << "tree_node  using group " << c << endl;
			chloc = loc;
			chloc.path = chloc.path + path_sep + chloc.name + path_sep + block_fs_group_dir;
			chloc.name = c;

			string child_path = chloc.path + path_sep + chloc.name;

			index_type nsamp;
			time_type start;
			time_type stop;
			map < data_type, size_t > counts;

			bool found = query_group ( chloc, nsamp, start, stop, counts );

			if ( ! found ) {
				ostringstream o;
				o << "inconsistency in group \"" << child_path << "\" when exporting tree";
				TIDAS_THROW( o.str().c_str() );
			}

			indexdb_group g;
			g.type = indexdb_object_type::group;
			g.path = child_path;
			g.nsamp = nsamp;
			g.counts = counts;
			g.start = start;
			g.stop = stop;

			indexdb_transaction tr;
			tr.op = indexdb_op::add;
			tr.obj.reset ( g.clone() );

			trans.push_back ( tr );

			// now get the schema and dict

			field_list fields;

			found = query_schema ( chloc, fields );

			if ( ! found ) {
				ostringstream o;
				o << "inconsistency in schema \"" << child_path << "\" when exporting tree";
				TIDAS_THROW( o.str().c_str() );
			}

			indexdb_schema s;
			s.type = indexdb_object_type::schema;
			s.path = child_path;
			s.fields = fields;

			tr.op = indexdb_op::add;
			tr.obj.reset ( s.clone() );

			trans.push_back ( tr );

			map < string, string > data;
			map < string, data_type > types;

			found = query_dict ( chloc, data, types );

			if ( ! found ) {
				ostringstream o;
				o << "inconsistency in dict \"" << child_path << "\" when exporting tree";
				TIDAS_THROW( o.str().c_str() );
			}

			indexdb_dict d;
			d.type = indexdb_object_type::dict;
			d.path = child_path;
			d.data = data;
			d.types = types;

			tr.op = indexdb_op::add;
			tr.obj.reset ( d.clone() );

			trans.push_back ( tr );

		}
	}

	// select intervals.  Again, we strip off sub-filters.

	// XXX[dict=XXX] ---> XXX [dict=XXX]

	filter_sub ( filts[ intervals_submatch_key ], filt_name, filt_pass );

	//cerr << "tree_node intervals filter = \"" << filt_name << "\"" << endl;

	regex intre ( filter_default ( filt_name ), std::regex::extended );

	for ( auto const & c : child_intervals ) {
		if ( regex_match ( c, intre ) ) {

			//cerr << "tree_node  using intervals " << c << endl;

			chloc = loc;
			chloc.path = chloc.path + path_sep + chloc.name + path_sep + block_fs_intervals_dir;
			chloc.name = c;

			string child_path = chloc.path + path_sep + chloc.name;

			size_t size;

			bool found = query_intervals ( chloc, size );

			if ( ! found ) {
				ostringstream o;
				o << "inconsistency in intervals \"" << child_path << "\" when exporting tree";
				TIDAS_THROW( o.str().c_str() );
			}

			indexdb_intervals t;
			t.type = indexdb_object_type::intervals;
			t.path = child_path;
			t.size = size;

			indexdb_transaction tr;
			tr.op = indexdb_op::add;
			tr.obj.reset ( t.clone() );

			trans.push_back ( tr );

			// now get the dict

			map < string, string > data;
			map < string, data_type > types;

			found = query_dict ( chloc, data, types );

			if ( ! found ) {
				ostringstream o;
				o << "inconsistency in dict \"" << child_path << "\" when exporting tree";
				TIDAS_THROW( o.str().c_str() );
			}

			indexdb_dict d;
			d.type = indexdb_object_type::dict;
			d.path = child_path;
			d.data = data;
			d.types = types;

			tr.op = indexdb_op::add;
			tr.obj.reset ( d.clone() );

			trans.push_back ( tr );

		}
	}

	// select sub-blocks

	// extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX [XX=XX]/XXX
	// check if we have reached a hard stop at this depth, and in this case ignore all sub-blocks.

	if ( ! stop ) {

		filter_sub ( filt_sub, filt_name, filt_pass );

		regex blockre ( filter_default ( filt_name ), std::regex::extended );

		//cerr << "tree_node block filter = \"" << filt_name << "\"" << endl;

		for ( auto const & c : child_blocks ) {
			if ( regex_match ( c, blockre ) ) {

				//cerr << "tree_node  using block " << c << endl;

				chloc = loc;
				chloc.path = chloc.path + path_sep + chloc.name;
				chloc.name = c;
				
				tree_node( chloc, filt_pass, trans );
			} else {
				//cerr << "tree_node  rejecting block " << c << endl;
			}
		}

	}

	return;
}


void tidas::indexdb_sql::tree ( backend_path root, std::string const & filter, std::deque < indexdb_transaction > & result ) {

	result.clear();

	tree_node ( root, filter, result );

	return;
}


