/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#include <tidas_internal.hpp>

#include <cstdio>

#include <regex>

#include <chrono>

#include <tidas/cereal/archives/portable_binary.hpp>

using namespace std;
using namespace tidas;


#define SQLERR(err, msg) \
    sql_err(err, msg, __FILE__, __LINE__)


tidas::indexdb_sql::indexdb_sql() : indexdb() {
    path_ = "";
    mode_ = access_mode::write;
    sql_ = NULL;
}

tidas::indexdb_sql::~indexdb_sql() {
    close();
}

indexdb_sql & tidas::indexdb_sql::operator=(indexdb_sql const & other) {
    if (this != &other) {
        copy(other);
    }
    return *this;
}

tidas::indexdb_sql::indexdb_sql(indexdb_sql const & other) : indexdb(other) {
    sql_ = NULL;
    copy(other);
}

void tidas::indexdb_sql::copy(indexdb_sql const & other) {
    close();
    path_ = other.path_;
    volpath_ = other.volpath_;
    mode_ = other.mode_;
    return;
}

tidas::indexdb_sql::indexdb_sql(string const & path, string const & volpath,
                                access_mode mode) : indexdb(volpath) {
    path_ = path;
    mode_ = mode;
    sql_ = NULL;
}

void tidas::indexdb_sql::init(string const & path) {
    if (path == "") {
        TIDAS_THROW("cannot initialize sqlite db with empty path");
    }

    int64_t size = fs_stat(path.c_str());

    int flags = 0;

    if (size > 0) {
        ostringstream o;
        o << "sqlite DB \"" << path << "\" already exists";
        TIDAS_THROW(o.str().c_str());
    } else {
        // create and initialize schema

        flags = flags | SQLITE_OPEN_READWRITE;
        flags = flags | SQLITE_OPEN_CREATE;

        sqlite3 * sql;

        int ret = sqlite3_open_v2(path.c_str(), &sql, flags, NULL);
        SQLERR(ret != SQLITE_OK, "open");

        string command;
        char * sqlerr;

        command =
            "CREATE TABLE blk ( path TEXT PRIMARY KEY, parent TEXT, FOREIGN KEY(parent) REFERENCES blk(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create block table");

        command =
            "CREATE TABLE grp ( path TEXT PRIMARY KEY, parent TEXT, nsamp INTEGER, start REAL, stop REAL, counts BLOB, FOREIGN KEY(parent) REFERENCES blk(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create group table");

        command =
            "CREATE TABLE intrvl ( path TEXT PRIMARY KEY, parent TEXT, size INTEGER, FOREIGN KEY(parent) REFERENCES blk(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create intervals table");

        command =
            "CREATE TABLE schm ( parent TEXT UNIQUE, fields BLOB, FOREIGN KEY(parent) REFERENCES grp(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create schema table");

        command =
            "CREATE TABLE dct_grp ( parent TEXT UNIQUE, data BLOB, types BLOB, FOREIGN KEY(parent) REFERENCES grp(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create group dict table");

        command =
            "CREATE TABLE dct_intrvl ( parent TEXT UNIQUE, data BLOB, types BLOB, FOREIGN KEY(parent) REFERENCES intrvl(path) );";

        ret = sqlite3_exec(sql, command.c_str(), NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "create intervals dict table");

        // close

        ret = sqlite3_close_v2(sql);
        SQLERR(ret != SQLITE_OK, "close");
    }

    return;
}

void tidas::indexdb_sql::open() {
    int ret;
    char * sqlerr;

    if (path_ == "") {
        TIDAS_THROW("cannot open sqlite db with empty path");
    }

    if (!sql_) {
        int64_t size = fs_stat(path_.c_str());

        // std::cerr << "TIDAS open sqlite DB at " << path_ << std::endl;

        int flags = 0;

        if (size > 0) {
            // just open

            if (mode_ == access_mode::write) {
                flags = flags | SQLITE_OPEN_READWRITE;
            } else {
                flags = flags | SQLITE_OPEN_READONLY;
            }

            ret = sqlite3_open_v2(path_.c_str(), &sql_, flags, NULL);
            SQLERR(ret != SQLITE_OK, "open");
        } else {
            // create and initialize schema

            if (mode_ == access_mode::write) {
                init(path_);

                flags = flags | SQLITE_OPEN_READWRITE;

                ret = sqlite3_open_v2(path_.c_str(), &sql_, flags, NULL);
                SQLERR(ret != SQLITE_OK, "open");
            } else {
                ostringstream o;
                o << "cannot create sqlite DB \"" << path_ << "\" in read-only mode";
                TIDAS_THROW(o.str().c_str());
            }
        }

        ret = sqlite3_busy_timeout(sql_, 60000);
        SQLERR(ret != SQLITE_OK, "pragma busy timeout");

        ret = sqlite3_exec(sql_, "PRAGMA TEMP_STORE = MEMORY", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma temp_store");

        ret = sqlite3_exec(sql_, "PRAGMA JOURNAL_MODE = PERSIST", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma journal_mode");

        ret = sqlite3_exec(sql_, "PRAGMA SYNCHRONOUS = NORMAL", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma synchronous");

        ret = sqlite3_exec(sql_, "PRAGMA LOCKING_MODE = NORMAL", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma locking_mode");

        ret = sqlite3_exec(sql_, "PRAGMA PAGE_SIZE = 4096", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma page_size");

        ret = sqlite3_exec(sql_, "PRAGMA CACHE_SIZE = 2000", NULL, NULL, &sqlerr);
        SQLERR(ret != SQLITE_OK, "pragma cache_size");


        // Create all prepared statements needed

        ostringstream command;
        command.precision(20);

        command.str("");
        command <<
            "INSERT OR REPLACE INTO dct_grp ( parent, data, types ) VALUES ( @parent, @data, @types );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_dictgroup_ins_),
                                 NULL);
        SQLERR(ret != SQLITE_OK, "dict group insert prepare");

        command.str("");
        command <<
            "INSERT OR REPLACE INTO dct_intrvl ( parent, data, types ) VALUES ( @parent, @data, @types );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_dictinterval_ins_),
                                 NULL);
        SQLERR(ret != SQLITE_OK, "dict interval insert prepare");

        command.str("");
        command << "DELETE FROM dct_grp WHERE parent = @parent;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_dictgroup_del_),
                                 NULL);
        SQLERR(ret != SQLITE_OK, "dict group delete prepare");

        command.str("");
        command << "DELETE FROM dct_intrvl WHERE parent = @parent;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_dictinterval_del_),
                                 NULL);
        SQLERR(ret != SQLITE_OK, "dict interval delete prepare");

        command.str("");
        command <<
            "INSERT OR REPLACE INTO schm ( parent, fields ) VALUES ( @parent, @fields );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_schema_ins_), NULL);
        SQLERR(ret != SQLITE_OK, "schema insert prepare");

        command.str("");
        command << "DELETE FROM schm WHERE parent = @parent;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_schema_del_), NULL);
        SQLERR(ret != SQLITE_OK, "schema delete prepare");

        command.str("");
        command <<
            "INSERT OR REPLACE INTO grp ( path, parent, nsamp, start, stop, counts ) VALUES ( @path, @parent, @nsamp, @start, @stop, @counts );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_group_ins_), NULL);
        SQLERR(ret != SQLITE_OK, "group insert prepare");

        command.str("");
        command << "DELETE FROM grp WHERE path = @path;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_group_del_), NULL);
        SQLERR(ret != SQLITE_OK, "group delete prepare");

        command.str("");
        command <<
            "INSERT OR REPLACE INTO intrvl ( path, parent, size ) VALUES ( @path, @parent, @size );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_interval_ins_), NULL);
        SQLERR(ret != SQLITE_OK, "interval insert prepare");

        command.str("");
        command << "DELETE FROM intrvl WHERE path = @path;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_interval_del_), NULL);
        SQLERR(ret != SQLITE_OK, "interval delete prepare");

        command.str("");
        command <<
            "INSERT OR REPLACE INTO blk ( path, parent ) VALUES ( @path, @parent );";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_block_ins_), NULL);
        SQLERR(ret != SQLITE_OK, "block insert prepare");

        command.str("");
        command << "DELETE FROM blk WHERE path = @path;";
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &(stmt_block_del_), NULL);
        SQLERR(ret != SQLITE_OK, "block delete prepare");
    }

    return;
}

void tidas::indexdb_sql::close() {
    if (sql_) {
        // free all prepared statements

        int ret = sqlite3_finalize(stmt_dictgroup_ins_);
        SQLERR(ret != SQLITE_OK, "dict group insert finalize");

        ret = sqlite3_finalize(stmt_dictgroup_del_);
        SQLERR(ret != SQLITE_OK, "dict group delete finalize");

        ret = sqlite3_finalize(stmt_dictinterval_ins_);
        SQLERR(ret != SQLITE_OK, "dict interval insert finalize");

        ret = sqlite3_finalize(stmt_dictinterval_del_);
        SQLERR(ret != SQLITE_OK, "dict interval delete finalize");

        ret = sqlite3_finalize(stmt_schema_ins_);
        SQLERR(ret != SQLITE_OK, "schema insert finalize");

        ret = sqlite3_finalize(stmt_schema_del_);
        SQLERR(ret != SQLITE_OK, "schema delete finalize");

        ret = sqlite3_finalize(stmt_group_ins_);
        SQLERR(ret != SQLITE_OK, "group insert finalize");

        ret = sqlite3_finalize(stmt_group_del_);
        SQLERR(ret != SQLITE_OK, "group delete finalize");

        ret = sqlite3_finalize(stmt_interval_ins_);
        SQLERR(ret != SQLITE_OK, "interval insert finalize");

        ret = sqlite3_finalize(stmt_interval_del_);
        SQLERR(ret != SQLITE_OK, "interval delete finalize");

        ret = sqlite3_finalize(stmt_block_ins_);
        SQLERR(ret != SQLITE_OK, "block insert finalize");

        ret = sqlite3_finalize(stmt_block_del_);
        SQLERR(ret != SQLITE_OK, "block delete finalize");

        // close DB

        ret = sqlite3_close_v2(sql_);
        SQLERR(ret != SQLITE_OK, "close");
        sql_ = NULL;

        // std::cerr << "TIDAS closed sqlite DB" << std::endl;
    }

    return;
}

void tidas::indexdb_sql::sql_err(bool err, char const * msg, char const * file,
                                 int line) {
    if (err) {
        ostringstream o;
        o << "sqlite DB error at " << msg << " (" << file << ", line " << line <<
            "):  \"" << sqlite3_errmsg(sql_) << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    return;
}

void tidas::indexdb_sql::ops_begin() {
    // std::cerr << "TIDAS:  sqlite ops_begin()" << std::endl;
    open();

    // sql_t1_ = chrono::steady_clock::now();
    // std::cerr << "DBG: BEGIN TRANSACTION" << std::endl;

    ostringstream command;
    command.str("");

    command << "BEGIN TRANSACTION;";

    char * sqlerr;

    int ret = sqlite3_exec(sql_, command.str().c_str(), NULL, NULL, &sqlerr);
    SQLERR(ret != SQLITE_OK, "begin transaction");

    return;
}

void tidas::indexdb_sql::ops_end() {
    // std::cerr << "TIDAS:  sqlite ops_end()" << std::endl;
    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    ostringstream command;
    command.str("");

    command << "END TRANSACTION;";

    char * sqlerr;

    int ret = sqlite3_exec(sql_, command.str().c_str(), NULL, NULL, &sqlerr);
    SQLERR(ret != SQLITE_OK, "end transaction");

    // sql_t2_ = chrono::steady_clock::now();
    // auto diff = sql_t2_ - sql_t1_;
    // double sec = 0.001 * chrono::duration<double, milli>(diff).count();
    // std::cerr << "DBG: END TRANSACTION at " << sec << " seconds" << std::endl;

    close();
    return;
}

void tidas::indexdb_sql::ops_dict(backend_path loc, indexdb_op op, map <string,
                                                                        string> const & data, map <string,
                                                                                                   data_type> const & types)
{
    int ret;

    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    if (mode_ != access_mode::write) {
        ostringstream o;
        o << "cannot modify read-only db \"" << path_ << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "DBG ops_dict path = " << dpath << std::endl;

    sqlite3_stmt * stmt;

    if ((op == indexdb_op::add) || (op == indexdb_op::update)) {
        size_t pos = dpath.find(block_fs_group_dir);
        if (pos != string::npos) {
            stmt = stmt_dictgroup_ins_;
        } else {
            pos = dpath.find(block_fs_intervals_dir);
            if (pos != string::npos) {
                stmt = stmt_dictinterval_ins_;
            } else {
                TIDAS_THROW("dictionary path not associated with group or intervals");
            }
        }

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "dict bind parent");

        ostringstream datastr;
        {
            cereal::PortableBinaryOutputArchive outdata(datastr);
            outdata(data);
        }

        ret = sqlite3_bind_blob(stmt, 2, (void *)datastr.str().c_str(),
                                datastr.str().size() + 1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "dict bind data");

        ostringstream typestr;
        {
            cereal::PortableBinaryOutputArchive outtype(typestr);
            outtype(types);
        }

        ret = sqlite3_bind_blob(stmt, 3, (void *)typestr.str().c_str(),
                                typestr.str().size() + 1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "dict bind types");
    } else if (op == indexdb_op::del) {
        size_t pos = dpath.find(block_fs_group_dir);
        if (pos != string::npos) {
            stmt = stmt_dictgroup_del_;
        } else {
            pos = dpath.find(block_fs_intervals_dir);
            if (pos != string::npos) {
                stmt = stmt_dictinterval_del_;
            } else {
                TIDAS_THROW("dictionary path not associated with group or intervals");
            }
        }

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "dict bind parent");
    } else {
        TIDAS_THROW("unknown indexdb dict operation");
    }

    ret = sqlite3_step(stmt);
    SQLERR(ret != SQLITE_DONE, "dict step");

    // ret = sqlite3_clear_bindings ( stmt );
    // SQLERR( ret != SQLITE_OK, "dict clear" );

    ret = sqlite3_reset(stmt);
    SQLERR(ret != SQLITE_OK, "dict reset");

    return;
}

void tidas::indexdb_sql::ops_schema(backend_path loc, indexdb_op op,
                                    field_list const & fields) {
    int ret;

    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    if (mode_ != access_mode::write) {
        ostringstream o;
        o << "cannot modify read-only db \"" << path_ << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "DBG ops_schema path = " << dpath << std::endl;

    sqlite3_stmt * stmt;

    if ((op == indexdb_op::add) || (op == indexdb_op::update)) {
        stmt = stmt_schema_ins_;

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "schema bind parent");

        ostringstream fieldstr;
        {
            cereal::PortableBinaryOutputArchive fielddata(fieldstr);
            fielddata(fields);
        }

        ret = sqlite3_bind_blob(stmt, 2, (void *)fieldstr.str().c_str(),
                                fieldstr.str().size() + 1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "schema bind fields");
    } else if (op == indexdb_op::del) {
        stmt = stmt_schema_del_;

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "schema bind parent");
    } else {
        TIDAS_THROW("unknown indexdb schema operation");
    }

    ret = sqlite3_step(stmt);
    SQLERR(ret != SQLITE_DONE, "schema step");

    // ret = sqlite3_clear_bindings ( stmt );
    // SQLERR( ret != SQLITE_OK, "schema clear" );

    ret = sqlite3_reset(stmt);
    SQLERR(ret != SQLITE_OK, "schema reset");

    return;
}

void tidas::indexdb_sql::ops_group(backend_path loc, indexdb_op op,
                                   index_type const & nsamp, time_type const & start,
                                   time_type const & stop, map <data_type,
                                                                size_t> const & counts)
{
    int ret;

    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    if (mode_ != access_mode::write) {
        ostringstream o;
        o << "cannot modify read-only db \"" << path_ << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "DBG ops_group path = " << dpath << std::endl;

    sqlite3_stmt * stmt;

    if ((op == indexdb_op::add) || (op == indexdb_op::update)) {
        stmt = stmt_group_ins_;

        size_t plen = dpath.find(path_sep + block_fs_group_dir + path_sep);
        if (plen == string::npos) {
            ostringstream o;
            o << "\"" << dpath << "\" is not a valid group path";
            TIDAS_THROW(o.str().c_str());
        }

        string parent = dpath.substr(0, plen);

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "group bind path");

        ret = sqlite3_bind_text(stmt, 2, parent.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "group bind parent");

        ret = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)nsamp);
        SQLERR(ret != SQLITE_OK, "group bind nsamp");

        ret = sqlite3_bind_double(stmt, 4, start);
        SQLERR(ret != SQLITE_OK, "group bind start");

        ret = sqlite3_bind_double(stmt, 5, stop);
        SQLERR(ret != SQLITE_OK, "group bind stop");

        ostringstream countstr;
        {
            cereal::PortableBinaryOutputArchive countdata(countstr);
            countdata(counts);
        }

        ret = sqlite3_bind_blob(stmt, 6, (void *)countstr.str().c_str(),
                                countstr.str().size() + 1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "group bind counts");
    } else if (op == indexdb_op::del) {
        stmt = stmt_group_del_;

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "group bind path");
    } else {
        TIDAS_THROW("unknown indexdb group operation");
    }

    ret = sqlite3_step(stmt);
    SQLERR(ret != SQLITE_DONE, "group step");

    // ret = sqlite3_clear_bindings ( stmt );
    // SQLERR( ret != SQLITE_OK, "group clear" );

    ret = sqlite3_reset(stmt);
    SQLERR(ret != SQLITE_OK, "group reset");

    return;
}

void tidas::indexdb_sql::ops_intervals(backend_path loc, indexdb_op op,
                                       size_t const & size) {
    int ret;

    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    if (mode_ != access_mode::write) {
        ostringstream o;
        o << "cannot modify read-only db \"" << path_ << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "DBG ops_intervals path = " << dpath << std::endl;

    sqlite3_stmt * stmt;

    if ((op == indexdb_op::add) || (op == indexdb_op::update)) {
        stmt = stmt_interval_ins_;

        size_t plen = dpath.find(path_sep + block_fs_intervals_dir + path_sep);
        if (plen == string::npos) {
            ostringstream o;
            o << "\"" << dpath << "\" is not a valid intervals path";
            TIDAS_THROW(o.str().c_str());
        }

        string parent = dpath.substr(0, plen);

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "intervals bind path");

        ret = sqlite3_bind_text(stmt, 2, parent.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "intervals bind parent");

        ret = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)size);
        SQLERR(ret != SQLITE_OK, "intervals bind size");
    } else if (op == indexdb_op::del) {
        stmt = stmt_interval_del_;

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "intervals bind path");
    } else {
        TIDAS_THROW("unknown indexdb intervals operation");
    }

    ret = sqlite3_step(stmt);
    SQLERR(ret != SQLITE_DONE, "intervals step");

    // ret = sqlite3_clear_bindings ( stmt );
    // SQLERR( ret != SQLITE_OK, "intervals clear" );

    ret = sqlite3_reset(stmt);
    SQLERR(ret != SQLITE_OK, "intervals reset");

    return;
}

void tidas::indexdb_sql::ops_block(backend_path loc, indexdb_op op) {
    int ret;

    if (!sql_) {
        TIDAS_THROW("sqlite DB is not open");
    }

    if (mode_ != access_mode::write) {
        ostringstream o;
        o << "cannot modify read-only db \"" << path_ << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "DBG ops_block path = " << dpath << std::endl;

    sqlite3_stmt * stmt;

    if ((op == indexdb_op::add) || (op == indexdb_op::update)) {
        stmt = stmt_block_ins_;

        string parent = indexdb_top_parent;
        size_t plen = dpath.rfind(path_sep);
        if (plen != string::npos) {
            parent = dpath.substr(0, plen);
        }

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "block bind path");

        ret = sqlite3_bind_text(stmt, 2, parent.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "block bind parent");
    } else if (op == indexdb_op::del) {
        stmt = stmt_block_del_;

        ret = sqlite3_bind_text(stmt, 1, dpath.c_str(), -1, SQLITE_TRANSIENT);
        SQLERR(ret != SQLITE_OK, "block bind path");
    } else {
        TIDAS_THROW("unknown indexdb block operation");
    }

    ret = sqlite3_step(stmt);
    SQLERR(ret != SQLITE_DONE, "block step");

    // ret = sqlite3_clear_bindings ( stmt );
    // SQLERR( ret != SQLITE_OK, "block clear" );

    ret = sqlite3_reset(stmt);
    SQLERR(ret != SQLITE_OK, "block reset");

    return;
}

void tidas::indexdb_sql::add_dict(backend_path loc, map <string, string> const & data,
                                  map <string, data_type> const & types) {
    open();
    ops_begin();
    ops_dict(loc, indexdb_op::add, data, types);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::update_dict(backend_path loc, map <string,
                                                            string> const & data,
                                     map <string,
                                          data_type> const & types)
{
    open();
    ops_begin();
    ops_dict(loc, indexdb_op::update, data, types);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::del_dict(backend_path loc) {
    map <string, string> fakedata;
    map <string, data_type> faketypes;
    open();
    ops_begin();
    ops_dict(loc, indexdb_op::del, fakedata, faketypes);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::add_schema(backend_path loc, field_list const & fields) {
    open();
    ops_begin();
    ops_schema(loc, indexdb_op::add, fields);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::update_schema(backend_path loc, field_list const & fields) {
    open();
    ops_begin();
    ops_schema(loc, indexdb_op::update, fields);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::del_schema(backend_path loc) {
    field_list fakefields;
    open();
    ops_begin();
    ops_schema(loc, indexdb_op::del, fakefields);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::add_group(backend_path loc, index_type const & nsamp,
                                   time_type const & start, time_type const & stop,
                                   map <data_type, size_t> const & counts) {
    open();
    ops_begin();
    ops_group(loc, indexdb_op::add, nsamp, start, stop, counts);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::update_group(backend_path loc, index_type const & nsamp,
                                      time_type const & start, time_type const & stop,
                                      map <data_type, size_t> const & counts) {
    open();
    ops_begin();
    ops_group(loc, indexdb_op::update, nsamp, start, stop, counts);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::del_group(backend_path loc) {
    map <data_type, size_t> fakecounts;
    open();
    ops_begin();
    ops_group(loc, indexdb_op::del, 0, 0.0, 0.0, fakecounts);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::add_intervals(backend_path loc, size_t const & size) {
    open();
    ops_begin();
    ops_intervals(loc, indexdb_op::add, size);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::update_intervals(backend_path loc, size_t const & size) {
    open();
    ops_begin();
    ops_intervals(loc, indexdb_op::update, size);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::del_intervals(backend_path loc) {
    open();
    ops_begin();
    ops_intervals(loc, indexdb_op::del, 0);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::add_block(backend_path loc) {
    open();
    ops_begin();
    ops_block(loc, indexdb_op::add);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::update_block(backend_path loc) {
    open();
    ops_begin();
    ops_block(loc, indexdb_op::update);
    ops_end();
    close();
    return;
}

void tidas::indexdb_sql::del_block(backend_path loc) {
    open();
    ops_begin();
    ops_block(loc, indexdb_op::del);
    ops_end();
    close();
    return;
}

bool tidas::indexdb_sql::query_dict(backend_path loc, map <string, string> & data,
                                    map <string, data_type> & types) {
    bool found = false;

    open();

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    ostringstream command;
    command.precision(20);

    size_t pos = dpath.find(block_fs_group_dir);
    if (pos != string::npos) {
        command.str("");
        command << "SELECT * FROM dct_grp WHERE parent = \"" << dpath << "\";";
    } else {
        pos = dpath.find(block_fs_intervals_dir);
        if (pos != string::npos) {
            command.str("");
            command << "SELECT * FROM dct_intrvl WHERE parent = \"" << dpath << "\";";
        } else {
            TIDAS_THROW("dictionary path not associated with group or intervals");
        }
    }

    sqlite3_stmt * stmt;
    int ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
    SQLERR(ret != SQLITE_OK, "dict query prepare");

    ret = sqlite3_step(stmt);

    if (ret == SQLITE_ROW) {
        char const * rawstr =
            reinterpret_cast <char const *> (sqlite3_column_blob(stmt, 1));

        size_t bytes = sqlite3_column_bytes(stmt, 1);

        string datablobstr(rawstr, bytes);

        istringstream datastr(datablobstr);
        {
            cereal::PortableBinaryInputArchive indata(datastr);
            indata(data);
        }

        rawstr = reinterpret_cast <char const *> (sqlite3_column_blob(stmt, 2));

        bytes = sqlite3_column_bytes(stmt, 2);

        string typeblobstr(rawstr, bytes);

        istringstream typestr(typeblobstr);
        {
            cereal::PortableBinaryInputArchive intypes(typestr);
            intypes(types);
        }

        found = true;
    }

    ret = sqlite3_finalize(stmt);
    SQLERR(ret != SQLITE_OK, "dict query finalize");

    close();

    return found;
}

bool tidas::indexdb_sql::query_schema(backend_path loc, field_list & fields) {
    bool found = false;

    open();

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    ostringstream command;
    command.precision(20);

    command.str("");
    command << "SELECT * FROM schm WHERE parent = \"" << dpath << "\";";

    sqlite3_stmt * stmt;
    int ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
    SQLERR(ret != SQLITE_OK, "schema query prepare");

    ret = sqlite3_step(stmt);

    if (ret == SQLITE_ROW) {
        char const * rawstr =
            reinterpret_cast <char const *> (sqlite3_column_blob(stmt, 1));

        size_t bytes = sqlite3_column_bytes(stmt, 1);

        string blobstr(rawstr, bytes);

        istringstream fieldstr(blobstr);
        {
            cereal::PortableBinaryInputArchive infields(fieldstr);
            infields(fields);
        }

        found = true;
    }

    ret = sqlite3_finalize(stmt);
    SQLERR(ret != SQLITE_OK, "schema query finalize");

    close();

    return found;
}

bool tidas::indexdb_sql::query_group(backend_path loc, index_type & nsamp,
                                     time_type & start, time_type & stop,
                                     map <data_type, size_t> & counts) {
    bool found = false;

    open();

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    ostringstream command;
    command.precision(20);

    command.str("");
    command << "SELECT * FROM grp WHERE path = \"" << dpath << "\";";

    sqlite3_stmt * stmt;
    int ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
    SQLERR(ret != SQLITE_OK, "group query prepare");

    ret = sqlite3_step(stmt);
    if (ret == SQLITE_ROW) {
        nsamp = (index_type)sqlite3_column_int64(stmt, 2);
        start = sqlite3_column_double(stmt, 3);
        stop = sqlite3_column_double(stmt, 4);

        char const * rawstr =
            reinterpret_cast <char const *> (sqlite3_column_blob(stmt, 5));

        size_t bytes = sqlite3_column_bytes(stmt, 5);

        string blobstr(rawstr, bytes);

        istringstream countstr(blobstr);
        {
            cereal::PortableBinaryInputArchive incounts(countstr);
            incounts(counts);
        }

        found = true;
    }

    ret = sqlite3_finalize(stmt);
    SQLERR(ret != SQLITE_OK, "group query finalize");

    close();

    return found;
}

bool tidas::indexdb_sql::query_intervals(backend_path loc, size_t & size) {
    bool found = false;

    open();

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    ostringstream command;
    command.precision(20);

    command.str("");
    command << "SELECT * FROM intrvl WHERE path = \"" << dpath << "\";";

    sqlite3_stmt * stmt;
    int ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
    SQLERR(ret != SQLITE_OK, "intervals query prepare");

    ret = sqlite3_step(stmt);

    if (ret == SQLITE_ROW) {
        size = (index_type)sqlite3_column_int64(stmt, 2);
        found = true;
    }

    ret = sqlite3_finalize(stmt);
    SQLERR(ret != SQLITE_OK, "intervals query finalize");

    close();

    return found;
}

bool tidas::indexdb_sql::query_block(backend_path loc, vector <string> & child_blocks,
                                     vector <string> & child_groups,
                                     vector <string> & child_intervals) {
    bool found = false;

    open();

    child_blocks.clear();
    child_groups.clear();
    child_intervals.clear();

    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    ostringstream command;
    command.precision(20);

    command.str("");
    command << "SELECT * FROM blk WHERE path = \"" << dpath << "\";";

    // std::cerr << command.str() << std::endl;

    sqlite3_stmt * bstmt;
    int ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &bstmt, NULL);
    SQLERR(ret != SQLITE_OK, "block query prepare");

    ret = sqlite3_step(bstmt);

    if (ret == SQLITE_ROW) {
        string dir;
        string base;

        // query direct descendents

        command.str("");
        command << "SELECT * FROM intrvl WHERE parent = \"" << dpath << "\";";

        sqlite3_stmt * stmt;
        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
        SQLERR(ret != SQLITE_OK, "intervals child query prepare");

        ret = sqlite3_step(stmt);
        while (ret == SQLITE_ROW) {
            string result = (char *)sqlite3_column_text(stmt, 0);
            indexdb_path_split(result, dir, base);
            child_intervals.push_back(base);
            ret = sqlite3_step(stmt);
        }

        ret = sqlite3_finalize(stmt);
        SQLERR(ret != SQLITE_OK, "intervals child query finalize");

        command.str("");
        command << "SELECT * FROM grp WHERE parent = \"" << dpath << "\";";

        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
        SQLERR(ret != SQLITE_OK, "group child query prepare");

        ret = sqlite3_step(stmt);
        while (ret == SQLITE_ROW) {
            string result = (char *)sqlite3_column_text(stmt, 0);
            indexdb_path_split(result, dir, base);
            child_groups.push_back(base);
            ret = sqlite3_step(stmt);
        }

        ret = sqlite3_finalize(stmt);
        SQLERR(ret != SQLITE_OK, "group child query finalize");

        command.str("");
        command << "SELECT * FROM blk WHERE parent = \"" << dpath << "\";";

        ret = sqlite3_prepare_v2(sql_, command.str().c_str(),
                                 command.str().size() + 1, &stmt, NULL);
        SQLERR(ret != SQLITE_OK, "block child query prepare");

        ret = sqlite3_step(stmt);
        while (ret == SQLITE_ROW) {
            string result = (char *)sqlite3_column_text(stmt, 0);
            indexdb_path_split(result, dir, base);
            child_blocks.push_back(base);
            ret = sqlite3_step(stmt);
        }

        ret = sqlite3_finalize(stmt);
        SQLERR(ret != SQLITE_OK, "block child query finalize");

        found = true;
    }

    ret = sqlite3_finalize(bstmt);
    SQLERR(ret != SQLITE_OK, "block query finalize");

    close();

    return found;
}

void tidas::indexdb_sql::commit(deque <indexdb_transaction> const & trans) {
    // std::cerr << "TIDAS:  sqlite commit " << trans.size() << " transactions" <<
    // std::endl;

    if (trans.size() == 0) {
        return;
    }

    open();

    ops_begin();

    for (auto tr : trans) {
        // we need to add our full volume path before calling the ops methods.

        string fullpath;
        if (volpath_ == "") {
            fullpath = tr.obj->path;

            // std::cerr << "commit volpath empty: " << fullpath << std::endl;
        } else {
            fullpath = volpath_ + path_sep + tr.obj->path;

            // std::cerr << "commit volpath non-empty: " << fullpath << std::endl;
        }

        backend_path loc;

        size_t pos = fullpath.rfind(path_sep);

        loc.path = fullpath.substr(0, pos);
        loc.name = fullpath.substr(pos + 1);

        indexdb_dict * dp;
        indexdb_schema * sp;
        indexdb_group * gp;
        indexdb_intervals * tp;
        indexdb_block * bp;

        switch (tr.obj->type) {
            case indexdb_object_type::dict:
                dp = dynamic_cast <indexdb_dict *> (tr.obj.get());
                ops_dict(loc, tr.op, dp->data, dp->types);
                break;

            case indexdb_object_type::schema:
                sp = dynamic_cast <indexdb_schema *> (tr.obj.get());
                ops_schema(loc, tr.op, sp->fields);
                break;

            case indexdb_object_type::group:
                gp = dynamic_cast <indexdb_group *> (tr.obj.get());
                ops_group(loc, tr.op, gp->nsamp, gp->start, gp->stop, gp->counts);
                break;

            case indexdb_object_type::intervals:
                tp = dynamic_cast <indexdb_intervals *> (tr.obj.get());
                ops_intervals(loc, tr.op, tp->size);
                break;

            case indexdb_object_type::block:
                bp = dynamic_cast <indexdb_block *> (tr.obj.get());
                ops_block(loc, tr.op);
                break;

            default:
                TIDAS_THROW("unknown indexdb object type");
                break;
        }
    }

    ops_end();

    close();

    return;
}

void tidas::indexdb_sql::tree_node(backend_path loc, std::string const & filter,
                                   std::deque <indexdb_transaction> & trans) {
    string path = loc.path + path_sep + loc.name;

    string dpath = dbpath(path);

    // std::cerr << "tree_node " << path << " --> " << dpath << std::endl;

    vector <string> child_blocks;
    vector <string> child_groups;
    vector <string> child_intervals;

    bool bfound = query_block(loc, child_blocks, child_groups, child_intervals);

    if (!bfound) {
        ostringstream o;
        o << "cannot find block \"" << (loc.path + path_sep + loc.name) <<
            "\" when exporting tree";
        TIDAS_THROW(o.str().c_str());
    }

    // add transaction for this block

    indexdb_block b;
    b.type = indexdb_object_type::block;
    b.path = dpath;

    indexdb_transaction tr;
    tr.op = indexdb_op::add;
    tr.obj.reset(b.clone());

    trans.push_back(tr);

    // cerr << "tree_node filter = \"" << filter << "\"" << endl;

    // split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX[X=X]/ --->
    // [XX=XX,XX=XX] XXXX[XX=XX]/XXX[X=X]/

    string filt_local;
    string filt_sub;
    bool stop;

    filter_block(filter, filt_local, filt_sub, stop);

    // split local filter string:  [XX=XX,XX=XX]

    map <string, string> filts = filter_split(filt_local);

    // select groups.  Since we are not copying objects to a
    // new location, we strip off any sub-object filters.

    string filt_name;
    string filt_pass;

    // XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]

    filter_sub(filts[group_submatch_key], filt_name, filt_pass);

    // cerr << "tree_node group filter = \"" << filt_name << "\"" << endl;

    regex groupre(filter_default(filt_name), std::regex::extended);

    backend_path chloc;

    for (auto const & c : child_groups) {
        // cerr << "tree_node checking group " << c << endl;
        if (regex_match(c, groupre)) {
            // cerr << "tree_node  using group " << c << endl;
            chloc = loc;
            chloc.path = chloc.path + path_sep + chloc.name + path_sep +
                         block_fs_group_dir;
            chloc.name = c;

            string child_path = chloc.path + path_sep + chloc.name;
            string chpath = dbpath(child_path);

            index_type nsamp;
            time_type start;
            time_type stop;
            map <data_type, size_t> counts;

            bool found = query_group(chloc, nsamp, start, stop, counts);

            if (!found) {
                ostringstream o;
                o << "inconsistency in group \"" << child_path <<
                    "\" when exporting tree";
                TIDAS_THROW(o.str().c_str());
            }

            indexdb_group g;
            g.type = indexdb_object_type::group;
            g.path = chpath;
            g.nsamp = nsamp;
            g.counts = counts;
            g.start = start;
            g.stop = stop;

            indexdb_transaction tr;
            tr.op = indexdb_op::add;
            tr.obj.reset(g.clone());

            trans.push_back(tr);

            // now get the schema and dict

            field_list fields;

            found = query_schema(chloc, fields);

            if (!found) {
                ostringstream o;
                o << "inconsistency in schema \"" << child_path <<
                    "\" when exporting tree";
                TIDAS_THROW(o.str().c_str());
            }

            indexdb_schema s;
            s.type = indexdb_object_type::schema;
            s.path = chpath;
            s.fields = fields;

            tr.op = indexdb_op::add;
            tr.obj.reset(s.clone());

            trans.push_back(tr);

            map <string, string> data;
            map <string, data_type> types;

            found = query_dict(chloc, data, types);

            if (!found) {
                ostringstream o;
                o << "inconsistency in dict \"" << child_path <<
                    "\" when exporting tree";
                TIDAS_THROW(o.str().c_str());
            }

            indexdb_dict d;
            d.type = indexdb_object_type::dict;
            d.path = chpath;
            d.data = data;
            d.types = types;

            tr.op = indexdb_op::add;
            tr.obj.reset(d.clone());

            trans.push_back(tr);
        }
    }

    // select intervals.  Again, we strip off sub-filters.

    // XXX[dict=XXX] ---> XXX [dict=XXX]

    filter_sub(filts[intervals_submatch_key], filt_name, filt_pass);

    // cerr << "tree_node intervals filter = \"" << filt_name << "\"" << endl;

    regex intre(filter_default(filt_name), std::regex::extended);

    for (auto const & c : child_intervals) {
        // cerr << "tree_node checking intervals " << c << endl;
        if (regex_match(c, intre)) {
            // cerr << "tree_node  using intervals " << c << endl;

            chloc = loc;
            chloc.path = chloc.path + path_sep + chloc.name + path_sep +
                         block_fs_intervals_dir;
            chloc.name = c;

            string child_path = chloc.path + path_sep + chloc.name;
            string chpath = dbpath(child_path);

            size_t size;

            bool found = query_intervals(chloc, size);

            if (!found) {
                ostringstream o;
                o << "inconsistency in intervals \"" << child_path <<
                    "\" when exporting tree";
                TIDAS_THROW(o.str().c_str());
            }

            indexdb_intervals t;
            t.type = indexdb_object_type::intervals;
            t.path = chpath;
            t.size = size;

            indexdb_transaction tr;
            tr.op = indexdb_op::add;
            tr.obj.reset(t.clone());

            trans.push_back(tr);

            // now get the dict

            map <string, string> data;
            map <string, data_type> types;

            found = query_dict(chloc, data, types);

            if (!found) {
                ostringstream o;
                o << "inconsistency in dict \"" << child_path <<
                    "\" when exporting tree";
                TIDAS_THROW(o.str().c_str());
            }

            indexdb_dict d;
            d.type = indexdb_object_type::dict;
            d.path = chpath;
            d.data = data;
            d.types = types;

            tr.op = indexdb_op::add;
            tr.obj.reset(d.clone());

            trans.push_back(tr);
        }
    }

    // select sub-blocks

    // extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX
    // [XX=XX]/XXX
    // check if we have reached a hard stop at this depth, and in this case ignore all
    // sub-blocks.

    if (!stop) {
        filter_sub(filt_sub, filt_name, filt_pass);

        regex blockre(filter_default(filt_name), std::regex::extended);

        // cerr << "tree_node block filter = \"" << filt_name << "\"" << endl;

        for (auto const & c : child_blocks) {
            // cerr << "tree_node checking block " << c << endl;
            if (regex_match(c, blockre)) {
                // cerr << "tree_node  using block " << c << endl;

                chloc = loc;
                chloc.path = chloc.path + path_sep + chloc.name;
                chloc.name = c;

                tree_node(chloc, filt_pass, trans);
            } else {
                // cerr << "tree_node  rejecting block " << c << endl;
            }
        }
    }

    return;
}

void tidas::indexdb_sql::tree(backend_path root, std::string const & filter,
                              std::deque <indexdb_transaction> & result) {
    result.clear();

    tree_node(root, filter, result);

    return;
}

CEREAL_REGISTER_TYPE(tidas::indexdb_sql);
