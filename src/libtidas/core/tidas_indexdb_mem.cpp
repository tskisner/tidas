/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <cstdio>


using namespace std;
using namespace tidas;



void tidas::indexdb_path_split ( string const & in, string & dir, string & base ) {
    size_t pos = in.rfind ( path_sep );
    if ( ( pos == 0 ) || ( pos == string::npos ) ) {
        dir = "";
        base = in;
    } else {
        dir = in.substr ( 0, pos );
        base = in.substr ( pos + 1 );
    }
    return;
}


tidas::indexdb_object::~indexdb_object() {

}


indexdb_object * tidas::indexdb_dict::clone () const {
    return new indexdb_dict ( *this );
}


indexdb_object * tidas::indexdb_schema::clone () const {
    return new indexdb_schema ( *this );
}


indexdb_object * tidas::indexdb_group::clone () const {
    return new indexdb_group ( *this );
}


indexdb_object * tidas::indexdb_intervals::clone () const {
    return new indexdb_intervals ( *this );
}


indexdb_object * tidas::indexdb_block::clone () const {
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


void tidas::indexdb_transaction::print ( ostream & out ) const {

    switch ( op ) {
        case indexdb_op::add :
            out << "TR: ADD" << endl;
            break;
        case indexdb_op::update :
            out << "TR: UPDATE" << endl;
            break;
        case indexdb_op::del :
            out << "TR: DEL" << endl;
            break;
        default :
            TIDAS_THROW( "unknown indexdb transaction operator" );
            break;
    }

    indexdb_dict * dp;
    indexdb_schema * sp;
    indexdb_group * gp;
    indexdb_intervals * tp;
    indexdb_block * bp;

    switch ( obj->type ) {
        case indexdb_object_type::dict :
            dp = dynamic_cast < indexdb_dict * > ( obj.get() );
            out << "TR:   dict " << dp->path << endl;
            for ( auto d : dp->data ) {
                out << "TR:     " << d.first << " = " << d.second << " (" << data_type_to_string( dp->types[ d.first ] ) << ")" << endl;
            }
            break;
        case indexdb_object_type::schema :
            sp = dynamic_cast < indexdb_schema * > ( obj.get() );
            out << "TR:   schema " << sp->path << endl;
            for ( auto f : sp->fields ) {
                out << "TR:     " << f.name << " : " << f.units << " : " << data_type_to_string( f.type ) << endl;
            }
            break;
        case indexdb_object_type::group :
            gp = dynamic_cast < indexdb_group * > ( obj.get() );
            out << "TR:   group " << gp->path << endl;
            out << "TR:     " << gp->nsamp << " samples, " << gp->start << " - " << gp->stop << endl;
            out << "TR:     counts:" << endl;
            for ( auto g : gp->counts ) {
                out << "TR:       " << data_type_to_string( g.first ) << " = " << g.second << endl;
            }
            break;
        case indexdb_object_type::intervals :
            tp = dynamic_cast < indexdb_intervals * > ( obj.get() );
            out << "TR:   intervals " << tp->path << endl;
            out << "TR:     " << tp->size << " regions" << endl;
            break;
        case indexdb_object_type::block :
            bp = dynamic_cast < indexdb_block * > ( obj.get() );
            out << "TR:   block " << bp->path << endl;
            break;
        default :
            TIDAS_THROW( "unknown indexdb object type" );
            break;
    }

    return;
}


tidas::indexdb::~indexdb () {

}


tidas::indexdb_mem::indexdb_mem () : indexdb() {

}


tidas::indexdb_mem::~indexdb_mem () {

}


indexdb_mem & tidas::indexdb_mem::operator= ( indexdb_mem const & other ) {
    if ( this != &other ) {
        copy ( other );
    }
    return *this;
}


tidas::indexdb_mem::indexdb_mem ( indexdb_mem const & other ) : indexdb( other ) {
    copy ( other );
}


void tidas::indexdb_mem::copy ( indexdb_mem const & other ) {
    history_ = other.history_;

    data_dict_ = other.data_dict_;
    data_schema_ = other.data_schema_;
    data_group_ = other.data_group_;
    data_intervals_ = other.data_intervals_;
    data_block_ = other.data_block_;

    return;
}


void tidas::indexdb_mem::ops_dict ( backend_path loc, indexdb_op op, map < string, string > const & data, map < string, data_type > const & types ) {

    string path = loc.path + path_sep + loc.name;

    indexdb_dict d;
    d.type = indexdb_object_type::dict;
    d.path = path;
    d.data = data;
    d.types = types;

    if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {
        data_dict_[ path ] = d;
    } else if ( op == indexdb_op::del ) {
        data_dict_.erase( path );
    } else {
        TIDAS_THROW( "unknown indexdb dict operation" );
    }

    indexdb_transaction tr;
    tr.op = op;
    tr.obj.reset ( d.clone() );

    history_.push_back ( tr );

    return;
}


void tidas::indexdb_mem::ops_schema ( backend_path loc, indexdb_op op, field_list const & fields ) {

    string path = loc.path + path_sep + loc.name;

    indexdb_schema s;
    s.type = indexdb_object_type::schema;
    s.path = path;
    s.fields = fields;

    if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {
        data_schema_[ path ] = s;
    } else if ( op == indexdb_op::del ) {
        data_schema_.erase( path );
    } else {
        TIDAS_THROW( "unknown indexdb schema operation" );
    }

    indexdb_transaction tr;
    tr.op = op;
    tr.obj.reset ( s.clone() );

    history_.push_back ( tr );

    return;
}


void tidas::indexdb_mem::ops_group ( backend_path loc, indexdb_op op, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {

    string path = loc.path + path_sep + loc.name;

    indexdb_group g;
    g.type = indexdb_object_type::group;
    g.path = path;
    g.nsamp = nsamp;
    g.counts = counts;
    g.start = start;
    g.stop = stop;

    if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {
        data_group_[ path ] = g;
    } else if ( op == indexdb_op::del ) {
        data_group_.erase( path );
    } else {
        TIDAS_THROW( "unknown indexdb group operation" );
    }

    indexdb_transaction tr;
    tr.op = op;
    tr.obj.reset ( g.clone() );

    history_.push_back ( tr );

    return;
}


void tidas::indexdb_mem::ops_intervals ( backend_path loc, indexdb_op op, size_t const & size ) {

    string path = loc.path + path_sep + loc.name;

    indexdb_intervals t;
    t.type = indexdb_object_type::intervals;
    t.path = path;
    t.size = size;

    if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {
        data_intervals_[ path ] = t;
    } else if ( op == indexdb_op::del ) {
        data_intervals_.erase( path );
    } else {
        TIDAS_THROW( "unknown indexdb intervals operation" );
    }

    indexdb_transaction tr;
    tr.op = op;
    tr.obj.reset ( t.clone() );

    history_.push_back ( tr );

    return;
}


void tidas::indexdb_mem::ops_block ( backend_path loc, indexdb_op op ) {

    string path = loc.path + path_sep + loc.name;

    indexdb_block b;
    b.type = indexdb_object_type::block;
    b.path = path;

    if ( ( op == indexdb_op::add ) || ( op == indexdb_op::update ) ) {
        data_block_[ path ] = b;
    } else if ( op == indexdb_op::del ) {
        data_block_.erase( path );
    } else {
        TIDAS_THROW( "unknown indexdb block operation" );
    }

    indexdb_transaction tr;
    tr.op = op;
    tr.obj.reset ( b.clone() );

    history_.push_back ( tr );

    return;
}


void tidas::indexdb_mem::add_dict ( backend_path loc, map < string, string > const & data, map < string, data_type > const & types ) {
    ops_dict ( loc, indexdb_op::add, data, types );
    return;
}


void tidas::indexdb_mem::update_dict ( backend_path loc, map < string, string > const & data, map < string, data_type > const & types ) {
    ops_dict ( loc, indexdb_op::update, data, types );
    return;
}


void tidas::indexdb_mem::del_dict ( backend_path loc ) {
    map < string, string > fakedata;
    map < string, data_type > faketypes;
    ops_dict ( loc, indexdb_op::del, fakedata, faketypes );
    return;
}


void tidas::indexdb_mem::add_schema ( backend_path loc, field_list const & fields ) {
    ops_schema ( loc, indexdb_op::add, fields );
    return;
}


void tidas::indexdb_mem::update_schema ( backend_path loc, field_list const & fields ) {
    ops_schema ( loc, indexdb_op::update, fields );
    return;
}


void tidas::indexdb_mem::del_schema ( backend_path loc ) {
    field_list fakefields;
    ops_schema ( loc, indexdb_op::del, fakefields );
    return;
}


void tidas::indexdb_mem::add_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {
    ops_group ( loc, indexdb_op::add, nsamp, start, stop, counts );
    return;
}


void tidas::indexdb_mem::update_group ( backend_path loc, index_type const & nsamp, time_type const & start, time_type const & stop, map < data_type, size_t > const & counts ) {
    ops_group ( loc, indexdb_op::update, nsamp, start, stop, counts );
    return;
}


void tidas::indexdb_mem::del_group ( backend_path loc ) {
    map < data_type, size_t > fakecounts;
    ops_group ( loc, indexdb_op::del, 0, 0.0, 0.0, fakecounts );
    return;
}


void tidas::indexdb_mem::add_intervals ( backend_path loc, size_t const & size ) {
    ops_intervals ( loc, indexdb_op::add, size );
    return;
}


void tidas::indexdb_mem::update_intervals ( backend_path loc, size_t const & size ) {
    ops_intervals ( loc, indexdb_op::update, size );
    return;
}


void tidas::indexdb_mem::del_intervals ( backend_path loc ) {
    ops_intervals ( loc, indexdb_op::del, 0 );
    return;
}


void tidas::indexdb_mem::add_block ( backend_path loc ) {
    ops_block ( loc, indexdb_op::add );
    return;
}


void tidas::indexdb_mem::update_block ( backend_path loc ) {
    ops_block ( loc, indexdb_op::update );
    return;
}


void tidas::indexdb_mem::del_block ( backend_path loc ) {
    ops_block ( loc, indexdb_op::del );
    return;
}


bool tidas::indexdb_mem::query_dict ( backend_path loc, map < string, string > & data, map < string, data_type > & types ) {

    bool found = false;

    string path = loc.path + path_sep + loc.name;

    if ( data_dict_.count ( path ) > 0 ) {
        data = data_dict_.at( path ).data;
        types = data_dict_.at( path ).types;
        found = true;
    }

    return found;
}


bool tidas::indexdb_mem::query_schema ( backend_path loc, field_list & fields ) {

    bool found = false;

    string path = loc.path + path_sep + loc.name;

    if ( data_schema_.count ( path ) > 0 ) {
        fields = data_schema_.at( path ).fields;
        found = true;
    }

    return found;
}


bool tidas::indexdb_mem::query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, map < data_type, size_t > & counts ) {

    bool found = false;

    string path = loc.path + path_sep + loc.name;

    if ( data_group_.count ( path ) > 0 ) {
        nsamp = data_group_.at( path ).nsamp;
        start = data_group_.at( path ).start;
        stop = data_group_.at( path ).stop;
        counts = data_group_.at( path ).counts;
        found = true;
    }

    return found;
}


bool tidas::indexdb_mem::query_intervals ( backend_path loc, size_t & size ) {

    bool found = false;

    string path = loc.path + path_sep + loc.name;

    if ( data_intervals_.count ( path ) > 0 ) {
        size = data_intervals_.at( path ).size;
        found = true;
    }

    return found;
}


bool tidas::indexdb_mem::query_block ( backend_path loc, vector < string > & child_blocks, vector < string > & child_groups, vector < string > & child_intervals ) {

    bool found = false;

    child_blocks.clear();
    child_groups.clear();
    child_intervals.clear();

    string path = loc.path + path_sep + loc.name;

    if ( data_block_.count ( path ) > 0 ) {

        map < string, indexdb_block > :: const_iterator bit = data_block_.lower_bound ( path );

        string dir;
        string base;

        while ( ( bit != data_block_.end() ) && ( bit->first.compare ( 0, path.size(), path ) == 0 ) ) {
            if ( bit->first.size() > path.size() ) {
                //(we don't want the parent itself)

                size_t off = path.size() + 1;
                size_t pos = bit->first.find ( path_sep, off );

                if ( pos == string::npos ) {
                    indexdb_path_split ( bit->first, dir, base );
                    child_blocks.push_back( base );
                }
            }
            ++bit;
        }

        string grpdir = path + path_sep + block_fs_group_dir;

        map < string, indexdb_group > :: const_iterator git = data_group_.lower_bound ( grpdir );

        while ( ( git != data_group_.end() ) && ( git->first.compare ( 0, grpdir.size(), grpdir ) == 0 ) ) {
            indexdb_path_split ( git->first, dir, base );
            child_groups.push_back ( base );
            ++git;
        }

        string intdir = path + path_sep + block_fs_intervals_dir;

        map < string, indexdb_intervals > :: const_iterator it = data_intervals_.lower_bound ( intdir );

        while ( ( it != data_intervals_.end() ) && ( it->first.compare ( 0, intdir.size(), intdir ) == 0 ) ) {
            indexdb_path_split ( it->first, dir, base );
            child_intervals.push_back ( base );
            ++it;
        }

        found = true;

    }

    return found;
}


deque < indexdb_transaction > const & tidas::indexdb_mem::history () const {
    return history_;
}


void tidas::indexdb_mem::history_clear() {
    history_.clear();
    return;
}


void tidas::indexdb_mem::history_set ( std::deque < indexdb_transaction > const & trans ) {
    history_ = trans;
    return;
}


void tidas::indexdb_mem::replay ( deque < indexdb_transaction > const & trans ) {

    for ( auto tr : trans ) {

        switch ( tr.op ) {
            case indexdb_op::add :

                switch ( tr.obj->type ) {
                    case indexdb_object_type::dict :
                        data_dict_[ tr.obj->path ] = *( dynamic_cast < indexdb_dict * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::schema :
                        data_schema_[ tr.obj->path ] = *( dynamic_cast < indexdb_schema * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::group :
                        data_group_[ tr.obj->path ] = *( dynamic_cast < indexdb_group * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::intervals :
                        data_intervals_[ tr.obj->path ] = *( dynamic_cast < indexdb_intervals * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::block :
                        data_block_[ tr.obj->path ] = *( dynamic_cast < indexdb_block * > ( tr.obj.get() ) );
                        break;
                    default :
                        TIDAS_THROW( "unknown indexdb object type" );
                        break;
                }

                break;
            case indexdb_op::del :

                switch ( tr.obj->type ) {
                    case indexdb_object_type::dict :
                        data_dict_.erase( tr.obj->path );
                        break;
                    case indexdb_object_type::schema :
                        data_schema_.erase( tr.obj->path );
                        break;
                    case indexdb_object_type::group :
                        data_group_.erase( tr.obj->path );
                        break;
                    case indexdb_object_type::intervals :
                        data_intervals_.erase( tr.obj->path );
                        break;
                    case indexdb_object_type::block :
                        data_block_.erase( tr.obj->path );
                        break;
                    default :
                        TIDAS_THROW( "unknown indexdb object type" );
                        break;
                }

                break;
            case indexdb_op::update :

                switch ( tr.obj->type ) {
                    case indexdb_object_type::dict :
                        data_dict_[ tr.obj->path ] = *( dynamic_cast < indexdb_dict * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::schema :
                        data_schema_[ tr.obj->path ] = *( dynamic_cast < indexdb_schema * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::group :
                        data_group_[ tr.obj->path ] = *( dynamic_cast < indexdb_group * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::intervals :
                        data_intervals_[ tr.obj->path ] = *( dynamic_cast < indexdb_intervals * > ( tr.obj.get() ) );
                        break;
                    case indexdb_object_type::block :
                        data_block_[ tr.obj->path ] = *( dynamic_cast < indexdb_block * > ( tr.obj.get() ) );
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




