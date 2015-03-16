/*
  TImestream DAta Storage (TIDAS)
  (c) 2014-2015, The Regents of the University of California, 
  through Lawrence Berkeley National Laboratory.  See top
  level LICENSE file for details.
*/


#include <tidas_internal.hpp>


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



tidas::indexdb::indexdb () {
	path_ = "";
	sql_ = NULL;
}


tidas::indexdb::~indexdb () {
	// close DB if it is open
	if ( path_ != "" ) {
		if ( sql_ ) {

		}
	}
}


indexdb & tidas::indexdb::operator= ( indexdb const & other ) {
	if ( this != &other ) {
		copy ( other );
	}
	return *this;
}


tidas::indexdb::indexdb ( indexdb const & other ) {
	copy ( other );
}


void tidas::indexdb::copy ( indexdb const & other ) {
	path_ = other.path_;
	history_ = other.history_;

	data_dict_ = other.data_dict_;
	data_schema_ = other.data_schema_;
	data_group_ = other.data_group_;
	data_intervals_ = other.data_intervals_;
	data_block_ = other.data_block_;

	// open DB if path is set
	if ( path_ != "" ) {

	} else {
		sql_ = NULL;
	}

	return;
}


tidas::indexdb::indexdb ( string const & path ) {
	path_ = path;

	// open DB if path is set
	if ( path_ != "" ) {

	}
}


void tidas::indexdb::ins_dict ( string const & path, indexdb_dict const & d ) {

	data_dict_[ path ] = d;

	// modify DB if path is set
	if ( path_ != "" ) {

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

	}

	return;
}


void tidas::indexdb::add_dict ( backend_path loc, std::map < std::string, std::string > const & data, std::map < std::string, data_type > const & types ) {

	string path = loc.path + path_sep + loc.name + path_sep + dict_submatch_key;

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

	string path = loc.path + path_sep + loc.name + path_sep + dict_submatch_key;

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

	string path = loc.path + path_sep + loc.name + path_sep + dict_submatch_key;

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


void tidas::indexdb::query_dict ( backend_path loc, std::map < std::string, std::string > & data, std::map < std::string, data_type > & types ) const {

	string path = loc.path + path_sep + loc.name + path_sep + dict_submatch_key;

	if ( data_dict_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB


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

	string path = loc.path + path_sep + loc.name + path_sep + schema_submatch_key;

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

	string path = loc.path + path_sep + loc.name + path_sep + schema_submatch_key;

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

	string path = loc.path + path_sep + loc.name + path_sep + schema_submatch_key;

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


void tidas::indexdb::query_schema ( backend_path loc, field_list & fields ) const {

	string path = loc.path + path_sep + loc.name + path_sep + schema_submatch_key;

	if ( data_schema_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB


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


void tidas::indexdb::query_group ( backend_path loc, index_type & nsamp, time_type & start, time_type & stop, std::map < data_type, size_t > & counts ) const {

	string path = loc.path + path_sep + loc.name;

	if ( data_group_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB


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


void tidas::indexdb::query_intervals ( backend_path loc, size_t & size ) const {

	string path = loc.path + path_sep + loc.name;

	if ( data_intervals_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB


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


void tidas::indexdb::query_block ( backend_path loc, std::vector < std::string > & child_blocks, std::vector < std::string > & child_groups, std::vector < std::string > & child_intervals ) const {

	string path = loc.path + path_sep + loc.name;

	if ( data_block_.count ( path ) == 0 ) {
		if ( path_ != "" ) {
			// fetch it from the DB


		} else {
			ostringstream o;
			o << "block at \"" << path << "\" does not exist in index";
			TIDAS_THROW( o.str().c_str() );
		}
	}

	// Find all direct descendants.  If we are using a DB, we query
	// this and update our copy in memory.

	child_blocks.clear();
	child_groups.clear();
	child_intervals.clear();

	if ( path_ != "" ) {



	}

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




