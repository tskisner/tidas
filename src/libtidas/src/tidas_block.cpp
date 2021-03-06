
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>

#include <regex>
#include <limits>


extern "C" {
    #include <dirent.h>
}


using namespace std;
using namespace tidas;


void tidas::block_link_operator::operator()(block const & blk) {
    // find the location of this block relative to the start

    backend_path loc = blk.location();

    string blkpath = loc.path + path_sep + loc.name;

    if (start == "") {
        start = blkpath;
    }

    if (blkpath.compare(0, start.size(), start) != 0) {
        TIDAS_THROW("link operator path error");
    }

    string relpath = blkpath.substr(start.size(), string::npos);

    string linkpath = path + relpath;

    bool hard = (type == link_type::hard);

    // create directories

    fs_mkdir(linkpath.c_str());

    string groupdir = linkpath + path_sep + block_fs_group_dir;
    fs_mkdir(groupdir.c_str());

    string intrdir = linkpath + path_sep + block_fs_intervals_dir;
    fs_mkdir(intrdir.c_str());

    // compute link paths for groups and intervals

    string grouplink = linkpath + path_sep + block_fs_group_dir;
    string intrlink = linkpath + path_sep + block_fs_intervals_dir;

    // link groups

    vector <string> names;

    names = blk.group_names();

    for (auto n : names) {
        string full_link = grouplink + path_sep + n;
        blk.group_get(n).link(type, full_link);
    }

    // link intervals

    names = blk.intervals_names();

    for (auto n : names) {
        string full_link = intrlink + path_sep + n;
        blk.intervals_get(n).link(type, full_link);
    }

    // recursively link all files in aux directory

    string auxtarget = blkpath + path_sep + block_fs_aux_dir;
    string auxdir = linkpath + path_sep + block_fs_aux_dir;

    fs_link_r(auxtarget.c_str(), auxdir.c_str(), hard);

    return;
}

void tidas::block_wipe_operator::operator()(block const & blk) {
    // wipe groups

    vector <string> names;

    names = blk.group_names();

    for (auto n : names) {
        blk.group_get(n).wipe();
    }

    // wipe intervals

    names = blk.intervals_names();

    for (auto n : names) {
        blk.intervals_get(n).wipe();
    }

    // remove sub-block directories that have already been wiped.

    names = blk.block_names();

    for (auto n : names) {
        backend_path loc = blk.block_get(n).location();
        string dir = loc.path + path_sep + loc.name;
        fs_rm_r(dir.c_str());
    }

    return;
}

tidas::block::block() {
    relocate(loc_);
}

tidas::block::~block() {}

block & tidas::block::operator=(block const & other) {
    if (this != &other) {
        copy(other, "", other.loc_);
    }
    return *this;
}

tidas::block::block(block const & other) {
    copy(other, "", other.loc_);
}

tidas::block::block(backend_path const & loc, string const & filter) {
    relocate(loc);
    sync(filter);
}

tidas::block::block(block const & other, std::string const & filter,
                    backend_path const & loc) {
    copy(other, filter, loc);
}

void tidas::block::relocate(backend_path const & loc) {
    if (loc.name == block_fs_intervals_dir) {
        ostringstream o;
        o << "cannot give block reserved name \"" << block_fs_intervals_dir << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    if (loc.name == block_fs_group_dir) {
        ostringstream o;
        o << "cannot give block reserved name \"" << block_fs_group_dir << "\"";
        TIDAS_THROW(o.str().c_str());
    }

    loc_ = loc;

    string fspath = loc_.path + path_sep + loc_.name;

    // relocate groups

    for (auto & gr : group_data_) {
        gr.second.relocate(group_loc(loc_, gr.first));
    }

    // relocate intervals

    for (auto & inv : intervals_data_) {
        inv.second.relocate(intervals_loc(loc_, inv.first));
    }

    // relocate sub blocks

    for (auto & blk : block_data_) {
        blk.second.relocate(block_loc(loc_, blk.first));
    }

    return;
}

void tidas::block::sync(string const & filter) {
    string fspath = loc_.path + path_sep + loc_.name;

    if (loc_.type != backend_type::none) {
        group_data_.clear();
        intervals_data_.clear();
        block_data_.clear();

        string filt_local;
        string filt_sub;
        bool stop;

        filter_block(filter, filt_local, filt_sub, stop);

        // split local filter string:  [XX=XX,XX=XX]

        map <string, string> filts = filter_split(filt_local);

        // extract sub-block filter

        string filt_name;
        string filt_pass;

        filter_sub(filt_sub, filt_name, filt_pass);
        regex blockre(filter_default(filt_name), std::regex::extended);

        // extract group filter

        filter_sub(filts[group_submatch_key], filt_name, filt_pass);
        regex groupre(filter_default(filt_name), std::regex::extended);

        // extract intervals filter

        filter_sub(filts[intervals_submatch_key], filt_name, filt_pass);
        regex intre(filter_default(filt_name), std::regex::extended);

        // check index, otherwise read from filesystem

        bool found = false;

        vector <string> child_blocks;
        vector <string> child_groups;
        vector <string> child_intervals;

        if (loc_.idx) {
            found = loc_.idx->query_block(loc_, child_blocks, child_groups,
                                          child_intervals);
        }

        if (found) {
            // std::cout << "DBG SYNC:  block " << fspath << " in index, querying
            // children" << std::endl;

            for (auto const & b : child_blocks) {
                if (!stop) {
                    if (regex_match(b, blockre)) {
                        block_data_[b] = block(block_loc(loc_, b));
                    }
                }
            }

            for (auto const & g : child_groups) {
                if (regex_match(g, groupre)) {
                    group_data_[g] = group(group_loc(loc_, g));
                }
            }

            for (auto const & t : child_intervals) {
                if (regex_match(t, intre)) {
                    intervals_data_[t] = intervals(intervals_loc(loc_, t));
                }
            }
        } else {
            // std::cout << "DBG SYNC:  block " << fspath << " not in index, reading
            // filesystem" << std::endl;

            // find all sub-blocks

            string fspath = loc_.path + path_sep + loc_.name;

            struct dirent * entry;
            DIR * dp;

            dp = opendir(fspath.c_str());

            if (dp == NULL) {
                ostringstream o;
                o << "cannot open block \"" << fspath << "\"";
                TIDAS_THROW(o.str().c_str());
            }

            bool found_groups;
            bool found_intervals;

            while ((entry = readdir(dp))) {
                string item = entry->d_name;

                // skip over extra directories that are optional
                if (item == ".") continue;
                if (item == "..") continue;
                if (item == block_fs_aux_dir) continue;

                if (item == block_fs_group_dir) {
                    found_groups = true;
                } else if (item == block_fs_intervals_dir) {
                    found_intervals = true;
                } else {
                    // this must be a block!
                    if (!stop) {
                        if (regex_match(item, blockre)) {
                            block_data_[item] = block(block_loc(loc_, item));
                        }
                    }
                }
            }

            closedir(dp);

            if ((!found_groups) || (!found_intervals)) {
                ostringstream o;
                o << "block \"" << fspath <<
                    "\" is missing the group or intervals subdirectories!";
                TIDAS_THROW(o.str().c_str());
            }

            if (loc_.idx && (loc_.mode == access_mode::write)) {
                loc_.idx->add_block(loc_);
            }

            // sync all groups

            string groupdir = fspath + path_sep + block_fs_group_dir;

            dp = opendir(groupdir.c_str());

            if (dp == NULL) {
                ostringstream o;
                o << "cannot open block group directory \"" << groupdir << "\"";
                TIDAS_THROW(o.str().c_str());
            }

            while ((entry = readdir(dp))) {
                string item = entry->d_name;
                if (item == ".") continue;
                if (item == "..") continue;
                if (regex_match(item, groupre)) {
                    group_data_[item] = group(group_loc(loc_, item));
                }
            }

            closedir(dp);

            // sync all intervals

            string intrdir = fspath + path_sep + block_fs_intervals_dir;

            dp = opendir(intrdir.c_str());

            if (dp == NULL) {
                ostringstream o;
                o << "cannot open block intervals directory \"" << intrdir << "\"";
                TIDAS_THROW(o.str().c_str());
            }

            while ((entry = readdir(dp))) {
                string item = entry->d_name;
                if (item == ".") continue;
                if (item == "..") continue;
                if (regex_match(item, intre)) {
                    intervals_data_[item] = intervals(intervals_loc(loc_, item));
                }
            }

            closedir(dp);
        }
    } else {
        // std::cout << "DBG SYNC:  block " << fspath << " no backend (will be empty)"
        // << std::endl;
    }

    return;
}

void tidas::block::flush() const {
    string dir = loc_.path + path_sep + loc_.name;

    if (loc_.type != backend_type::none) {
        if (loc_.mode == access_mode::write) {
            fs_mkdir(dir.c_str());

            string groupdir = dir + path_sep + block_fs_group_dir;
            fs_mkdir(groupdir.c_str());

            string intrdir = dir + path_sep + block_fs_intervals_dir;
            fs_mkdir(intrdir.c_str());

            string auxdir = dir + path_sep + block_fs_aux_dir;
            fs_mkdir(auxdir.c_str());

            // update index

            if (loc_.idx) {
                // std::cout << "DBG FLUSH:  block " << dir << " to index " <<
                // loc_.idx.get() << std::endl;
                loc_.idx->update_block(loc_);
            }
        }

        // std::cout << "DBG FLUSH:  block " << dir << " opened read-only, skipping
        // flush" << std::endl;
    } else {
        // std::cout << "DBG FLUSH:  block " << dir << " no backend, skipping flush" <<
        // std::endl;
    }

    // flush groups

    for (auto & gr : group_data_) {
        gr.second.flush();
    }

    // flush intervals

    for (auto & inv : intervals_data_) {
        inv.second.flush();
    }

    // flush sub blocks

    for (auto & blk : block_data_) {
        blk.second.flush();
    }

    return;
}

void tidas::block::copy(block const & other, string const & filter,
                        backend_path const & loc) {
    string filt = filter_default(filter);

    if ((filt != ".*") && (loc.type != backend_type::none) && (loc == other.loc_)) {
        TIDAS_THROW("copy of non-memory block with a filter requires a new location");
    }

    // split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX --->
    // [XX=XX,XX=XX] XXXX[XX=XX]/XXX

    string filt_local;
    string filt_sub;
    bool stop;

    filter_block(filter, filt_local, filt_sub, stop);

    // split local filter string:  [XX=XX,XX=XX]

    map <string, string> filts = filter_split(filt_local);

    // set location

    loc_ = loc;

    // update index only if this is a new location

    if (loc_.idx && (loc_.mode == access_mode::write) && (loc != other.loc_)) {
        loc_.idx->update_block(loc_);
    }

    // copy groups

    string filt_name;
    string filt_pass;

    // XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]

    filter_sub(filts[group_submatch_key], filt_name, filt_pass);

    regex groupre(filter_default(filt_name), std::regex::extended);

    group_data_.clear();

    for (auto & gr : other.group_data_) {
        if (regex_match(gr.first, groupre)) {
            group_data_[gr.first].copy(gr.second, filt_pass, group_loc(loc_, gr.first));
        }
    }

    // copy intervals

    // XXX[dict=XXX] ---> XXX [dict=XXX]

    filter_sub(filts[intervals_submatch_key], filt_name, filt_pass);

    regex intre(filter_default(filt_name), std::regex::extended);

    intervals_data_.clear();

    for (auto & inv : other.intervals_data_) {
        if (regex_match(inv.first, intre)) {
            intervals_data_[inv.first].copy(inv.second, filt_pass,
                                            intervals_loc(loc_, inv.first));
        }
    }

    // copy sub-blocks

    // extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX
    // [XX=XX]/XXX
    // check if we have reached a hard stop at this depth, and in this case ignore all
    // sub-blocks.

    if (!stop) {
        filter_sub(filt_sub, filt_name, filt_pass);

        regex blockre(filter_default(filt_name), std::regex::extended);

        block_data_.clear();

        for (auto & blk : other.block_data_) {
            if (regex_match(blk.first, blockre)) {
                block_data_[blk.first].copy(blk.second, filt_pass,
                                            block_loc(loc_, blk.first));
            }
        }
    }

    return;
}

block tidas::block::select(string const & filter) const {
    block ret;
    ret.relocate(loc_);

    // split filter into local and sub blocks:  [XX=XX,XX=XX]/XXXX[XX=XX]/XXX[X=X]/ --->
    // [XX=XX,XX=XX] XXXX[XX=XX]/XXX[X=X]/

    string filt_local;
    string filt_sub;
    bool stop;

    filter_block(filter, filt_local, filt_sub, stop);

    // std::cout << "filter_block:  local = \"" << filt_local << "\", sub = \"" <<
    // filt_sub << "\", stop = " << stop << std::endl;

    // split local filter string:  [XX=XX,XX=XX]

    map <string, string> filts = filter_split(filt_local);

    // select groups.  Since we are not copying objects to a
    // new location, we strip off any sub-object filters.

    string filt_name;
    string filt_pass;

    if (filts.count(group_submatch_key) > 0) {
        // XXX[schm=XXX,dict=XXX] ---> XXX [schm=XXX,dict=XXX]
        filter_sub(filts[group_submatch_key], filt_name, filt_pass);

        regex groupre(filter_default(filt_name), std::regex::extended);

        for (auto & gr : group_data_) {
            if (regex_match(gr.first, groupre)) {
                ret.group_data_[gr.first].copy(gr.second, "",
                                               group_loc(ret.loc_, gr.first));
            }
        }
    } else {
        for (auto & gr : group_data_) {
            ret.group_data_[gr.first].copy(gr.second, "",
                                           group_loc(ret.loc_, gr.first));
        }
    }

    // select intervals.  Again, we strip off sub-filters.

    if (filts.count(intervals_submatch_key) > 0) {
        // XXX[dict=XXX] ---> XXX [dict=XXX]
        filter_sub(filts[intervals_submatch_key], filt_name, filt_pass);

        regex intre(filter_default(filt_name), std::regex::extended);

        for (auto & inv : intervals_data_) {
            if (regex_match(inv.first, intre)) {
                ret.intervals_data_[inv.first].copy(inv.second, "",
                                                    intervals_loc(ret.loc_, inv.first));
            }
        }
    } else {
        for (auto & inv : intervals_data_) {
            ret.intervals_data_[inv.first].copy(inv.second, "",
                                                intervals_loc(ret.loc_, inv.first));
        }
    }

    // select sub-blocks

    // extract sub-block name match and filter to pass on:  XXXX[XX=XX]/XXX ---> XXXX
    // [XX=XX]/XXX
    // check if we have reached a hard stop at this depth, and in this case ignore all
    // sub-blocks.

    if (!stop) {
        filter_sub(filt_sub, filt_name, filt_pass);

        // std::cout << "  sub blocks:  name = \"" << filt_name << "\" (" <<
        // filter_default(filt_name) << ") " << " pass = \"" << filt_pass << "\"" <<
        // std::endl;

        regex blockre(filter_default(filt_name), std::regex::extended);

        // std::cout << "    searching " << block_data_.size() << " sub blocks" <<
        // std::endl;

        for (auto & blk : block_data_) {
            if (regex_match(blk.first, blockre)) {
                // std::cout << "    block \"" << blk.first << "\" matches" <<
                // std::endl;
                ret.block_data_[blk.first] = blk.second.select(filt_pass);
            } else {
                // std::cout << "    block \"" << blk.first << "\" no match" <<
                // std::endl;
            }
        }
    }

    return ret;
}

void tidas::block::link(link_type const & type, string const & path) const {
    if (type != link_type::none) {
        if (loc_.type != backend_type::none) {
            block_link_operator op;
            op.type = type;
            op.path = path;

            exec(op, exec_order::depth_last);
        }
    }

    return;
}

void tidas::block::wipe() const {
    if (loc_.type != backend_type::none) {
        if (loc_.mode == access_mode::write) {
            block_wipe_operator op;

            exec(op, exec_order::depth_first);
        } else {
            TIDAS_THROW("cannot wipe block in read-only mode");
        }
    }

    return;
}

backend_path tidas::block::location() const {
    return loc_;
}

string tidas::block::aux_dir() const {
    string ret = loc_.path + path_sep + loc_.name + path_sep + block_fs_aux_dir;
    return ret;
}

backend_path tidas::block::group_loc(backend_path const & loc,
                                     string const & name) const {
    backend_path ret = loc;

    ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_group_dir;
    ret.name = name;

    return ret;
}

backend_path tidas::block::intervals_loc(backend_path const & loc,
                                         string const & name) const {
    backend_path ret = loc;

    ret.path = loc.path + path_sep + loc.name + path_sep + block_fs_intervals_dir;
    ret.name = name;

    return ret;
}

backend_path tidas::block::block_loc(backend_path const & loc,
                                     string const & name) const {
    backend_path ret = loc;

    ret.path = loc.path + path_sep + loc.name;
    ret.name = name;

    return ret;
}

void tidas::block::range(time_type & start, time_type & stop) const {
    // iterate over all groups, and find the maximum time extent

    time_type mintime = numeric_limits <time_type>::max();
    time_type maxtime = numeric_limits <time_type>::min();

    time_type grmin;
    time_type grmax;

    for (auto & gr : group_data_) {
        gr.second.range(grmin, grmax);

        if (grmin < mintime) {
            mintime = grmin;
        }

        if (grmax > maxtime) {
            maxtime = grmax;
        }
    }

    start = mintime;
    stop = maxtime;

    return;
}

group & tidas::block::group_add(string const & name, group const & grp) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot add group: block is open in read-only mode");
    }

    if (group_data_.count(name) > 0) {
        ostringstream o;
        o << "cannot add group with name \"" << name << "\": already exists";
        TIDAS_THROW(o.str().c_str());
    }

    group_data_[name].copy(grp, "", group_loc(loc_, name));
    group_data_[name].flush();

    if ((loc_.type != backend_type::none) &&
        (grp.location().type != backend_type::none)) {
        data_copy(grp, group_data_.at(name));
    }

    return group_data_.at(name);
}

group & tidas::block::group_get(string const & name) {
    if (group_data_.count(name) == 0) {
        ostringstream o;
        o << "group with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return group_data_.at(name);
}

group const & tidas::block::group_get(string const & name) const {
    if (group_data_.count(name) == 0) {
        ostringstream o;
        o << "group with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return group_data_.at(name);
}

void tidas::block::group_del(string const & name) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot delete group: block is open in read-only mode");
    }

    if (group_data_.count(name) == 0) {
        ostringstream o;
        o << "cannot remove group with name \"" << name << "\": does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    group_data_[name].wipe();
    group_data_.erase(name);

    return;
}

vector <string> tidas::block::group_names() const {
    vector <string> ret;
    for (auto & gr : group_data_) {
        ret.push_back(gr.first);
    }
    return ret;
}

void tidas::block::clear_groups() {
    vector <string> all = group_names();
    for (auto & gr : all) {
        group_del(gr);
    }
    return;
}

intervals & tidas::block::intervals_add(string const & name, intervals const & intr) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot add intervals: block is open in read-only mode");
    }

    if (intervals_data_.count(name) > 0) {
        ostringstream o;
        o << "cannot add intervals with name \"" << name << "\": already exists";
        TIDAS_THROW(o.str().c_str());
    }

    intervals_data_[name].copy(intr, "", intervals_loc(loc_, name));
    intervals_data_[name].flush();

    if ((loc_.type != backend_type::none) &&
        (intr.location().type != backend_type::none)) {
        data_copy(intr, intervals_data_.at(name));
    }

    return intervals_data_.at(name);
}

intervals & tidas::block::intervals_get(string const & name) {
    if (intervals_data_.count(name) == 0) {
        ostringstream o;
        o << "intervals with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return intervals_data_.at(name);
}

intervals const & tidas::block::intervals_get(string const & name) const {
    if (intervals_data_.count(name) == 0) {
        ostringstream o;
        o << "intervals with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return intervals_data_.at(name);
}

void tidas::block::intervals_del(string const & name) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot delete intervals: block is open in read-only mode");
    }

    if (intervals_data_.count(name) == 0) {
        ostringstream o;
        o << "cannot remove intervals with name \"" << name << "\": does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    intervals_data_[name].wipe();
    intervals_data_.erase(name);

    return;
}

vector <string> tidas::block::intervals_names() const {
    vector <string> ret;
    for (auto & inv : intervals_data_) {
        ret.push_back(inv.first);
    }
    return ret;
}

void tidas::block::clear_intervals() {
    vector <string> all = intervals_names();
    for (auto & inv : all) {
        intervals_del(inv);
    }
    return;
}

block & tidas::block::block_add(string const & name, block const & blk) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot add sub-block: block is open in read-only mode");
    }

    if (block_data_.count(name) > 0) {
        ostringstream o;
        o << "cannot add block with name \"" << name << "\": already exists";
        TIDAS_THROW(o.str().c_str());
    }

    block_data_[name].copy(blk, "", block_loc(loc_, name));
    block_data_[name].flush();

    // std::cout << "DBG:  block " << loc_.path << "/" << loc_.name << " adding child "
    // << name << std::endl;

    if ((loc_.type != backend_type::none) &&
        (blk.location().type != backend_type::none)) {
        data_copy(blk, block_data_.at(name));
    }

    return block_data_.at(name);
}

block & tidas::block::block_get(string const & name) {
    if (block_data_.count(name) == 0) {
        ostringstream o;
        o << "block with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return block_data_.at(name);
}

block const & tidas::block::block_get(string const & name) const {
    if (block_data_.count(name) == 0) {
        ostringstream o;
        o << "block with name \"" << name << "\" does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    return block_data_.at(name);
}

void tidas::block::block_del(string const & name) {
    if (loc_.mode == access_mode::read) {
        TIDAS_THROW("cannot delete sub-block: block is open in read-only mode");
    }

    if (block_data_.count(name) == 0) {
        ostringstream o;
        o << "cannot remove block with name \"" << name << "\": does not exist";
        TIDAS_THROW(o.str().c_str());
    }

    block_data_[name].wipe();
    block_data_.erase(name);

    return;
}

vector <string> tidas::block::block_names() const {
    vector <string> ret;
    for (auto & blk : block_data_) {
        ret.push_back(blk.first);
    }
    return ret;
}

void tidas::block::clear_blocks() {
    vector <string> all = block_names();
    for (auto & blk : all) {
        block_del(blk);
    }
    return;
}

void tidas::block::clear() {
    clear_groups();
    clear_intervals();
    clear_blocks();
    return;
}

void tidas::block::info(std::ostream & out, size_t indent, bool recurse) const {
    std::ostringstream ind;
    ind.str("");
    ind << "TIDAS:  ";
    for (size_t i = 0; i < indent; ++i) {
        ind << " ";
    }
    for (auto & inv : intervals_data_) {
        out << ind.str() << "Intervals \"" << inv.first << "\":" << std::endl;
        inv.second.info(out, indent + 2);
    }
    for (auto & gr : group_data_) {
        out << ind.str() << "Group \"" << gr.first << "\":" << std::endl;
        gr.second.info(out, indent + 2);
    }
    if (recurse) {
        for (auto & blk : block_data_) {
            out << ind.str() << "Block \"" << blk.first << "\":" << std::endl;
            blk.second.info(out, indent + 2, recurse);
        }
    } else {
        for (auto & blk : block_data_) {
            out << ind.str() << "Sub-Block \"" << blk.first << "\""
                << std::endl;
        }
    }
    return;
}
