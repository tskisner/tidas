/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#include <tidas_internal.hpp>

#include <tidas_backend_getdata.hpp>


using namespace std;
using namespace tidas;


tidas::intervals_backend_getdata::intervals_backend_getdata() : intervals_backend() {}

tidas::intervals_backend_getdata::~intervals_backend_getdata() {}

tidas::intervals_backend_getdata::intervals_backend_getdata(
    intervals_backend_getdata const & other) {}

intervals_backend_getdata & tidas::intervals_backend_getdata::operator=(
    intervals_backend_getdata const & other) {
    if (this != &other) {}
    return *this;
}

string tidas::intervals_backend_getdata::dict_meta() const {
    return "";
}

void tidas::intervals_backend_getdata::read(backend_path const & loc, size_t & size) {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::intervals_backend_getdata::write(backend_path const & loc,
                                             size_t const & size) const {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::intervals_backend_getdata::link(backend_path const & loc, link_type type,
                                            std::string const & path) const {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::intervals_backend_getdata::wipe(backend_path const & loc) const {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::intervals_backend_getdata::read_data(backend_path const & loc,
                                                 interval_list & intr) const {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::intervals_backend_getdata::write_data(backend_path const & loc,
                                                  interval_list const & intr) {
    TIDAS_THROW("GetData backend not supported");
    return;
}
