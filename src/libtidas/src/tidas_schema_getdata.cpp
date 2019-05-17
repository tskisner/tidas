
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>

#include <tidas_backend_getdata.hpp>


using namespace std;
using namespace tidas;


tidas::schema_backend_getdata::schema_backend_getdata() {}

tidas::schema_backend_getdata::~schema_backend_getdata() {}

tidas::schema_backend_getdata::schema_backend_getdata(
    schema_backend_getdata const & other) {}

schema_backend_getdata & tidas::schema_backend_getdata::operator=(
    schema_backend_getdata const & other) {
    if (this != &other) {}
    return *this;
}

void tidas::schema_backend_getdata::read(backend_path const & loc,
                                         field_list & fields) {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::schema_backend_getdata::write(backend_path const & loc,
                                          field_list const & fields) const {
    TIDAS_THROW("GetData backend not supported");
    return;
}
