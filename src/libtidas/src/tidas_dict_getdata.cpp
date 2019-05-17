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


tidas::dict_backend_getdata::dict_backend_getdata() {}

tidas::dict_backend_getdata::~dict_backend_getdata() {}

tidas::dict_backend_getdata::dict_backend_getdata(dict_backend_getdata const & other) {}

dict_backend_getdata & tidas::dict_backend_getdata::operator=(
    dict_backend_getdata const & other) {
    if (this != &other) {}
    return *this;
}

void tidas::dict_backend_getdata::read(backend_path const & loc, map <string,
                                                                      string> & data,
                                       map <string, data_type> & types) {
    TIDAS_THROW("GetData backend not supported");
    return;
}

void tidas::dict_backend_getdata::write(backend_path const & loc, map <string,
                                                                       string> const & data, map <string,
                                                                                                  data_type> const & types)
const {
    TIDAS_THROW("GetData backend not supported");
    return;
}
