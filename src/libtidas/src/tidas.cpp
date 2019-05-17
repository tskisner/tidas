
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>


using namespace std;
using namespace tidas;


tidas::backend_path::backend_path() {
    path = "";
    name = "";
    meta = "";
    type = backend_type::none;
    comp = compression_type::none;
    mode = access_mode::read;
    backparams.clear();
    idx.reset();
}

bool tidas::backend_path::operator==(const backend_path & other) const {
    if (path != other.path) {
        return false;
    }
    if (name != other.name) {
        return false;
    }
    if (meta != other.meta) {
        return false;
    }
    if (type != other.type) {
        return false;
    }
    if (comp != other.comp) {
        return false;
    }
    if (mode != other.mode) {
        return false;
    }
    if (backparams != other.backparams) {
        return false;
    }
    return true;
}

bool tidas::backend_path::operator!=(const backend_path & other) const {
    return !(*this == other);
}
