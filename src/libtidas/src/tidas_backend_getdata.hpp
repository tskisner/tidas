
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TIDAS_GETDATA_HPP
#define TIDAS_GETDATA_HPP

#ifdef HAVE_GETDATA
extern "C" {
    # include <getdata.h>
}
#endif // ifdef HAVE_GETDATA


namespace tidas {

static const std::string schema_getdata_dir = "schema";

static const std::string intervals_getdata_field_start = "start";
static const std::string intervals_getdata_field_stop = "stop";
static const std::string intervals_getdata_field_first = "first";
static const std::string intervals_getdata_field_last = "last";

}


#endif // ifndef TIDAS_GETDATA_HPP
