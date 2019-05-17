
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TIDAS_COPY_HPP
#define TIDAS_COPY_HPP


namespace tidas {

// intervals copy

void data_copy(intervals const & in, intervals & out);


// group copy

template <class T>
void group_helper_copy(group const & in, group & out, std::string const & field,
                       index_type n) {
    std::vector <T> data(n);
    in.read_field(field, 0, data);
    out.write_field(field, 0, data);

    return;
}

void data_copy(group const & in, group & out);


// block copy

void data_copy(block const & in, block & out);


// volume copy

void data_copy(volume const & in, volume & out);

}


#endif // ifndef TIDAS_COPY_HPP
