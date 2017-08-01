##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function, unicode_literals

import ctypes as ct
import numpy as np

from .ctidas import *


class Intervals(object):
    """
    Class which represents a TIDAS intervals object.

    Args:
        size (long):   size of the list of intervals.
        props (dict):  (optional) a dictionary of properties to associate
            with the group.
    """
    def __init__(self, size=0, props=dict(), handle=None ):
        self.cp = handle
        if self.cp is not None:
            # we are constructing from a C pointer
            cd = lib.ctidas_intervals_dict(self.cp)
            self._prps = dict_c2py(cd)
            self._sz = lib.ctidas_intervals_size(self.cp)
        else:
            self._sz = size
            self._prps = props

    def close(self):
        """
        Explicitly close the intervals (free the underlying C++ pointer).
        """
        if self.cp is not None:
            lib.ctidas_intervals_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    @property
    def props(self):
        """
        The dictionary of properties set at construction time.
        """
        return self._prps

    @property
    def size(self):
        """
        The number of intervals.
        """
        return self._sz

    def _handle(self):
        return self.cp

    def read(self):
        """
        Read the list of intervals.

        This reads the underlying intervals data and returns the results as
        a simple Python class with data members for start / stop times and
        first / last samples.

        Returns (list):
            a list of Intrvl objects.
        """
        data = None
        if self.cp is not None:
            nint = ct.c_ulong(0)
            cdata = lib.ctidas_intervals_read(self.cp, ct.byref(nint))
            data = intrvl_list_c2py(cdata, nint.value)
            lib.ctidas_intrvl_list_free(cdata, nint)
        else:
            raise RuntimeError("intervals object is not associated with a block- cannot read or write data")
        return data

    def write(self, data):
        """
        Write the list of intervals.

        Takes a list of Intrvl objects and passes them to the underlying C++
        code for writing.

        Args:
            data (list): a list of Intrvl objects.
        """
        ndata = len(data)
        if ndata != self._sz:
            raise IndexError("cannot write interval list of length {} to intervals object with size {}".format(ndata, self._sz))
        if self.cp is not None:
            nint = ct.c_ulong(ndata)
            cdata = intrvl_list_py2c(data)
            lib.ctidas_intervals_write(self.cp, cdata, nint)
            lib.ctidas_intrvl_list_free(cdata, nint)
        else:
            raise RuntimeError("intervals object is not associated with a block- cannot read or write data")
        return
            

def intervals_samples(data):
    """
    Sum the total number of samples in a list of Intrvl objects.

    Args:
        data (list): a list of Intrvl objects.

    Returns (int):
        the total number of samples in all intervals.
    """
    ndata = len(data)
    nint = ct.c_ulong(ndata)
    cdata = intrvl_list_py2c(data)
    result = lib.ctidas_intervals_samples(cdata, nint)
    lib.ctidas_intrvl_list_free(cdata, nint)
    return result

def intervals_time(data):
    """
    Sum the total time in a list of Intrvl objects.

    Args:
        data (list): a list of Intrvl objects.

    Returns (float):
        the total number of seconds in all intervals.
    """
    ndata = len(data)
    nint = ct.c_ulong(ndata)
    cdata = intrvl_list_py2c(data)
    result = lib.ctidas_intervals_time(cdata, nint)
    lib.ctidas_intrvl_list_free(cdata, nint)
    return result

def intervals_seek(data, time):
    """
    Return the single interval that contains a specified time.

    If no interval contains the time, an empty interval is
    returned.

    Args:
        data (list): a list of Intrvl objects.
        time (float): the time to compare.

    Returns (Intrvl):
        a single Intrvl object.
    """
    ndata = len(data)
    nint = ct.c_ulong(ndata)
    cdata = intrvl_list_py2c(data)
    cint = lib.ctidas_intervals_seek(cdata, nint, ct.c_double(time))
    start = lib.ctidas_intrvl_start_get(cint)
    stop = lib.ctidas_intrvl_stop_get(cint)
    first = lib.ctidas_intrvl_first_get(cint)
    last = lib.ctidas_intrvl_last_get(cint)
    result = Intrvl(start=start, stop=stop, first=first, last=last)
    lib.ctidas_intrvl_free(cint)
    return result

def intervals_seek_ceil(data, time):
    """
    Return the first interval whose stop time is after the
    specified time.

    Args:
        data (list): a list of Intrvl objects.
        time (float): the time to compare.

    Returns (Intrvl):
        a single Intrvl object.
    """
    ndata = len(data)
    nint = ct.c_ulong(ndata)
    cdata = intrvl_list_py2c(data)
    cint = lib.ctidas_intervals_seek(cdata, nint, ct.c_double(time))
    start = lib.ctidas_intrvl_start_get(cint)
    stop = lib.ctidas_intrvl_stop_get(cint)
    first = lib.ctidas_intrvl_first_get(cint)
    last = lib.ctidas_intrvl_last_get(cint)
    result = Intrvl(start=start, stop=stop, first=first, last=last)
    lib.ctidas_intrvl_free(cint)
    return result

def intervals_seek_floor(data, time):
    """
    Return the last interval whose start time is before the
    specified time.

    Args:
        data (list): a list of Intrvl objects.
        time (float): the time to compare.

    Returns (Intrvl):
        a single Intrvl object.
    """
    ndata = len(data)
    nint = ct.c_ulong(ndata)
    cdata = intrvl_list_py2c(data)
    cint = lib.ctidas_intervals_seek(cdata, nint, ct.c_double(time))
    start = lib.ctidas_intrvl_start_get(cint)
    stop = lib.ctidas_intrvl_stop_get(cint)
    first = lib.ctidas_intrvl_first_get(cint)
    last = lib.ctidas_intrvl_last_get(cint)
    result = Intrvl(start=start, stop=stop, first=first, last=last)
    lib.ctidas_intrvl_free(cint)
    return result

