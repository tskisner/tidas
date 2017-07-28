##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import ctypes as ct
import numpy as np

from .ctidas import *


class Group(object):
    """
    Class which represents a TIDAS group.

    Args:
        schema (dict): dictionary of fields and their types and units to
                       create in the group.  Example:
                       { "field1" : ("uint16", "counts"), 
                         "field2" : ("double", "volts") }
        size (long):   number of samples in the group.
        props (dict):  (optional) a dictionary of properties to associate
                       with the group.
    """
    def __init__(self, schema=dict(), size=0, props=dict(), handle=None ):
        self.cp = handle
        if self.cp is not None:
            # we are constructing from a C pointer
            #sys.stderr.write("group ctor c pointer {}\n".format(self.cp))
            cs = lib.ctidas_group_schema(self.cp)
            #sys.stderr.write("group ctor c schema = {}\n".format(cs))
            self._schm = schema_c2py(cs)
            #sys.stderr.write("group ctor py schema = {}\n".format(self._schm))
            cd = lib.ctidas_group_dict(self.cp)
            #sys.stderr.write("group ctor c dict = {}\n".format(cd))
            self._prps = dict_c2py(cd)
            #sys.stderr.write("group ctor py dict = {}\n".format(self._prps))
            self._sz = lib.ctidas_group_size(self.cp)
        else:
            self._schm = schema
            self._sz = int(size)
            self._prps = props

    def close(self):
        if self.cp is not None:
            #sys.stderr.write("group close c pointer {}\n".format(self.cp))
            lib.ctidas_group_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    @property
    def props(self):
        return self._prps

    @property
    def schema(self):
        return self._schm

    @property
    def size(self):
        return self._sz

    def _handle(self):
        return self.cp

    def resize(self, newsize):
        if self.cp is not None:
            lib.ctidas_group_resize(self.cp, newsize)
            self._sz = lib.ctidas_group_size(self.cp)
        else:
            self._sz = newsize
        return

    def range(self):
        start = ct.c_double(0)
        stop = ct.c_double(0)
        if self.cp is not None:
            lib.ctidas_group_range(self.cp, ct.byref(start), ct.byref(stop))
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return (start.value, stop.value)

    def read_times(self):
        data = np.zeros(self._sz, dtype=np.float64, order='C')
        if self.cp is not None:
            lib.ctidas_group_read_times(self.cp, self._sz, data)
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return data

    def write_times(self, data):
        if self.cp is not None:
            if data.shape[0] != self._sz:
                raise ValueError("cannot write full time vector which is smaller than the group")
            lib.ctidas_group_write_times(self.cp, self._sz, data)
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return

    def read(self, field, offset, ndata):
        last = offset + ndata
        if last > self._sz:
            raise IndexError("cannot read sample range {} - {} from group with {} samples".format(offset, last-1, self._sz))
        data = None
        cfield = field.encode("utf-8")
        pcf = ct.c_char_p(cfield)
        if self.cp is not None:
            # determine type and correct method to call
            if self._schm[field][0] == "int8":
                data = np.zeros(ndata, dtype=np.int8, order='C')
                lib.ctidas_group_read_int8(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint8":
                data = np.zeros(ndata, dtype=np.uint8, order='C')
                lib.ctidas_group_read_uint8(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int16":
                data = np.zeros(ndata, dtype=np.int16, order='C')
                lib.ctidas_group_read_int16(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint16":
                data = np.zeros(ndata, dtype=np.uint16, order='C')
                lib.ctidas_group_read_uint16(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int32":
                data = np.zeros(ndata, dtype=np.int32, order='C')
                lib.ctidas_group_read_int32(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint32":
                data = np.zeros(ndata, dtype=np.uint32, order='C')
                lib.ctidas_group_read_uint32(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int64":
                data = np.zeros(ndata, dtype=np.int64, order='C')
                lib.ctidas_group_read_int64(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint64":
                data = np.zeros(ndata, dtype=np.uint64, order='C')
                lib.ctidas_group_read_uint64(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "float32":
                data = np.zeros(ndata, dtype=np.float32, order='C')
                lib.ctidas_group_read_float(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "float64":
                data = np.zeros(ndata, dtype=np.float64, order='C')
                lib.ctidas_group_read_double(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "string":
                backsize = lib.ctidas_backend_string_size()
                cdata = lib.ctidas_string_alloc(ndata, backsize)
                lib.ctidas_group_read_string(self.cp, pcf, offset, ndata, cdata)
                data = []
                for i in range(ndata):
                    data.append( str(cdata[i].decode()) )
                lib.ctidas_string_free(ndata, cdata)
            else:
                raise ValueError("cannot read field with unknown data type \"{}\"".format(self.schm[field][0]))
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return data

    def write(self, field, offset, data):
        ndata = data.shape[0]
        last = offset + ndata
        if last > self._sz:
            raise IndexError("cannot write sample range {} - {} from group with {} samples".format(offset, last-1, self._sz))
        cfield = field.encode("utf-8")
        pcf = ct.c_char_p(cfield)
        if self.cp is not None:
            # determine type and correct method to call
            if self._schm[field][0] == "int8":
                lib.ctidas_group_write_int8(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint8":
                lib.ctidas_group_write_uint8(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int16":
                lib.ctidas_group_write_int16(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint16":
                lib.ctidas_group_write_uint16(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int32":
                lib.ctidas_group_write_int32(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint32":
                lib.ctidas_group_write_uint32(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "int64":
                lib.ctidas_group_write_int64(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "uint64":
                lib.ctidas_group_write_uint64(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "float32":
                lib.ctidas_group_write_float(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "float64":
                lib.ctidas_group_write_double(self.cp, pcf, offset, ndata, data)
            elif self._schm[field][0] == "string":
                backsize = lib.ctidas_backend_string_size()
                cdata = lib.ctidas_string_alloc(ndata, backsize)
                for i in range(ndata):
                    cdata[i] = data[i].decode()
                lib.ctidas_group_write_string(self.cp, field, offset, ndata, cdata)
                lib.ctidas_string_free(ndata, cdata)
            else:
                raise ValueError("cannot write field with unknown data type \"{}\"".format(self._schm[field][0]))
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return
            
    def info(self, name, indent=2):
        prf = "TIDAS:  "
        for i in range(indent):
            prf = "{} ".format(prf)
        start, stop = self.range()
        print("{}Group \"{}\" ({} samples) start = {}, stop = {}".format(prf, name, self.size, start, stop))
        p = self.props
        for key in sorted(p):
            print("{}  Properties {} = {}".format(prf, key, p[key]))
        s = self.schema
        for key in sorted(s):
            print("{}  Field {} ({}, {})".format(prf, key, s[key][0], s[key][1]))
        return


