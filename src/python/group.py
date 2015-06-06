
import ctypes as ct
import numpy as np

import ctidas as tds


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
            cs = tds.lib.ctidas_group_schema(self.cp)
            self._schm = tds.schema_c2py(cs)
            tds.lib.ctidas_schema_free(cs)
            cd = tds.lib.ctidas_group_dict(self.cp)
            self._prps = tds.dict_c2py(cd)
            tds.lib.ctidas_dict_free(cd)
            self._sz = tds.lib.ctidas_group_size(self.cp)
        else:
            self._schm = schema
            self._sz = size
            self._prps = props

    def close(self):
        if self.cp is not None:
            tds.lib.ctidas_group_free(self.cp)
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

    def resize(self, newsize):
        if self.cp is not None:
            tds.lib.ctidas_group_resize(self.cp, newsize)
            self._sz = tds.lib.ctidas_group_size(self.cp)
        else:
            self._sz = newsize
        return

    def range(self):
        start = ct.c_double(0)
        stop = ct.c_double(0)
        if self.cp is not None:
            tds.lib.ctidas_group_range(self.cp, start, stop)
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return (start, stop)

    def read_times(self):
        data = np.zeros(self._sz, dtype=np.float64, order='C')
        if self.cp is not None:
            tds.lib.ctidas_group_read_times(self.cp, self._sz, data)
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return data

    def write_times(self, data):
        if self.cp is not None:
            if data.shape[0] != self._sz:
                raise ValueError("cannot write full time vector which is smaller than the group")
            tds.lib.ctidas_group_write_times(self.cp, self._sz, data)
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return

    def read(self, field, offset, ndata):
        last = offset + ndata
        if last > self._sz:
            raise IndexError("cannot read sample range {} - {} from group with {} samples".format(offset, last-1, self._sz))
        data = None
        if self.cp is not None:
            # determine type and correct method to call
            if self._schm[field][0] == "int8":
                data = np.zeros(ndata, dtype=np.int8, order='C')
                tds.lib.ctidas_group_read_int8(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint8":
                data = np.zeros(ndata, dtype=np.uint8, order='C')
                tds.lib.ctidas_group_read_uint8(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int16":
                data = np.zeros(ndata, dtype=np.int16, order='C')
                tds.lib.ctidas_group_read_int16(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint16":
                data = np.zeros(ndata, dtype=np.uint16, order='C')
                tds.lib.ctidas_group_read_uint16(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int32":
                data = np.zeros(ndata, dtype=np.int32, order='C')
                tds.lib.ctidas_group_read_int32(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint32":
                data = np.zeros(ndata, dtype=np.uint32, order='C')
                tds.lib.ctidas_group_read_uint32(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int64":
                data = np.zeros(ndata, dtype=np.int64, order='C')
                tds.lib.ctidas_group_read_int64(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint64":
                data = np.zeros(ndata, dtype=np.uint64, order='C')
                tds.lib.ctidas_group_read_uint64(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "float":
                data = np.zeros(ndata, dtype=np.float32, order='C')
                tds.lib.ctidas_group_read_float(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "double":
                data = np.zeros(ndata, dtype=np.float64, order='C')
                tds.lib.ctidas_group_read_double(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "string":
                backsize = tds.lib.ctidas_backend_string_size()
                cdata = tds.lib.ctidas_string_alloc(ndata, backsize)
                tds.lib.ctidas_group_read_string(self.cp, field, offset, ndata, cdata)
                data = []
                for i in range(ndata):
                    data.append( str(cdata[i]) )
                tds.lib.ctidas_string_free(ndata, cdata)
            else:
                raise ValueError("cannot read field with unknown data type \"{}\"".format(self.schm[field][0]))
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return data

    def write(self, field, offset, data):
        ndata = len(data)
        last = offset + ndata
        if last > self.sz:
            raise IndexError("cannot write sample range {} - {} from group with {} samples".format(offset, last-1, self.sz))
        if self.cp is not None:
            # determine type and correct method to call
            if self._schm[field][0] == "int8":
                tds.lib.ctidas_group_write_int8(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint8":
                tds.lib.ctidas_group_write_uint8(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int16":
                tds.lib.ctidas_group_write_int16(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint16":
                tds.lib.ctidas_group_write_uint16(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int32":
                tds.lib.ctidas_group_write_int32(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint32":
                tds.lib.ctidas_group_write_uint32(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "int64":
                tds.lib.ctidas_group_write_int64(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "uint64":
                tds.lib.ctidas_group_write_uint64(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "float":
                tds.lib.ctidas_group_write_float(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "double":
                tds.lib.ctidas_group_write_double(self.cp, field, offset, ndata, data)
            elif self._schm[field][0] == "string":
                backsize = tds.lib.ctidas_backend_string_size()
                cdata = tds.lib.ctidas_string_alloc(ndata, backsize)
                for i in range(ndata):
                    cdata[i] = data[i]
                tds.lib.ctidas_group_write_string(self.cp, field, offset, ndata, cdata)
                tds.lib.ctidas_string_free(ndata, cdata)
            else:
                raise ValueError("cannot write field with unknown data type \"{}\"".format(self.schm[field][0]))
        else:
            raise RuntimeError("group is not associated with a block- cannot read or write data")
        return
            



