
import ctypes as ct

from ctypes.util import find_library

import numpy as np
import numpy.ctypeslib as npc


# parameters that map to C enums

data_type = {
    'none' : ct.c_uint(0),
    'int8' : ct.c_uint(1),
    'uint8' : ct.c_uint(2),
    'int16' : ct.c_uint(3),
    'uint16' : ct.c_uint(4),
    'int32' : ct.c_uint(5),
    'uint32' : ct.c_uint(6),
    'int64' : ct.c_uint(7),
    'uint64' : ct.c_uint(8),
    'float32' : ct.c_uint(9),
    'float64' : ct.c_uint(10),
    'string' : ct.c_uint(11)
}

data_type_str = {
    0 : 'none',
    1 : 'int8',
    2 : 'uint8',
    3 : 'int16',
    4 : 'uint16',
    5 : 'int32',
    6 : 'uint32',
    7 : 'int64',
    8 : 'uint64',
    9 : 'float32',
    10 : 'float64',
    11 : 'string'
}

data_type_to_npy = {
    'int8' : np.int8,
    'uint8' : np.uint8,
    'int16' : np.int16,
    'uint16' : np.uint16,
    'int32' : np.int32,
    'uint32' : np.uint32,
    'int64' : np.int64,
    'uint64' : np.uint64,
    'float32' : np.float32,
    'float64' : np.float64,
}

backend_type = {
    'none' : ct.c_uint(0),
    'hdf5' : ct.c_uint(1),
    'getdata' : ct.c_uint(2)
}

backend_type_str = {
    ct.c_uint(0).value : 'none',
    ct.c_uint(1).value : 'hdf5',
    ct.c_uint(2).value : 'getdata'
}

compression_type = {
    'none' : ct.c_uint(0),
    'gzip' : ct.c_uint(1),
    'bzip2' : ct.c_uint(2)
}

compression_type_str = {
    ct.c_uint(0).value : 'none',
    ct.c_uint(1).value : 'gzip',
    ct.c_uint(2).value : 'bzip2'
}

access_mode = {
    'read' : ct.c_uint(0),
    'write' : ct.c_uint(1)
}

access_mode_str = {
    ct.c_uint(0).value : 'read',
    ct.c_uint(1).value : 'write'
}

exec_order = {
    'depth_first' : ct.c_uint(0),
    'depth_last' : ct.c_uint(1),
    'leaf' : ct.c_uint(2)
}

exec_order_str = {
    ct.c_uint(0).value : 'depth_first',
    ct.c_uint(1).value : 'depth_last',
    ct.c_uint(2).value : 'leaf'
}


# open library

library_path = find_library('tidas')
lib = ct.CDLL(library_path, mode=ct.RTLD_GLOBAL)


# libc routines

libc_path = find_library('libc')
libc = ct.CDLL(libc_path, mode=ct.RTLD_GLOBAL)

libc.free.restype = None
libc.free.argtypes = [ ct.c_void_p ]


# utils

lib.ctidas_string_alloc.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_string_alloc.argtypes = [ ct.c_ulong, ct.c_ulong ]

lib.ctidas_string_free.restype = None
lib.ctidas_string_free.argtypes = [ ct.c_ulong, ct.POINTER(ct.c_char_p) ]

lib.ctidas_backend_string_size.restype = ct.c_ulong
lib.ctidas_backend_string_size.argtypes = []

# define a subclass of ctypes.c_char_p, for use by functions where
# we want access to the raw char_p returned (in order to free it)

class raw_c_char_p(ct.c_char_p):
    pass

# dictionary

class cDict(ct.Structure):
    pass

lib.ctidas_dict_alloc.restype = ct.POINTER(cDict)
lib.ctidas_dict_alloc.argtypes = []

lib.ctidas_dict_free.restype = None
lib.ctidas_dict_free.argtypes = [ ct.POINTER(cDict) ]

lib.ctidas_dict_clear.restype = None
lib.ctidas_dict_clear.argtypes = [ ct.POINTER(cDict) ]

lib.ctidas_dict_nkeys.restype = ct.c_ulong
lib.ctidas_dict_nkeys.argtypes = [ ct.POINTER(cDict) ]

lib.ctidas_dict_keys.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_dict_keys.argtypes = [ ct.POINTER(cDict), ct.POINTER(ct.c_ulong) ]

def cdict_keys(dct):
    ret = []
    nkeys = ct.c_ulong(0)
    buf = lib.ctidas_dict_keys(dct, ct.byref(nkeys))
    for k in range(nkeys.value):
        ret.append(str(buf[k]))
    lib.ctidas_string_free(nkeys, buf)
    return ret

lib.ctidas_dict_type.restype = ct.c_uint
lib.ctidas_dict_type.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_put_int8.restype = None
lib.ctidas_dict_put_int8.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_byte ]

lib.ctidas_dict_put_int16.restype = None
lib.ctidas_dict_put_int16.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_short ]

lib.ctidas_dict_put_int32.restype = None
lib.ctidas_dict_put_int32.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_int ]

lib.ctidas_dict_put_int64.restype = None
lib.ctidas_dict_put_int64.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_longlong ]

lib.ctidas_dict_put_uint8.restype = None
lib.ctidas_dict_put_uint8.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_ubyte ]

lib.ctidas_dict_put_uint16.restype = None
lib.ctidas_dict_put_uint16.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_ushort ]

lib.ctidas_dict_put_uint32.restype = None
lib.ctidas_dict_put_uint32.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_uint ]

lib.ctidas_dict_put_uint64.restype = None
lib.ctidas_dict_put_uint64.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_ulonglong ]

lib.ctidas_dict_put_float.restype = None
lib.ctidas_dict_put_float.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_float ]

lib.ctidas_dict_put_double.restype = None
lib.ctidas_dict_put_double.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_double ]

lib.ctidas_dict_put_string.restype = None
lib.ctidas_dict_put_string.argtypes = [ ct.POINTER(cDict), ct.c_char_p, ct.c_char_p ]

lib.ctidas_dict_get_int8.restype = ct.c_byte
lib.ctidas_dict_get_int8.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_int16.restype = ct.c_short
lib.ctidas_dict_get_int16.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_int32.restype = ct.c_int
lib.ctidas_dict_get_int32.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_int64.restype = ct.c_longlong
lib.ctidas_dict_get_int64.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_uint8.restype = ct.c_ubyte
lib.ctidas_dict_get_uint8.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_uint16.restype = ct.c_ushort
lib.ctidas_dict_get_uint16.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_uint32.restype = ct.c_uint
lib.ctidas_dict_get_uint32.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_uint64.restype = ct.c_ulonglong
lib.ctidas_dict_get_uint64.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_float.restype = ct.c_float
lib.ctidas_dict_get_float.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_double.restype = ct.c_double
lib.ctidas_dict_get_double.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

lib.ctidas_dict_get_string.restype = raw_c_char_p
lib.ctidas_dict_get_string.argtypes = [ ct.POINTER(cDict), ct.c_char_p ]

def cdict_put(dct, key, val):
    if type(val) is int:
        lib.ctidas_dict_put_int32(dct, key, ct.c_int(val))
    elif type(val) is long:
        lib.ctidas_dict_put_int64(dct, key, ct.c_longlong(val))
    elif type(val) is float:
        lib.ctidas_dict_put_double(dct, key, ct.c_double(val))
    elif type(val) is str:
        lib.ctidas_dict_put_string(dct, key, val)
    else:
        raise ValueError("unknown python type \"{}\" for value \"{}\"".format(type(val), val))
    return

def cdict_get(dct, key):
    typ = lib.ctidas_dict_type(dct, key)
    if data_type_str[typ] == 'int8':
        return lib.ctidas_dict_get_int8(dct, key)
    elif data_type_str[typ] == 'int16':
        return lib.ctidas_dict_get_int16(dct, key)
    elif data_type_str[typ] == 'int32':
        return lib.ctidas_dict_get_int32(dct, key)
    elif data_type_str[typ] == 'int64':
        return lib.ctidas_dict_get_int64(dct, key)
    elif data_type_str[typ] == 'uint8':
        return lib.ctidas_dict_get_uint8(dct, key)
    elif data_type_str[typ] == 'uint16':
        return lib.ctidas_dict_get_uint16(dct, key)
    elif data_type_str[typ] == 'uint32':
        return lib.ctidas_dict_get_uint32(dct, key)
    elif data_type_str[typ] == 'uint64':
        return lib.ctidas_dict_get_uint64(dct, key)
    elif data_type_str[typ] == 'float32':
        return lib.ctidas_dict_get_float(dct, key)
    elif data_type_str[typ] == 'float64':
        return lib.ctidas_dict_get_double(dct, key)
    elif data_type_str[typ] == 'string':
        tmpstr = lib.ctidas_dict_get_string(dct, key)
        retstr = str(tmpstr.value)
        libc.free(tmpstr)
        return retstr
    else:
        raise ValueError("unknown tidas dict type \"{}\" for key \"{}\"".format(typ, key))
    return

def dict_c2py(dct):
    ret = {}
    keys = cdict_keys(dct)
    for k in keys:
        ret[k] = cdict_get(dct, k)
    return ret

def dict_py2c(pdct):
    ret = lib.ctidas_dict_alloc()
    for k in pdct.keys():
        cdict_put(ret, k, pdct[k])
    return ret


# schema


class cField(ct.Structure):
    pass

lib.ctidas_field_alloc.restype = ct.POINTER(cField)
lib.ctidas_field_alloc.argtypes = []

lib.ctidas_field_free.restype = None
lib.ctidas_field_free.argtypes = [ ct.POINTER(cField) ]

lib.ctidas_field_type_set.restype = None
lib.ctidas_field_type_set.argtypes = [ ct.POINTER(cField), ct.c_uint ]

lib.ctidas_field_name_set.restype = None
lib.ctidas_field_name_set.argtypes = [ ct.POINTER(cField), ct.c_char_p ]

lib.ctidas_field_units_set.restype = None
lib.ctidas_field_units_set.argtypes = [ ct.POINTER(cField), ct.c_char_p ]

lib.ctidas_field_type_get.restype = ct.c_uint
lib.ctidas_field_type_get.argtypes = [ ct.POINTER(cField) ]

lib.ctidas_field_name_get.restype = ct.c_char_p
lib.ctidas_field_name_get.argtypes = [ ct.POINTER(cField) ]

lib.ctidas_field_units_get.restype = ct.c_char_p
lib.ctidas_field_units_get.argtypes = [ ct.POINTER(cField) ]

def field_c2py(fld):
    name = str(lib.ctidas_field_name_get(fld))
    units = str(lib.ctidas_field_units_get(fld))
    typ = lib.ctidas_field_type_get(fld)
    return (name, data_type_str[typ], units)

def field_py2c(pfld):
    if len(pfld) != 3:
        raise ValueError("field tuple must have 3 elements (name, type, units)")
    fld = lib.ctidas_field_alloc()
    lib.ctidas_field_name_set(fld, pfld[0])
    typ = data_type[pfld[1]]
    lib.ctidas_field_type_set(fld, typ)
    lib.ctidas_field_units_set(fld, pfld[2])
    return fld


class cSchema(ct.Structure):
    pass

lib.ctidas_schema_alloc.restype = ct.POINTER(cSchema)
lib.ctidas_schema_alloc.argtypes = []

lib.ctidas_schema_free.restype = None
lib.ctidas_schema_free.argtypes = [ ct.POINTER(cSchema) ]

lib.ctidas_schema_clear.restype = None
lib.ctidas_schema_clear.argtypes = [ ct.POINTER(cSchema) ]

lib.ctidas_schema_fields.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_schema_fields.argtypes = [ ct.POINTER(cSchema), ct.POINTER(ct.c_ulong) ]

def cschema_fields(schm):
    ret = []
    nfields = ct.c_ulong(0)
    buf = lib.ctidas_schema_fields(schm, ct.byref(nfields))
    for f in range(nfields.value):
        ret.append(str(buf[f]))
    lib.ctidas_string_free(nfields, buf)
    return ret

lib.ctidas_schema_field_add.restype = None
lib.ctidas_schema_field_add.argtypes = [ ct.POINTER(cSchema), ct.POINTER(cField) ]

lib.ctidas_schema_field_del.restype = None
lib.ctidas_schema_field_del.argtypes = [ ct.POINTER(cSchema), ct.c_char_p ]

lib.ctidas_schema_field_get.restype = ct.POINTER(cField)
lib.ctidas_schema_field_get.argtypes = [ ct.POINTER(cSchema), ct.c_char_p ]

def schema_c2py(schm):
    ret = {}
    fields = cschema_fields(schm)
    for f in fields:
        cf = lib.ctidas_schema_field_get(schm, f)
        pf = field_c2py(cf)
        ret[pf[0]] = (pf[1], pf[2])
        lib.ctidas_field_free(cf)
    return ret

def schema_py2c(fields):
    ret = lib.ctidas_schema_alloc()
    for f in fields.keys():
        fld = field_py2c( (f, fields[f][0], fields[f][1]) )
        lib.ctidas_schema_field_add(ret, fld)
        lib.ctidas_field_free(fld)
    return ret


# intervals

class cIntrvl(ct.Structure):
    pass

lib.ctidas_intrvl_alloc.restype = ct.POINTER(cIntrvl)
lib.ctidas_intrvl_alloc.argtypes = []

lib.ctidas_intrvl_free.restype = None
lib.ctidas_intrvl_free.argtypes = [ ct.POINTER(cIntrvl) ]

lib.ctidas_intrvl_start_set.restype = None
lib.ctidas_intrvl_start_set.argtypes = [ ct.POINTER(cIntrvl), ct.c_double ]

lib.ctidas_intrvl_start_get.restype = ct.c_double
lib.ctidas_intrvl_start_get.argtypes = [ ct.POINTER(cIntrvl) ]

lib.ctidas_intrvl_stop_set.restype = None
lib.ctidas_intrvl_stop_set.argtypes = [ ct.POINTER(cIntrvl), ct.c_double ]

lib.ctidas_intrvl_stop_get.restype = ct.c_double
lib.ctidas_intrvl_stop_get.argtypes = [ ct.POINTER(cIntrvl) ]

lib.ctidas_intrvl_first_set.restype = None
lib.ctidas_intrvl_first_set.argtypes = [ ct.POINTER(cIntrvl), ct.c_longlong ]

lib.ctidas_intrvl_first_get.restype = ct.c_longlong
lib.ctidas_intrvl_first_get.argtypes = [ ct.POINTER(cIntrvl) ]

lib.ctidas_intrvl_last_set.restype = None
lib.ctidas_intrvl_last_set.argtypes = [ ct.POINTER(cIntrvl), ct.c_longlong ]

lib.ctidas_intrvl_last_get.restype = ct.c_longlong
lib.ctidas_intrvl_last_get.argtypes = [ ct.POINTER(cIntrvl) ]

class Intrvl(object):

    def __init__(self, start=0.0, stop=0.0, first=-1L, last=-1L):
        self.start = start
        self.stop = stop
        self.first = first
        self.last = last

lib.ctidas_intrvl_list_alloc.restype = ct.POINTER(ct.POINTER(cIntrvl))
lib.ctidas_intrvl_list_alloc.argtypes = [ ct.c_ulong ]

lib.ctidas_intrvl_list_free.restype = None
lib.ctidas_intrvl_list_free.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong ]

def intrvl_list_c2py(intrl, size):
    ret = []
    for i in range(size):
        start = lib.ctidas_intrvl_start_get(intrl[i])
        stop = lib.ctidas_intrvl_stop_get(intrl[i])
        first = lib.ctidas_intrvl_first_get(intrl[i])
        last = lib.ctidas_intrvl_last_get(intrl[i])
        ret.append(Intrvl(start=start, stop=stop, first=first, last=last))
    return ret

def intrvl_list_py2c(pintrl):
    size = len(pintrl)
    ret = lib.ctidas_intrvl_list_alloc(size)
    for i in range(size):
        lib.ctidas_intrvl_start_set(ret[i], ct.c_double(pintrl[i].start))
        lib.ctidas_intrvl_stop_set(ret[i], ct.c_double(pintrl[i].stop))
        lib.ctidas_intrvl_first_set(ret[i], ct.c_longlong(pintrl[i].first))
        lib.ctidas_intrvl_last_set(ret[i], ct.c_longlong(pintrl[i].last))
    return ret


class cIntervals(ct.Structure):
    pass

lib.ctidas_intervals_alloc.restype = ct.POINTER(cIntervals)
lib.ctidas_intervals_alloc.argtypes = [ ct.POINTER(cDict), ct.c_ulong ]

lib.ctidas_intervals_free.restype = None
lib.ctidas_intervals_free.argtypes = [ ct.POINTER(cIntervals) ]

lib.ctidas_intervals_size.restype = ct.c_ulong
lib.ctidas_intervals_size.argtypes = [ ct.POINTER(cIntervals) ]

lib.ctidas_intervals_dict.restype = ct.POINTER(cDict)
lib.ctidas_intervals_dict.argtypes = [ ct.POINTER(cIntervals) ]

lib.ctidas_intervals_read.restype = ct.POINTER(ct.POINTER(cIntrvl))
lib.ctidas_intervals_read.argtypes = [ ct.POINTER(cIntervals), ct.POINTER(ct.c_ulong) ]

lib.ctidas_intervals_write.restype = None
lib.ctidas_intervals_write.argtypes = [ ct.POINTER(cIntervals), ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong ]

lib.ctidas_intervals_samples.restype = ct.c_longlong
lib.ctidas_intervals_samples.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong ]

lib.ctidas_intervals_time.restype = ct.c_double
lib.ctidas_intervals_time.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong ]

lib.ctidas_intervals_seek.restype = ct.POINTER(cIntrvl)
lib.ctidas_intervals_seek.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong, ct.c_double ]

lib.ctidas_intervals_seek_ceil.restype = ct.POINTER(cIntrvl)
lib.ctidas_intervals_seek_ceil.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong, ct.c_double ]

lib.ctidas_intervals_seek_floor.restype = ct.POINTER(cIntrvl)
lib.ctidas_intervals_seek_floor.argtypes = [ ct.POINTER(ct.POINTER(cIntrvl)), ct.c_ulong, ct.c_double ]


# group

class cGroup(ct.Structure):
    pass

lib.ctidas_group_alloc.restype = ct.POINTER(cGroup)
lib.ctidas_group_alloc.argtypes = [ ct.POINTER(cSchema), ct.POINTER(cDict), ct.c_longlong ]

lib.ctidas_group_free.restype = None
lib.ctidas_group_free.argtypes = [ ct.POINTER(cGroup) ]

lib.ctidas_group_dict.restype = ct.POINTER(cDict)
lib.ctidas_group_dict.argtypes = [ ct.POINTER(cGroup) ]

lib.ctidas_group_schema.restype = ct.POINTER(cSchema)
lib.ctidas_group_schema.argtypes = [ ct.POINTER(cGroup) ]

lib.ctidas_group_size.restype = ct.c_longlong
lib.ctidas_group_size.argtypes = [ ct.POINTER(cGroup) ]

lib.ctidas_group_resize.restype = None
lib.ctidas_group_resize.argtypes = [ ct.POINTER(cGroup), ct.c_longlong ]

lib.ctidas_group_range.restype = None
lib.ctidas_group_range.argtypes = [ ct.POINTER(cGroup), ct.POINTER(ct.c_double), ct.POINTER(ct.c_double) ]

lib.ctidas_group_read_times.restype = None
lib.ctidas_group_read_times.argtypes = [ ct.POINTER(cGroup), ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_times.restype = None
lib.ctidas_group_write_times.argtypes = [ ct.POINTER(cGroup), ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_int8.restype = None
lib.ctidas_group_read_int8.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int8"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_int8.restype = None
lib.ctidas_group_write_int8.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int8"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_int16.restype = None
lib.ctidas_group_read_int16.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int16"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_int16.restype = None
lib.ctidas_group_write_int16.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int16"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_int32.restype = None
lib.ctidas_group_read_int32.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_int32.restype = None
lib.ctidas_group_write_int32.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_int64.restype = None
lib.ctidas_group_read_int64.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_int64.restype = None
lib.ctidas_group_write_int64.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["int64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_uint8.restype = None
lib.ctidas_group_read_uint8.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint8"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_uint8.restype = None
lib.ctidas_group_write_uint8.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint8"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_uint16.restype = None
lib.ctidas_group_read_uint16.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint16"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_uint16.restype = None
lib.ctidas_group_write_uint16.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint16"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_uint32.restype = None
lib.ctidas_group_read_uint32.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_uint32.restype = None
lib.ctidas_group_write_uint32.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_uint64.restype = None
lib.ctidas_group_read_uint64.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_uint64.restype = None
lib.ctidas_group_write_uint64.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["uint64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_float.restype = None
lib.ctidas_group_read_float.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_float.restype = None
lib.ctidas_group_write_float.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float32"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_double.restype = None
lib.ctidas_group_read_double.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_write_double.restype = None
lib.ctidas_group_write_double.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, npc.ndpointer(dtype=data_type_to_npy["float64"], ndim=1, flags='C_CONTIGUOUS') ]

lib.ctidas_group_read_string.restype = None
lib.ctidas_group_read_string.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, ct.POINTER(ct.c_char_p) ]

lib.ctidas_group_write_string.restype = None
lib.ctidas_group_write_string.argtypes = [ ct.POINTER(cGroup), ct.c_char_p, ct.c_longlong, ct.c_longlong, ct.POINTER(ct.c_char_p) ]


# block

class cBlock(ct.Structure):
    pass

lib.ctidas_block_alloc.restype = ct.POINTER(cBlock)
lib.ctidas_block_alloc.argtypes = []

lib.ctidas_block_free.restype = None
lib.ctidas_block_free.argtypes = [ ct.POINTER(cBlock) ]

lib.ctidas_block_range.restype = None
lib.ctidas_block_range.argtypes = [ ct.POINTER(cBlock), ct.POINTER(ct.c_double), ct.POINTER(ct.c_double) ]

lib.ctidas_block_clear.restype = None
lib.ctidas_block_clear.argtypes = [ ct.POINTER(cBlock) ]

lib.ctidas_block_group_add.restype = ct.POINTER(cGroup)
lib.ctidas_block_group_add.argtypes = [ ct.POINTER(cBlock), ct.c_char_p, ct.POINTER(cGroup) ]

lib.ctidas_block_group_get.restype = ct.POINTER(cGroup)
lib.ctidas_block_group_get.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_group_del.restype = None
lib.ctidas_block_group_del.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_all_groups.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_block_all_groups.argtypes = [ ct.POINTER(cBlock), ct.POINTER(ct.c_ulong) ]

lib.ctidas_block_clear_groups.restype = None
lib.ctidas_block_clear_groups.argtypes = [ ct.POINTER(cBlock) ]

lib.ctidas_block_intervals_add.restype = ct.POINTER(cIntervals)
lib.ctidas_block_intervals_add.argtypes = [ ct.POINTER(cBlock), ct.c_char_p, ct.POINTER(cIntervals) ]

lib.ctidas_block_intervals_get.restype = ct.POINTER(cIntervals)
lib.ctidas_block_intervals_get.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_intervals_del.restype = None
lib.ctidas_block_intervals_del.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_all_intervals.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_block_all_intervals.argtypes = [ ct.POINTER(cBlock), ct.POINTER(ct.c_ulong) ]

lib.ctidas_block_clear_intervals.restype = None
lib.ctidas_block_clear_intervals.argtypes = [ ct.POINTER(cBlock) ]

lib.ctidas_block_child_add.restype = ct.POINTER(cBlock)
lib.ctidas_block_child_add.argtypes = [ ct.POINTER(cBlock), ct.c_char_p, ct.POINTER(cBlock) ]

lib.ctidas_block_child_get.restype = ct.POINTER(cBlock)
lib.ctidas_block_child_get.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_child_del.restype = None
lib.ctidas_block_child_del.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

lib.ctidas_block_all_children.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_block_all_children.argtypes = [ ct.POINTER(cBlock), ct.POINTER(ct.c_ulong) ]

lib.ctidas_block_clear_children.restype = None
lib.ctidas_block_clear_children.argtypes = [ ct.POINTER(cBlock) ]

lib.ctidas_block_select.restype = ct.POINTER(cBlock)
lib.ctidas_block_select.argtypes = [ ct.POINTER(cBlock), ct.c_char_p ]

cBlockExec = ct.CFUNCTYPE(None, ct.POINTER(cBlock), ct.c_void_p)

lib.ctidas_block_exec.restype = None
lib.ctidas_block_exec.argtypes = [ ct.POINTER(cBlock), ct.c_uint, cBlockExec, ct.c_void_p ]

def cblock_groups(blk):
    ret = []
    n = ct.c_ulong(0)
    buf = lib.ctidas_block_all_groups(blk, ct.byref(n))
    for i in range(n.value):
        ret.append(str(buf[i]))
    lib.ctidas_string_free(n, buf)
    return ret

def cblock_intervals(blk):
    ret = []
    n = ct.c_ulong(0)
    buf = lib.ctidas_block_all_intervals(blk, ct.byref(n))
    for i in range(n.value):
        ret.append(str(buf[i]))
    lib.ctidas_string_free(n, buf)
    return ret

def cblock_children(blk):
    ret = []
    n = ct.c_ulong(0)
    buf = lib.ctidas_block_all_children(blk, ct.byref(n))
    for i in range(n.value):
        ret.append(str(buf[i]))
    lib.ctidas_string_free(n, buf)
    return ret

# volume

class cVolume(ct.Structure):
    pass

lib.ctidas_volume_create.restype = ct.POINTER(cVolume)
lib.ctidas_volume_create.argtypes = [ ct.c_char_p, ct.c_uint, ct.c_uint ]

lib.ctidas_volume_open.restype = ct.POINTER(cVolume)
lib.ctidas_volume_open.argtypes = [ ct.c_char_p, ct.c_uint ]

lib.ctidas_volume_close.restype = None
lib.ctidas_volume_close.argtypes = [ ct.POINTER(cVolume) ]

lib.ctidas_volume_root.restype = ct.POINTER(cBlock)
lib.ctidas_volume_root.argtypes = [ ct.POINTER(cVolume) ]

#lib.ctidas_volume_exec.restype = None
#lib.ctidas_volume_exec.argtypes = [ ct.POINTER(cVolume), ct.c_uint, cBlockExec, ct.c_void_p ]

