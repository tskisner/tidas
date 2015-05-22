
import ctypes as ct

from ctypes.util import find_library

import libc as lc


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
    ct.c_uint(0) : 'none',
    ct.c_uint(1) : 'int8',
    ct.c_uint(2) : 'uint8',
    ct.c_uint(3) : 'int16',
    ct.c_uint(4) : 'uint16',
    ct.c_uint(5) : 'int32',
    ct.c_uint(6) : 'uint32',
    ct.c_uint(7) : 'int64',
    ct.c_uint(8) : 'uint64',
    ct.c_uint(9) : 'float32',
    ct.c_uint(10) : 'float64',
    ct.c_uint(11) : 'string'
}

backend_type = {
    'none' : ct.c_uint(0),
    'hdf5' : ct.c_uint(1),
    'getdata' : ct.c_uint(2)
}

backend_type_str = {
    ct.c_uint(0) : 'none',
    ct.c_uint(1) : 'hdf5',
    ct.c_uint(2) : 'getdata'
}

compression_type = {
    'none' : ct.c_uint(0),
    'gzip' : ct.c_uint(1),
    'bzip2' : ct.c_uint(2)
}

compression_type_str = {
    ct.c_uint(0) : 'none',
    ct.c_uint(1) : 'gzip',
    ct.c_uint(2) : 'bzip2'
}

access_mode = {
    'read' : ct.c_uint(0),
    'write' : ct.c_uint(1)
}

access_mode_str = {
    ct.c_uint(0) : 'read',
    ct.c_uint(1) : 'write'
}

exec_order = {
    'depth_first' : ct.c_uint(0),
    'depth_last' : ct.c_uint(1),
    'leaf' : ct.c_uint(2)
}

exec_order_str = {
    ct.c_uint(0) : 'depth_first',
    ct.c_uint(1) : 'depth_last',
    ct.c_uint(2) : 'leaf'
}


# open library

library_path = find_library('tidas')

lib = ct.CDLL(library_path, mode=ct.RTLD_GLOBAL)


# utils

lib.ctidas_string_alloc.restype = ct.POINTER(ct.c_char_p)
lib.ctidas_string_alloc.argtypes = [ ct.c_ulong, ct.c_ulong ]

lib.ctidas_string_free.restype = None
lib.ctidas_string_free.argtypes = [ ct.c_ulong, ct.POINTER(ct.c_char_p) ]


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
lib.ctidas_dict_keys.argtypes = [ ct.POINTER(cDict), ct.byref(ct.c_ulong) ]

def cdict_keys(dct):
    ret = []
    nkeys = ct.c_ulong(0)
    buf = lib.ctidas_dict_keys(dct, nkeys)
    for k in range(nkeys):
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

lib.ctidas_dict_get_string.restype = ct.c_char_p
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
    typ = ct.c_uint(0)
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
    elif data_type_str[typ] == 'float':
        return lib.ctidas_dict_get_float(dct, key)
    elif data_type_str[typ] == 'double':
        return lib.ctidas_dict_get_double(dct, key)
    elif data_type_str[typ] == 'string':
        tmpstr = lib.ctidas_dict_get_string(dct, key)
        retstr = str(tmpstr)
        lc.free(tmpstr)
        return retstr
    else:
        raise ValueError("unknown tidas dict type \"{}\" for key \"{}\"".format(typ, key))
    return

def pydict(dct):
    ret = {}
    keys = cdict_keys
    for k in keys:
        ret[k] = cdict_get(dct, k)
    return ret

def cdict(pdct):
    ret = ct.POINTER(cDict)
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


class cSchema(ct.Structure):
    pass

    

ctidas_schema * ctidas_schema_alloc ( );

void ctidas_schema_free ( ctidas_schema * schm );

void ctidas_schema_clear ( ctidas_schema * schm );

char ** ctidas_schema_fields ( ctidas_schema const * schm, size_t * nfields );

void ctidas_schema_field_add ( ctidas_schema * schm, ctidas_field const * fld );

void ctidas_schema_field_del ( ctidas_schema * schm, char const * name );

ctidas_field * ctidas_schema_field_get ( ctidas_schema const * schm, char const * name );


# intervals


# group

class cGroup(ct.Structure):
    pass


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




# ctidas_group * ctidas_block_group_add ( ctidas_block * blk, char const * name, ctidas_group * grp );

# ctidas_group * ctidas_block_group_get ( ctidas_block * blk, char const * name );

# ctidas_group const * ctidas_block_group_cget ( ctidas_block const * blk, char const * name );

# void ctidas_block_group_del ( ctidas_block * blk, char const * name );

# char ** ctidas_block_all_groups ( ctidas_block const * blk, size_t * ngroup );

# void ctidas_block_clear_groups ( ctidas_block * blk );


# ctidas_intervals * ctidas_block_intervals_add ( ctidas_block * blk, char const * name, ctidas_intervals * inv );

# ctidas_intervals * ctidas_block_intervals_get ( ctidas_block * blk, char const * name );

# ctidas_intervals const * ctidas_block_intervals_cget ( ctidas_block const * blk, char const * name );

# void ctidas_block_intervals_del ( ctidas_block * blk, char const * name );

# char ** ctidas_block_all_intervals ( ctidas_block const * blk, size_t * nintervals );

# void ctidas_block_clear_intervals ( ctidas_block * blk );


# ctidas_block * ctidas_block_child_add ( ctidas_block * blk, char const * name, ctidas_block * child );

# ctidas_block * ctidas_block_child_get ( ctidas_block * blk, char const * name );

# ctidas_block const * ctidas_block_child_cget ( ctidas_block const * blk, char const * name );

# void ctidas_block_child_del ( ctidas_block * blk, char const * name );

# char ** ctidas_block_all_children ( ctidas_block const * blk, size_t * nchild );

# void ctidas_block_clear_children ( ctidas_block * blk );


# ctidas_block * ctidas_block_select ( ctidas_block const * blk, char const * filter );


cBlockExec = ct.CFUNCTYPE(None, ct.POINTER(cBlock), ct.c_void_p)


# typedef void (*CTIDAS_EXEC_OP) ( ctidas_block const * blk, void * aux );

# void ctidas_block_exec ( ctidas_block const * blk, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux );


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

#void ctidas_volume_exec ( ctidas_volume * vol, ctidas_exec_order order, CTIDAS_EXEC_OP op, void * aux );

