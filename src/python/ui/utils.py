##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
"""TIDAS User interface utilities.
"""

from .._pytidas import (DataType, Dictionary)


def to_dict(td):
    ret = dict()
    for k in td.keys():
        tp = td.get_type(k)
        if (tp == "d"):
            ret[k] = td.get_float64(k)
        elif (tp == "f"):
            ret[k] = td.get_float32(k)
        elif (tp == "l"):
            ret[k] = td.get_int64(k)
        elif (tp == "L"):
            ret[k] = td.get_uint64(k)
        elif (tp == "i"):
            ret[k] = td.get_int32(k)
        elif (tp == "I"):
            ret[k] = td.get_uint32(k)
        elif (tp == "h"):
            ret[k] = td.get_int16(k)
        elif (tp == "H"):
            ret[k] = td.get_uint16(k)
        elif (tp == "b"):
            ret[k] = td.get_int8(k)
        elif (tp == "B"):
            ret[k] = td.get_uint8(k)
        else:
            ret[k] = td.get_string(k)
    return ret
