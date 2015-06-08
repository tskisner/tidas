##
##  TImestream DAta Storage (TIDAS)
##  (c) 2014-2015, The Regents of the University of California, 
##  through Lawrence Berkeley National Laboratory.  See top
##  level LICENSE file for details.
##

import os
import tempfile
import numpy.testing as nt

import ctypes as ct

from .ctidas import *
from .volume import *


def test_dict_setup():
    ret = {}
    ret["string"] = "blahblahblah"
    ret["double"] = -123456789.0123
    ret["float"] = -123456789.0123
    ret["int8"] = -100
    ret["uint8"] = 100
    ret["int16"] = -10000
    ret["uint16"] = 10000
    ret["int32"] = -1000000000
    ret["uint32"] = 1000000000
    ret["int64"] = -100000000000L
    ret["uint64"] = 100000000000L
    return ret


def test_dict_verify(dct):
    nt.assert_equal(dct["string"], "blahblahblah")
    nt.assert_equal(dct["int8"], -100)
    nt.assert_equal(dct["uint8"], 100)
    nt.assert_equal(dct["int16"], -10000)
    nt.assert_equal(dct["uint16"], 10000)
    nt.assert_equal(dct["int32"], -1000000000)
    nt.assert_equal(dct["uint32"], 1000000000)
    nt.assert_equal(dct["int64"], -100000000000)
    nt.assert_equal(dct["uint64"], 100000000000)
    nt.assert_almost_equal(dct["float"], -123456789.0123)
    nt.assert_almost_equal(dct["double"], -123456789.0123)
    return


def test_schema_setup():
    fields = {}
    fields["int8"] = ("int8", "int8")
    fields["uint8"] = ("uint8", "uint8")
    fields["int16"] = ("int16", "int16")
    fields["uint16"] = ("uint16", "uint16")
    fields["int32"] = ("int32", "int32")
    fields["uint32"] = ("uint32", "uint32")
    fields["int64"] = ("int64", "int64")
    fields["uint64"] = ("uint64", "uint64")
    fields["float32"] = ("float32", "float32")
    fields["float64"] = ("float64", "float64")
    fields["string"] = ("string", "string")
    return fields


def test_schema_verify(fields):
    assert fields["int8"] == ("int8", "int8")
    assert fields["uint8"] == ("uint8", "uint8")
    assert fields["int16"] == ("int16", "int16")
    assert fields["uint16"] == ("uint16", "uint16")
    assert fields["int32"] == ("int32", "int32")
    assert fields["uint32"] == ("uint32", "uint32")
    assert fields["int64"] == ("int64", "int64")
    assert fields["uint64"] == ("uint64", "uint64")
    assert fields["float32"] == ("float32", "float32")
    assert fields["float64"] == ("float64", "float64")
    assert fields["string"] == ("string", "string")
    return


def test_intervals_setup():
    ilist = []
    nint = 10
    gap = 1.0
    span = 123.4
    gap_samp = 5L
    span_samp = 617L
    for i in range(nint):
        start = gap + float(i) * ( span + gap )
        stop = float(i + 1) * ( span + gap )
        first = gap_samp + long(i) * ( span_samp + gap_samp );
        last = long(i + 1) * ( span_samp + gap_samp );
        ilist.append(Intrvl(start, stop, first, last))
    return ilist


def test_intervals_verify(ilist):
    comp = test_intervals_setup()
    for i in range(len(comp)):
        nt.assert_almost_equal(ilist[i].start, comp[i].start)
        nt.assert_almost_equal(ilist[i].stop, comp[i].stop)
        assert ilist[i].first == comp[i].first
        assert ilist[i].last == comp[i].last


def test_group_setup(grp):
    return


def test_group_verify(grp):
    return


def test_block_setup(blk):
    return


def test_block_verify(blk):
    return


def test_volume_setup(vol):

    return


def test_volume_verify(vol):
    return



def test(tmpdir=None):
    dirpath = ""
    if tmpdir is None:
        dirpath = tempfile.mkdtemp()
    else:
        if os.path.isdir(tmpdir):
            dirpath = os.path.abspath(tmpdir)
        else:
            raise RuntimeError("test output path \"{}\" is not a directory".format(tmpdir))

    print("Testing dictionary conversion...")

    pd = test_dict_setup()
    
    cd = dict_py2c(pd)
    check_pd = dict_c2py(cd)
    lib.ctidas_dict_free(cd)

    test_dict_verify(check_pd)

    print("   PASS")

    print("Testing schema conversion...")

    ps = test_schema_setup()

    cs = schema_py2c(ps)
    check_ps = schema_c2py(cs)
    lib.ctidas_schema_free(cs)

    test_schema_verify(check_ps)
    
    print("   PASS")

    print("Testing interval list conversion...")

    pt = test_intervals_setup()

    ct = intrvl_list_py2c(pt)
    check_pt = intrvl_list_c2py(ct, len(pt))
    lib.ctidas_intrvl_list_free(ct, len(pt))

    test_intervals_verify(check_pt)
    
    print("   PASS")

    print("Testing volume operations...")

    volpath = os.path.join(dirpath, "tidas_py_volume")

    with Volume(volpath) as vol:
        test_volume_setup(vol)
        test_volume_verify(vol)


    return

