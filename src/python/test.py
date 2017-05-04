##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function

import os
import sys
import tempfile
import numpy.testing as nt

import ctypes as ct

from .ctidas import *
from .group import *
from .intervals import *
from .block import *
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
    ret["int64"] = -100000000000
    ret["uint64"] = 100000000000
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
    gap_samp = 5
    span_samp = 617
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


def test_group_setup(grp, nsamp):

    time = np.zeros(nsamp, dtype=np.float64)
    int8_data = np.zeros(nsamp, dtype=np.int8)
    uint8_data = np.zeros(nsamp, dtype=np.uint8)
    int16_data = np.zeros(nsamp, dtype=np.int16)
    uint16_data = np.zeros(nsamp, dtype=np.uint16)
    int32_data = np.zeros(nsamp, dtype=np.int32)
    uint32_data = np.zeros(nsamp, dtype=np.uint32)
    int64_data = np.zeros(nsamp, dtype=np.int64)
    uint64_data = np.zeros(nsamp, dtype=np.uint64)
    float32_data = np.zeros(nsamp, dtype=np.float32)
    float64_data = np.zeros(nsamp, dtype=np.float64)

    for i in range(nsamp):
        fi = float(i)
        time[i] = fi * 0.001
        int8_data[i] = -(i % 128)
        uint8_data[i] = (i % 128)
        int16_data[i] = -(i % 32678)
        uint16_data[i] = (i % 32678)
        int32_data[i] = -i
        uint32_data[i] = i
        int64_data[i] = -i
        uint64_data[i] = i
        float32_data[i] = fi
        float64_data[i] = fi

    grp.write_times(time)
    grp.write("int8", 0, int8_data)
    grp.write("uint8", 0, uint8_data)
    grp.write("int16", 0, int16_data)
    grp.write("uint16", 0, uint16_data)
    grp.write("int32", 0, int32_data)
    grp.write("uint32", 0, uint32_data)
    grp.write("int64", 0, int64_data)
    grp.write("uint64", 0, uint64_data)
    grp.write("float32", 0, float32_data)
    grp.write("float64", 0, float64_data)

    return


def test_group_verify(grp, nsamp):

    time_check = np.zeros(nsamp, dtype=np.float64)
    int8_data_check = np.zeros(nsamp, dtype=np.int8)
    uint8_data_check = np.zeros(nsamp, dtype=np.uint8)
    int16_data_check = np.zeros(nsamp, dtype=np.int16)
    uint16_data_check = np.zeros(nsamp, dtype=np.uint16)
    int32_data_check = np.zeros(nsamp, dtype=np.int32)
    uint32_data_check = np.zeros(nsamp, dtype=np.uint32)
    int64_data_check = np.zeros(nsamp, dtype=np.int64)
    uint64_data_check = np.zeros(nsamp, dtype=np.uint64)
    float32_data_check = np.zeros(nsamp, dtype=np.float32)
    float64_data_check = np.zeros(nsamp, dtype=np.float64)

    for i in range(nsamp):
        fi = float(i)
        time_check[i] = fi * 0.001
        int8_data_check[i] = -(i % 128)
        uint8_data_check[i] = (i % 128)
        int16_data_check[i] = -(i % 32678)
        uint16_data_check[i] = (i % 32678)
        int32_data_check[i] = -i
        uint32_data_check[i] = i
        int64_data_check[i] = -i
        uint64_data_check[i] = i
        float32_data_check[i] = fi
        float64_data_check[i] = fi

    time = grp.read_times()
    int8_data = grp.read("int8", 0, nsamp)
    uint8_data = grp.read("uint8", 0, nsamp)
    int16_data = grp.read("int16", 0, nsamp)
    uint16_data = grp.read("uint16", 0, nsamp)
    int32_data = grp.read("int32", 0, nsamp)
    uint32_data = grp.read("uint32", 0, nsamp)
    int64_data = grp.read("int64", 0, nsamp)
    uint64_data = grp.read("uint64", 0, nsamp)
    float32_data = grp.read("float32", 0, nsamp)
    float64_data = grp.read("float64", 0, nsamp)

    nt.assert_equal(int8_data, int8_data_check)
    nt.assert_equal(uint8_data, uint8_data_check)
    nt.assert_equal(int16_data, int16_data_check)
    nt.assert_equal(uint16_data, uint16_data_check)
    nt.assert_equal(int32_data, int32_data_check)
    nt.assert_equal(uint32_data, uint32_data_check)
    nt.assert_equal(int64_data, int64_data_check)
    nt.assert_equal(uint64_data, uint64_data_check)

    nt.assert_almost_equal(float32_data, float32_data_check)
    nt.assert_almost_equal(float64_data, float64_data_check)
    nt.assert_almost_equal(time, time_check)

    return


def test_block_setup(blk, nsamp):
    #sys.stderr.write("block setup {}\n".format(blk._handle()))

    blk.clear()

    #sys.stderr.write("block setup {} dict\n".format(blk._handle()))

    dct = test_dict_setup()

    #sys.stderr.write("block setup {} ilist\n".format(blk._handle()))

    ilist = test_intervals_setup()

    #sys.stderr.write("block setup {} intervals\n".format(blk._handle()))

    intr = Intervals(size=len(ilist), props=dct)

    #sys.stderr.write("block setup {} group\n".format(blk._handle()))

    fields = test_schema_setup()
    grp = Group(schema=fields, size=nsamp, props=dct)

    #sys.stderr.write("block setup {} add group A\n".format(blk._handle()))

    blk.group_add("group_A", grp)
    ga = blk.group_get("group_A")
    #sys.stderr.write("block setup {} got group A {}\n".format(blk._handle(), ga._handle()))

    test_group_setup(ga, nsamp)

    #sys.stderr.write("block setup {} add group B\n".format(blk._handle()))

    blk.group_add("group_B", grp)
    gb = blk.group_get("group_B")
    test_group_setup(gb, nsamp)

    #sys.stderr.write("block setup {} add intr A\n".format(blk._handle()))

    blk.intervals_add("intr_A", intr)
    ia = blk.intervals_get("intr_A")
    ia.write(ilist)

    #sys.stderr.write("block setup {} add intr B\n".format(blk._handle()))

    blk.intervals_add("intr_B", intr)
    ib = blk.intervals_get("intr_B")
    ib.write(ilist)

    return


def test_block_verify(blk, nsamp):
    #sys.stderr.write("block verify {}\n".format(blk._handle()))

    grp = blk.group_get("group_A")
    test_group_verify(grp, nsamp)

    grp = blk.group_get("group_B")
    test_group_verify(grp, nsamp)

    intr = blk.intervals_get("intr_A")
    ilist = intr.read()
    test_intervals_verify(ilist)

    intr = blk.intervals_get("intr_B")
    ilist = intr.read()
    test_intervals_verify(ilist)

    names = blk.block_names()
    all = blk.blocks(names)

    for b in all:
        test_block_verify(b, nsamp)

    return


def test_volume_setup(vol, nblock, nsamp):
    rt = vol.root()
    #sys.stderr.write("volume setup root {}\n".format(rt._handle()))
    rt.clear()
    for b in range(nblock):
        rt.block_add("block_{}".format(b))
        child = rt.block_get("block_{}".format(b))
        #sys.stderr.write("volume setup root/child {}\n".format(child._handle()))
        test_block_setup(child, nsamp)
    return


def test_volume_verify(vol, nblock, nsamp):
    rt = vol.root()
    for b in range(nblock):
        child = rt.block_get("block_{}".format(b))
        test_block_verify(child, nsamp)
    return



def test(tmpdir=None, recurse=False):
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

    nblock = 3
    nsamp = 10

    with Volume(volpath, backend="hdf5", comp="gzip") as vol:
        test_volume_setup(vol, nblock, nsamp)

    with Volume(volpath, mode="r") as vol:
        test_volume_verify(vol, nblock, nsamp)
        vol.info(recurse=recurse)

    print("   PASS")

    return

