##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import shutil
import tempfile

import numpy as np
import numpy.testing as nt
#
# import ctypes as ct

from tidas import (DataType, BackendType, CompressionType, AccessMode,
    Dictionary, Intrvl, Intervals, Field, Schema, Group, Block, Volume)

# from .group import *
# from .intervals import *
# from .block import *
# from .volume import *


def test_dict_setup():
    ret = Dictionary()
    ret.put_string("string", "blahblahblah")
    ret.put_float64("float64", -123456789.0123)
    ret.put_float32("float32", -123.456)
    ret.put_int8("int8", -100)
    ret.put_uint8("uint8", 100)
    ret.put_int16("int16", -10000)
    ret.put_uint16("uint16", 10000)
    ret.put_int32("int32", -1000000000)
    ret.put_uint32("uint32", 1000000000)
    ret.put_int64("int64", -100000000000)
    ret.put_uint64("uint64", 100000000000)
    return ret


def test_dict_verify(dct):
    nt.assert_equal(dct.get_string("string"), "blahblahblah")
    nt.assert_equal(dct.get_int8("int8"), -100)
    nt.assert_equal(dct.get_uint8("uint8"), 100)
    nt.assert_equal(dct.get_int16("int16"), -10000)
    nt.assert_equal(dct.get_uint16("uint16"), 10000)
    nt.assert_equal(dct.get_int32("int32"), -1000000000)
    nt.assert_equal(dct.get_uint32("uint32"), 1000000000)
    nt.assert_equal(dct.get_int64("int64"), -100000000000)
    nt.assert_equal(dct.get_uint64("uint64"), 100000000000)
    nt.assert_almost_equal(dct.get_float32("float32"), -123.456, decimal=3)
    nt.assert_almost_equal(dct.get_float64("float64"), -123456789.0123)
    return


def test_schema_setup():
    fields = list()
    fields.append( Field("int8", DataType.int8, "int8") )
    fields.append( Field("uint8", DataType.uint8, "uint8") )
    fields.append( Field("int16", DataType.int16, "int16") )
    fields.append( Field("uint16", DataType.uint16, "uint16") )
    fields.append( Field("int32", DataType.int32, "int32") )
    fields.append( Field("uint32", DataType.uint32, "uint32") )
    fields.append( Field("int64", DataType.int64, "int64") )
    fields.append( Field("uint64", DataType.uint64, "uint64") )
    fields.append( Field("float32", DataType.float32, "float32") )
    fields.append( Field("float64", DataType.float64, "float64") )
    fields.append( Field("string", DataType.string, "string") )
    ret = Schema(fields)
    return ret


def test_schema_verify(fields):
    for fl in fields:
        assert(fl.name == fl.units)
        if fl.name == "int8":
            assert(fl.type == DataType.int8)
        elif fl.name == "uint8":
            assert(fl.type == DataType.uint8)
        elif fl.name == "int16":
            assert(fl.type == DataType.int16)
        elif fl.name == "uint16":
            assert(fl.type == DataType.uint16)
        elif fl.name == "int32":
            assert(fl.type == DataType.int32)
        elif fl.name == "uint32":
            assert(fl.type == DataType.uint32)
        elif fl.name == "int64":
            assert(fl.type == DataType.int64)
        elif fl.name == "uint64":
            assert(fl.type == DataType.uint64)
        elif fl.name == "float32":
            assert(fl.type == DataType.float32)
        elif fl.name == "float64":
            assert(fl.type == DataType.float64)
        elif fl.name == "string":
            assert(fl.type == DataType.string)
        else:
            print("error, unrecognized field \"{}\"".format(fl.name))
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
        first = gap_samp + i * ( span_samp + gap_samp );
        last = (i + 1) * ( span_samp + gap_samp );
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
    string_data = np.empty(nsamp, dtype='S64')

    for i in range(nsamp):
        fi = float(i)
        time[i] = fi * 0.001
        int8_data[i] = -(i % 128)
        uint8_data[i] = (i % 128)
        int16_data[i] = -(i % 32768)
        uint16_data[i] = (i % 32768)
        int32_data[i] = -i
        uint32_data[i] = i
        int64_data[i] = -i
        uint64_data[i] = i
        float32_data[i] = fi
        float64_data[i] = fi
        string_data[i] = 'foobarbahblat'

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
    grp.write("string", 0, string_data)

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
    string_data_check = np.zeros(nsamp, dtype='S64')

    for i in range(nsamp):
        fi = float(i)
        time_check[i] = fi * 0.001
        int8_data_check[i] = -(i % 128)
        uint8_data_check[i] = (i % 128)
        int16_data_check[i] = -(i % 32768)
        uint16_data_check[i] = (i % 32768)
        int32_data_check[i] = -i
        uint32_data_check[i] = i
        int64_data_check[i] = -i
        uint64_data_check[i] = i
        float32_data_check[i] = fi
        float64_data_check[i] = fi
        string_data_check[i] = 'foobarbahblat'

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
    string_data = grp.read("string", 0, nsamp)

    nt.assert_equal(int8_data, int8_data_check)
    nt.assert_equal(uint8_data, uint8_data_check)
    nt.assert_equal(int16_data, int16_data_check)
    nt.assert_equal(uint16_data, uint16_data_check)
    nt.assert_equal(int32_data, int32_data_check)
    nt.assert_equal(uint32_data, uint32_data_check)
    nt.assert_equal(int64_data, int64_data_check)
    nt.assert_equal(uint64_data, uint64_data_check)
    nt.assert_equal(string_data, string_data_check)

    nt.assert_almost_equal(float32_data, float32_data_check)
    nt.assert_almost_equal(float64_data, float64_data_check)
    nt.assert_almost_equal(time, time_check)

    return


def test_block_setup(blk, nsamp):
    blk.clear()

    dct = test_dict_setup()

    ilist = test_intervals_setup()
    intr = Intervals(dct, len(ilist))

    schm = test_schema_setup()
    grp = Group(schm, dct, nsamp)

    ga = blk.group_add("group_A", grp)
    test_group_setup(ga, nsamp)

    gb = blk.group_add("group_B", grp)
    test_group_setup(gb, nsamp)

    ia = blk.intervals_add("intr_A", intr)
    ia.write(ilist)

    ib = blk.intervals_add("intr_B", intr)
    ib.write(ilist)

    return


def test_block_verify(blk, nsamp):
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

    for b in names:
        bl = blk.block_get(b)
        test_block_verify(bl, nsamp)

    return


def test_volume_setup(vol, nblock, nsamp):
    rt = vol.root()
    rt.clear()
    for b in range(nblock):
        child = rt.block_add("block_{}".format(b), Block())
        test_block_setup(child, nsamp)
    return


def test_volume_verify(vol, nblock, nsamp):
    rt = vol.root()
    for b in range(nblock):
        child = rt.block_get("block_{}".format(b))
        test_block_verify(child, nsamp)
    return



def run(tmpdir=None):
    dirpath = ""
    if tmpdir is None:
        dirpath = tempfile.mkdtemp()
    else:
        dirpath = os.path.abspath(tmpdir)
        if not os.path.isdir(tmpdir):
            os.makedirs(dirpath)

    print("Testing dictionary creation...")

    pd = test_dict_setup()
    test_dict_verify(pd)

    print("   PASS")

    print("Testing schema creation...")

    ps = test_schema_setup()
    test_schema_verify(ps.fields())

    print("   PASS")

    print("Testing interval list creation...")

    pt = test_intervals_setup()
    test_intervals_verify(pt)

    print("   PASS")

    print("Testing volume operations...")

    volpath = os.path.join(dirpath, "tidas_py_volume")
    if os.path.isdir(volpath):
        shutil.rmtree(volpath)

    nblock = 3
    nsamp = 10

    vol = Volume(volpath, BackendType.hdf5, CompressionType.gzip, dict())
    test_volume_setup(vol, nblock, nsamp)

    vol = Volume(volpath, AccessMode.read)
    test_volume_verify(vol, nblock, nsamp)
    #vol.info()

    print("   PASS")

    return


def test_mpi_volume_setup(vol, nblock, nsamp):
    from tidas.mpi import mpi_dist_uniform

    rt = vol.root()
    rt.clear()

    rank = vol.comm().rank

    offset, nlocal = mpi_dist_uniform(vol.comm(), nblock)

    for b in range(offset, offset + nlocal):
        child = rt.block_add("block_{}".format(b), Block())
        test_block_setup(child, nsamp)

    vol.meta_sync()
    return



def run_mpi(tmpdir=None):
    from mpi4py import MPI
    from tidas.mpi import MPIVolume, mpi_dist_uniform

    comm = MPI.COMM_WORLD

    dirpath = ""

    if comm.rank == 0:
        if tmpdir is None:
            dirpath = tempfile.mkdtemp()
        else:
            dirpath = os.path.abspath(tmpdir)
            if not os.path.isdir(tmpdir):
                os.makedirs(dirpath)

    dirpath = comm.bcast(dirpath, root=0)

    if comm.rank == 0:
        print("Testing MPI volume operations...")

    volpath = os.path.join(dirpath, "tidas_py_mpi_volume")

    if comm.rank == 0:
        if os.path.isdir(volpath):
            shutil.rmtree(volpath)
    comm.barrier()

    nblock = 10
    nsamp = 10

    vol = MPIVolume(comm, volpath, BackendType.hdf5, CompressionType.gzip,
        dict())
    test_mpi_volume_setup(vol, nblock, nsamp)

    vol = MPIVolume(comm, volpath, AccessMode.read)
    test_volume_verify(vol, nblock, nsamp)
    #vol.info()

    if comm.rank == 0:
        print("   PASS")

    return
