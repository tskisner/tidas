#!/usr/bin/env python3
##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function

from mpi4py import MPI

import os
import sys
import argparse
import shutil

import numpy as np
import numpy.testing as nt

import calendar

from tidas import (DataType, BackendType, CompressionType, AccessMode,
    Dictionary, Intrvl, Intervals, Field, Schema, Group, Block, Volume)

from tidas.mpi import mpi_dist_uniform, MPIVolume


def dict_setup():
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


def dict_verify(dct):
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


def schema_setup(ndet):
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
    for d in range(ndet):
        dname = "det_{}".format(d)
        fields.append( Field(dname, DataType.float64, "volts") )
    ret = Schema(fields)
    return ret


def test_schema_verify(fields):
    for fl in fields:
        if fl.name == "int8":
            assert(fl.type == DataType.int8)
            assert(fl.name == fl.units)
        elif fl.name == "uint8":
            assert(fl.type == DataType.uint8)
            assert(fl.name == fl.units)
        elif fl.name == "int16":
            assert(fl.type == DataType.int16)
            assert(fl.name == fl.units)
        elif fl.name == "uint16":
            assert(fl.type == DataType.uint16)
            assert(fl.name == fl.units)
        elif fl.name == "int32":
            assert(fl.type == DataType.int32)
            assert(fl.name == fl.units)
        elif fl.name == "uint32":
            assert(fl.type == DataType.uint32)
            assert(fl.name == fl.units)
        elif fl.name == "int64":
            assert(fl.type == DataType.int64)
            assert(fl.name == fl.units)
        elif fl.name == "uint64":
            assert(fl.type == DataType.uint64)
            assert(fl.name == fl.units)
        elif fl.name == "float32":
            assert(fl.type == DataType.float32)
            assert(fl.name == fl.units)
        elif fl.name == "float64":
            assert(fl.type == DataType.float64)
            assert(fl.name == fl.units)
        elif fl.name == "string":
            assert(fl.type == DataType.string)
            assert(fl.name == fl.units)
        else:
            # Must be a det
            assert(fl.type == DataType.float64)
            assert(fl.units == "volts")
    return


def intervals_setup(nint):
    ilist = []
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


def intervals_verify(ilist):
    comp = test_intervals_setup(len(ilist))
    for i in range(len(comp)):
        nt.assert_almost_equal(ilist[i].start, comp[i].start)
        nt.assert_almost_equal(ilist[i].stop, comp[i].stop)
        assert ilist[i].first == comp[i].first
        assert ilist[i].last == comp[i].last


def group_setup(grp, ndet, nsamp):

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

    for d in range(ndet):
        dname = "det_{}".format(d)
        np.random.seed(d)
        data = np.random.normal(loc=0.0, scale=10.0, size=nsamp)
        grp.write(dname, 0, data)

    return


def group_verify(grp, ndet, nsamp):

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

    for d in range(ndet):
        dname = "det_{}".format(d)
        numpy.random.seed(d)
        data = np.random.normal(loc=0.0, scale=10.0, size=nsamp)
        check = grp.read(dname, 0, nsamp)
        nt.assert_almost_equal(data, check)

    return



def block_create(blk, gnames, inames, ndet, nsamp, nint):
    blk.clear()

    dct = dict_setup()

    schm = schema_setup(ndet)
    grp = Group(schm, dct, nsamp)

    intr = Intervals(dct, nint)

    for gn in gnames:
        ref = blk.group_add(gn, grp)

    for intn in inames:
        ref = blk.intervals_add(intn, intr)

    return


def block_write(blk, gnames, inames, ndet, nsamp, nint):

    ilist = intervals_setup(nint)

    for gn in gnames:
        ref = blk.group_get(gn)
        group_setup(ref, ndet, nsamp)

    for intn in inames:
        ref = blk.intervals_get(intn)
        ref.write(ilist)

    return


def block_verify(blk, gnames, inames, ndet, nsamp, nint):

    for gn in gnames:
        ref = blk.group_get(gn)
        group_verify(ref, ndet, nsamp)

    for intn in inames:
        ref = blk.intervals_get(intn)
        ilist = ref.read()
        intervals_verify(ilist)

    return


def timing(msg=""):
    if "last" not in timing.__dict__:
        timing.last = MPI.Wtime()
    else:
        stop = MPI.Wtime()
        diff = stop - timing.last
        print("{}: {} seconds".format(msg, diff))
        timing.last = stop
    return


def main():

    comm = MPI.COMM_WORLD

    if comm.rank == 0:
        print("Running with {} processes".format(comm.size))

    timing()

    global_start = MPI.Wtime()
    start = global_start

    parser = argparse.ArgumentParser(
        description="TIDAS I/O Timing Tests")

    parser.add_argument("--path", required=False, type=str, default="tidas_demo",
                        help="Path to volume")

    parser.add_argument("--ndet", required=False, type=int, default=1,
                        help="Number of detectors")

    parser.add_argument("--rate", required=False, type=float, default=0.01,
                        help="Sample rate in Hz")

    parser.add_argument("--startyear", required=False, default=2018, type=int,
                        help="Starting year")

    parser.add_argument("--years", required=False, default=1, type=int,
                        help="Number of years of data")

    parser.add_argument("--intervals", required=False, default=100, type=int,
                        help="Number of intervals in one day")

    args = parser.parse_args()

    # Group names to create

    gnames = [
        "readout_0",
        "readout_1",
        "readout_2"
    ]

    # Intervals to create

    inames = [
        "subscans",
        "otherevents"
    ]

    # Compute the total number of observations.

    day_to_year = {}
    day_to_month = {}
    day_to_mday = {}

    totaldays = 0
    for yr in range(args.startyear, args.startyear + args.years):
        for mn in range(12):
            weekday, nday = calendar.monthrange(yr, mn+1)
            for dy in range(nday):
                cur = totaldays + dy
                day_to_mday[cur] = dy + 1
                day_to_month[cur] = mn
                day_to_year[cur] = yr
            totaldays += nday

    daysamples = int(24.0 * 3600.0 * args.rate)

    # Distribute days among processes

    firstday, ndays = mpi_dist_uniform(comm, totaldays)

    # Create the volume.

    if comm.rank == 0:
        if os.path.isdir(args.path):
            shutil.rmtree(args.path)

    comm.barrier()
    if comm.rank == 0:
        timing("Reading options and wiping old data")

    vol = MPIVolume(comm, args.path, BackendType.hdf5, CompressionType.gzip,
        dict())
    root = vol.root()

    if comm.rank == 0:
        # Rank zero process creates the empty blocks for all
        # years and months.
        for yr in range(args.startyear, args.startyear + args.years):
            yrblock = root.block_add("{:04d}".format(yr), Block())
            #print("Add block {}".format("/{:04d}".format(yr)))
            sys.stdout.flush()
            for mn in range(12):
                mnstr = calendar.month_abbr[mn+1]
                mnblock = yrblock.block_add(mnstr, Block())
                #print("Add block {}".format("/{:04d}/{}".format(yr, mnstr)))

    # sync this meta data to all processes
    vol.meta_sync()

    # now every process creates its days
    for dy in range(firstday, firstday + ndays):
        yr = day_to_year[dy]
        mn = day_to_month[dy]
        mday = day_to_mday[dy]
        sys.stdout.flush()
        #print("Getting year block {:04d}".format(yr))
        sys.stdout.flush()
        yrblk = root.block_get("{:04d}".format(yr))
        sys.stdout.flush()
        #print("Getting month block {}".format(calendar.month_abbr[mn+1]))
        sys.stdout.flush()
        mblk = yrblk.block_get("{}".format(calendar.month_abbr[mn+1]))
        dyblk = mblk.block_add("{:02d}".format(mday), Block())
        sys.stdout.flush()
        #print("Add block /{:04d}/{}/{:02d}".format(yr, calendar.month_abbr[mn+1], mday))
        sys.stdout.flush()
        block_create(dyblk, gnames, inames, args.ndet, daysamples, args.intervals)

    # sync this meta data to all processes
    vol.meta_sync()

    #root.info("/")

    comm.barrier()
    if comm.rank == 0:
        timing("Create Volume")
    del vol

    # Re-open the volume and write the data.  We could have done this
    # all in one step, but we split it up to allow timing the creation
    # and the writing separately.

    vol = MPIVolume(comm, args.path, AccessMode.write)
    root = vol.root()

    for dy in range(firstday, firstday + ndays):
        yr = day_to_year[dy]
        mn = day_to_month[dy]
        mday = day_to_mday[dy]
        yrblk = root.block_get("{:04d}".format(yr))
        mname = "{}".format(calendar.month_abbr[mn+1])
        mblk = yrblk.block_get(mname)
        dyblk = mblk.block_get("{:02d}".format(mday))
        sys.stdout.flush()
        print("Writing /{:04d}/{}/{:02d}".format(yr, calendar.month_abbr[mn+1], mday))
        sys.stdout.flush()
        block_write(dyblk, gnames, inames, args.ndet, daysamples, args.intervals)

    comm.barrier()
    if comm.rank == 0:
        timing("Write Volume")



if __name__ == "__main__":
    main()
