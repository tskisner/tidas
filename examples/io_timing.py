#!/usr/bin/env python3
##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function

#from mpi4py import MPI

import os
import sys
import argparse

import numpy as np
import numpy.testing as nt

import tidas as tds
# from tidas.ctidas_mpi import dist_uniform
# from tidas.mpi_volume import MPIVolume


def dict_setup():
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


def dict_verify(dct):
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


def schema_setup(ndet):
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
    for d in range(ndet):
        dname = "det_{}".format(d)
        fields[dname] = ("float64", "volts")
    return fields


def schema_verify(fields, ndet):
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
    for d in range(ndet):
        dname = "det_{}".format(d)
        assert fields[dname] == ("float64", "volts")
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
        first = gap_samp + long(i) * ( span_samp + gap_samp );
        last = long(i + 1) * ( span_samp + gap_samp );
        ilist.append(tds.Intrvl(start, stop, first, last))
    return ilist


def intervals_verify(ilist):
    comp = test_intervals_setup()
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

    for d in range(ndet):
        dname = "det_{}".format(d)
        numpy.random.seed(d)
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

    for d in range(ndet):
        dname = "det_{}".format(d)
        numpy.random.seed(d)
        data = np.random.normal(loc=0.0, scale=10.0, size=nsamp)
        check = grp.read(dname, 0, nsamp)
        nt.assert_almost_equal(data, check)

    return


def block_setup(blk, gnames, inames, ndet, nsamp, nint):
    blk.clear()

    dct = dict_setup()

    fields = schema_setup(ndet)
    grp = tds.Group(schema=fields, size=nsamp, props=dct)

    ilist = intervals_setup(nint)
    intr = tds.Intervals(size=nint, props=dct)

    for gn in gnames:
        blk.group_add(gn, grp)
        ref = blk.group_get(gn)
        group_verify(ref, ndet, nsamp)

    for intn in inames:
        blk.intervals_add(intn, intr)
        ref = blk.intervals_get(intn)
        ref.write(ilist)

    return


def block_verify(blk, gnames, inames, ndet, nsamp, nint):

    for gn in gnames:
        ref = blk.group_get(gn)
        group_setup(ref, ndet, nsamp)

    for intn in inames:
        ref = blk.intervals_get(intn)
        ilist = ref.read()
        intervals_verify(ilist)

    return


def main():

    # comm = MPI.COMM_WORLD

    # if comm.rank == 0:
    #     print("Running with {} processes at {}".format(
    #         comm.size, str(datetime.now())), flush=True)

    # global_start = MPI.Wtime()

    parser = argparse.ArgumentParser(
        description="TIDAS I/O Timing Tests")

    parser.add_argument("--path", required=False, type=str, default="tidas_demo",
                        help="Path to volume")

    parser.add_argument("--ndet", required=False, type=int, default=100,
                        help="Number of detectors")

    parser.add_argument("--nsamp", required=False, default=1000, type=int,
                        help="Number of samples")

    parser.add_argument("--ninterval", required=False, default=100, type=int,
                        help="Number of intervals")

    parser.add_argument("--nobs", required=False, default=10, type=int,
                        help="Number of observations")

    args = parser.parse_args()

    # Distribute obs among processes

    #firstobs, localobs = dist_uniform(comm, args.nobs)
    firstobs = 0
    localobs = args.nobs

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

    # Create and write the volume

    with tds.Volume(args.path, backend="hdf5", comp="gzip") as vol:
        root = vol.root()

        # each proc creates their observations
        
        for ob in range(firstobs, firstobs + localobs):
            obname = "observation_{:03d}".format(ob)
            root.block_add(obname)
            blk = root.block_get(obname)
            block_setup(blk, gnames, inames, args.ndet, args.nsamp, args.ninterval)


if __name__ == "__main__":
    main()
