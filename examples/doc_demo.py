#!/usr/bin/env python3
##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function


import os
import sys
import shutil

import numpy as np

import datetime
import calendar

import tidas


# The name of the volume

path = "example_weather"

# Create the schemas for the data groups that we will have for each day

wind_schema = {
    "speed" : ("float32", "Meters / second"),
    "direction" : ("float32", "Degrees")
}

# sampled at 1Hz
wind_rate = 1.0
wind_daysamples = int(24.0 * 3600.0 * wind_rate)

thermal_schema = {
    "temperature" : ("float32", "Degrees Celsius"),
    "pressure" : ("float32", "Millibars"),
    "humidity" : ("float32", "Percent")
}

# sampled once per minute
thermal_rate = 1.0 / 60.0
thermal_daysamples = int(24.0 * 3600.0 * thermal_rate)

precip_schema = {
    "rainfall" : ("float32", "Centimeters")
}

# sampled every 5 minutes
precip_rate = 1.0 / (5 * 60.0)
precip_daysamples = int(24.0 * 3600.0 * precip_rate)

day_seconds = 24 * 3600

# Remove the volume if it exists

if os.path.isdir(path):
    shutil.rmtree(path)

# Create the volume all at once.

with tidas.Volume(path, backend="hdf5") as vol:

    # Get the root block of the volume
    root = vol.root()

    volstart = datetime.datetime(2018, 1, 1)
    volstartsec = volstart.timestamp()

    for year in ["2018", "2019", "2020"]:
        # Add a block for this year
        yb = root.block_add(year)

        for monthnum in range(12):
            # Add a block for the month
            month = calendar.month_abbr[monthnum + 1]
            mb = yb.block_add(month)

            weekday, nday = calendar.monthrange(year, monthnum + 1)
            for dy in range(nday):

                daystart = datetime.datetime(int(year), monthnum + 1, dy)
                daystartsec = (daystart - volstart).total_seconds() + volstartsec

                # Add a block for the day
                day = "{:02d}".format(dy)
                db = mb.block_add(day)

                # Now we are going to add the data groups for this day.

                wind = tidas.Group(schema=wind_schema, size=wind_daysamples)
                wind = db.group_add("wind", wind)

                thermal = tidas.Group(schema=thermal_schema, size=thermal_daysamples)
                thermal = db.group_add("thermal", thermal)

                precip = tidas.Group(schema=precip_schema, size=precip_daysamples)
                precip = db.group_add("precip", precip)

                # Write timestamps to all groups

                wind.write_times(np.linspace(daystartsec, 
                    daystartsec + day_seconds, num=wind_daysamples))

                thermal.write_times(np.linspace(daystartsec, 
                    daystartsec + day_seconds, num=thermal_daysamples))

                precip.write_times(np.linspace(daystartsec, 
                    daystartsec + day_seconds, num=precip_daysamples))

                # Write some random data to the fields

                seed = int(year) * 1000000 + monthnum * 10000 + dy * 100
                np.random.seed(seed)

                data = np.absolute(np.random.normal(loc=0.0, scale=5.0, size=wind_daysamples))
                wind.write("speed", 0, data)

                data = 360.0 * np.absolute(np.random.random(size=wind_daysamples))
                wind.write("direction", 0, data)                

                data = np.absolute(np.random.normal(loc=25.0, scale=5.0, size=thermal_daysamples))
                thermal.write("temperature", 0, data)

                data = np.absolute(np.random.normal(loc=1013.25, scale=30.0, size=thermal_daysamples))
                thermal.write("pressure", 0, data)

                data = np.absolute(np.random.normal(loc=30.0, scale=10.0, size=thermal_daysamples))
                thermal.write("humidity", 0, data)

                data = 360.0 * np.absolute(np.random.random(size=precip_daysamples))
                precip.write("rainfall", 0, data)




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
        first = gap_samp + i * ( span_samp + gap_samp );
        last = (i + 1) * ( span_samp + gap_samp );
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


def block_create(blk, gnames, inames, ndet, nsamp, nint):
    blk.clear()

    dct = dict_setup()

    fields = schema_setup(ndet)
    grp = tds.Group(schema=fields, size=nsamp, props=dct)

    intr = tds.Intervals(size=nint, props=dct)

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

    with MPIVolume(comm, args.path, backend="hdf5") as vol:
        root = vol.root()

        if comm.rank == 0:
            # Rank zero process creates the empty blocks for all 
            # years and months.
            for yr in range(args.startyear, args.startyear + args.years):
                yrblock = root.block_add("{:04d}".format(yr), tds.Block(None))
                #print("Add block {}".format("/{:04d}".format(yr)))
                sys.stdout.flush()
                for mn in range(12):
                    mnstr = calendar.month_abbr[mn+1]
                    mnblock = yrblock.block_add(mnstr, tds.Block(None))
                    #print("Add block {}".format("/{:04d}/{}".format(yr, mnstr)))

        # sync this meta data to all processes
        vol.meta_sync()

        #root.info("/")

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
            dyblk = mblk.block_add("{:02d}".format(mday), tds.Block(None))
            sys.stdout.flush()
            #print("Add block /{:04d}/{}/{:02d}".format(yr, calendar.month_abbr[mn+1], mday))
            sys.stdout.flush()
            block_create(dyblk, gnames, inames, args.ndet, daysamples, args.intervals)

        #root.info("/")

    comm.barrier()
    if comm.rank == 0:
        timing("Create Volume")

    # Re-open the volume and write the data.  We could have done this 
    # all in one step, but we split it up to allow timing the creation
    # and the writing separately.

    with MPIVolume(comm, args.path, mode="w") as vol:
        root = vol.root()

        for dy in range(firstday, firstday + ndays):
            yr = day_to_year[dy]
            mn = day_to_month[dy]
            mday = day_to_mday[dy]
            yrblk = root.block_get("{:04d}".format(yr))
            mblk = yrblk.block_get("{}".format(calendar.month_abbr[mn+1]))
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
