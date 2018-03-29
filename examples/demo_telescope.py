#!/usr/bin/env python3
##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##

# WARNING:  Running this script will generate a several GB of data...

# This is just a toy exercise.  We assume continuous data collection with
# no gaps.  We use very simple types of detector and housekeeping data.
# In a "real" dataset, we would be unpacking raw frame data from a data
# acquisition system and would know how many samples we have for a given
# observation.  Here we just make these numbers up.

# THIS IS JUST AN EXAMPLE.

import os
import sys
import shutil

import numpy as np

import datetime
import calendar

import tidas
from tidas import DataType as tdt

# The name of the volume

path = "demo_telescope"

# Create the schemas for the data groups that we will have for each day

# ---- Data from a weather station ----

wfields = list()
wfields.append( tidas.Field("windspeed", tdt.float32, "Meters / second") )
wfields.append( tidas.Field("windangle", tdt.float32, "Degrees") )
wfields.append( tidas.Field("temperature", tdt.float32, "Degrees Celsius") )
wfields.append( tidas.Field("pressure", tdt.float32, "Millibars") )
wfields.append( tidas.Field("humidity", tdt.float32, "Percent") )
wfields.append( tidas.Field("PWV", tdt.float32, "mm") )
weather_schema = tidas.Schema(wfields)

# sampled every 10 seconds
weather_rate = 1.0 / 10.0
weather_daysamples = int(24.0 * 3600.0 * weather_rate)

# ---- Housekeeping data ----

hfields = list()
hfields.append( tidas.Field("thermo1", tdt.float32, "Degrees Kelvin") )
hfields.append( tidas.Field("thermo2", tdt.float32, "Degrees Kelvin") )
hk_schema = tidas.Schema(hfields)

# sampled once per minute
hk_rate = 1.0 / 60.0
hk_daysamples = int(24.0 * 3600.0 * hk_rate)

# ---- Pointing data ----

pfields = list()
pfields.append( tidas.Field("az", tdt.float32, "Radians") )
pfields.append( tidas.Field("el", tdt.float32, "Radians") )
pfields.append( tidas.Field("psi", tdt.float32, "Radians") )
pointing_schema = tidas.Schema(pfields)

# sampled at 20 Hz
pointing_rate = 20.0
pointing_daysamples = int(24.0 * 3600.0 * pointing_rate)

# ---- Detector data ----

ndet = 50

dfields = list()

for d in range(ndet):
    detname = "det_{:04d}".format(d)
    dfields.append( tidas.Field(detname, tdt.int16, "ADU") )

det_schema = tidas.Schema(dfields)

# sampled at 100Hz
det_rate = 100.0
det_daysamples = int(24.0 * 3600.0 * det_rate)

day_seconds = 24 * 3600

# Remove the volume if it exists

if os.path.isdir(path):
    shutil.rmtree(path)

# Create the volume all at once.  To keep the size of the volume
# reasonable for this demo, only write 3 days of data.

vol = tidas.Volume(path, tidas.BackendType.hdf5,
    tidas.CompressionType.none, dict())

# Get the root block of the volume
root = vol.root()

volstart = datetime.datetime(2018, 1, 1)
volstartsec = volstart.timestamp()

for year in ["2018",]:
    # Add a block for this year
    yb = root.block_add(year, tidas.Block())

    for monthnum in range(1, 2):
        # Add a block for the month
        month = calendar.month_abbr[monthnum]
        mb = yb.block_add(month, tidas.Block())

        weekday, nday = calendar.monthrange(int(year), monthnum)
        for dy in range(1, 4):

            daystart = datetime.datetime(int(year), monthnum, dy)
            daystartsec = (daystart - volstart).total_seconds() \
                + volstartsec

            # Add a block for the day
            day = "{:02d}".format(dy)
            db = mb.block_add(day, tidas.Block())

            # Just fake some seed for now
            seed = int(year) * 1000000 + monthnum * 10000 + dy * 100
            np.random.seed(seed)

            # Now we are going to add the data groups for this day.

            print("{}-{}-{:02d}:".format(year, month, dy))

            print("  writing weather data")

            weather = tidas.Group(weather_schema, tidas.Dictionary(),
                weather_daysamples)
            weather = db.group_add("weather", weather)
            weather.write_times(0, np.linspace(daystartsec,
                daystartsec + day_seconds, num=weather_daysamples))

            data = np.absolute(np.random.normal(loc=0.0, scale=5.0,
                size=weather_daysamples)).astype(np.float32)
            weather.write("windspeed", 0, data)

            data = 360.0 * np.absolute(np.random.random(
                size=weather_daysamples)).astype(np.float32)
            weather.write("windangle", 0, data)

            data = np.absolute(np.random.normal(loc=25.0, scale=5.0,
                size=weather_daysamples)).astype(np.float32)
            weather.write("temperature", 0, data)

            data = np.absolute(np.random.normal(loc=1013.25, scale=30.0,
                size=weather_daysamples)).astype(np.float32)
            weather.write("pressure", 0, data)

            data = np.absolute(np.random.normal(loc=30.0, scale=10.0,
                size=weather_daysamples)).astype(np.float32)
            weather.write("humidity", 0, data)

            data = np.absolute(np.random.normal(loc=10.0, scale=5.0,
                size=weather_daysamples)).astype(np.float32)
            weather.write("PWV", 0, data)

            print("  writing housekeeping data")

            hk = tidas.Group(hk_schema, tidas.Dictionary(), hk_daysamples)
            hk = db.group_add("hk", hk)
            hk.write_times(0, np.linspace(daystartsec,
                daystartsec + day_seconds, num=hk_daysamples))

            data = np.random.normal(loc=273.0, scale=5.0,
                size=hk_daysamples).astype(np.float32)
            hk.write("thermo1", 0, data)

            data = np.random.normal(loc=77.0, scale=5.0,
                size=hk_daysamples).astype(np.float32)
            hk.write("thermo2", 0, data)

            print("  writing pointing data")

            pointing = tidas.Group(pointing_schema, tidas.Dictionary(),
                pointing_daysamples)
            pointing = db.group_add("pointing", pointing)
            pointing.write_times(0, np.linspace(daystartsec,
                daystartsec + day_seconds, num=pointing_daysamples))

            data = 2.0 * np.pi * np.random.random(
                size=pointing_daysamples).astype(np.float32)
            pointing.write("az", 0, data)

            data = 0.4 * np.pi * np.random.random(
                size=pointing_daysamples).astype(np.float32)
            pointing.write("el", 0, data)

            data = np.random.normal(loc=0.0, scale=(0.01*np.pi),
                size=pointing_daysamples).astype(np.float32)
            pointing.write("psi", 0, data)

            print("  writing detector data")

            det = tidas.Group(det_schema, tidas.Dictionary(),
                det_daysamples)
            det = db.group_add("detectors", det)
            det.write_times(0, np.linspace(daystartsec,
                daystartsec + day_seconds, num=det_daysamples))

            for d in range(ndet):
                detname = "det_{:04d}".format(d)
                data = np.random.normal(loc=32768, scale=2000,
                size=det_daysamples).astype(np.int16)
                det.write(detname, 0, data)

# Take a quick peek at organization:

vol = tidas.Volume(path, tidas.AccessMode.read)
vol.info()
