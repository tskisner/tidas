#!/usr/bin/env python3
##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##

import os
import sys
import shutil

import numpy as np

import datetime
import calendar

import tidas


# The name of the volume

path = "demo_weather"

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
                daystartsec = (daystart - volstart).total_seconds() \
                    + volstartsec

                # Add a block for the day
                day = "{:02d}".format(dy)
                db = mb.block_add(day)

                # Now we are going to add the data groups for this day.

                wind = tidas.Group(schema=wind_schema, size=wind_daysamples)
                wind = db.group_add("wind", wind)

                thermal = tidas.Group(schema=thermal_schema, 
                    size=thermal_daysamples)
                thermal = db.group_add("thermal", thermal)

                precip = tidas.Group(schema=precip_schema, 
                    size=precip_daysamples)
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

                data = np.absolute(np.random.normal(loc=0.0, scale=5.0, 
                    size=wind_daysamples))
                wind.write("speed", 0, data)

                data = 360.0 * np.absolute(np.random.random(
                    size=wind_daysamples))
                wind.write("direction", 0, data)                

                data = np.absolute(np.random.normal(loc=25.0, scale=5.0, 
                    size=thermal_daysamples))
                thermal.write("temperature", 0, data)

                data = np.absolute(np.random.normal(loc=1013.25, scale=30.0, 
                    size=thermal_daysamples))
                thermal.write("pressure", 0, data)

                data = np.absolute(np.random.normal(loc=30.0, scale=10.0, 
                    size=thermal_daysamples))
                thermal.write("humidity", 0, data)

                data = 360.0 * np.absolute(np.random.random(
                    size=precip_daysamples))
                precip.write("rainfall", 0, data)


# Now that we have our initial dataset, there are several things we might want
# to do with it.  Pretend this volume was actually huge and located at a
# computing center far away.  We want to get just a small subset to play 
# with on our laptop.

# Let's say we only care about wind data, and in fact we are only 
# interested in the speed, not the direction.  Let's extract just the wind
# speed data for the month of June, 2019.

file = "demo_weather_small"

with tidas.Volume(path) as vol:
    vol.duplicate(file, backend="hdf5", 
        selection="/2019/Jun/.*[grp=wind[schm=speed]]")

# Take a quick peek at the small data volume:

with tidas.Volume(file) as vol:
    vol.info()

# Now consider another use case.  We have our original dataset which is
# priceless and we have changed the permissions on it so that it is read-only.
# However, we want to do some operations on that original data and produce some
# new derived data products.  We can make a volume which links to the original
# data and then add new groups to this.

file = "demo_weather_link"

with tidas.Volume(path) as vol:
    vol.link(file)

# The resulting volume has symlinks at the group / interval level to the
# original read-only files.  Now open this volume up and make some new
# new intervals that will be only in our local linked copy of the volume:

with tidas.Volume(file) as vol:
    # Get the root block of the volume
    root = vol.root()

    for yb in root.blocks():
        for mb in yb.blocks():
            for db in mb.blocks():
                # Pretend that we have 10 spans of time where the temperature
                # is "low".  Just fake uniform intervals here...
                nlow = 10
                chunk = 20
                span = chunk / thermal_rate

                data = []
                for i in range(nlow):
                    start = 2 * i * span
                    stop = (2 * i + 1) * span
                    first = 2 * i * chunk
                    last = (2 * i + 1) * chunk - 1
                    data.append(tidas.Intrvl(start=start, stop=stop,
                        first=first, last=last))

                lowtemps = tidas.Intervals(size=nlow)
                lowtemps = db.intervals_add("low", lowtemps)
                lowtemps.write(data)


