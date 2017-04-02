##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
"""
TImestream DAta Storage (TIDAS) is a software package for reading,
writing, selecting, and facilitating operations on collections of 
timestreams.
"""

from __future__ import print_function

import ctypes as ct

from .ctidas import (
    Intrvl
)

from .intervals import (
    Intervals,
    intervals_samples,
    intervals_time, 
    intervals_seek,
    intervals_seek_ceil,
    intervals_seek_floor
)

from .group import (
    Group
)

from .block import (
    Block
)

from .volume import (
    Volume
)

from .test import (
    test
)

