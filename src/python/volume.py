##
##  TImestream DAta Storage (TIDAS)
##  (c) 2014-2015, The Regents of the University of California, 
##  through Lawrence Berkeley National Laboratory.  See top
##  level LICENSE file for details.
##

import os
import sys

import ctypes as ct

from .ctidas import *

from .block import *


class Volume(object):
    """
    Class which represents a TIDAS volume.

    Args:
        path (string): filesystem location of the volume to open or create.
        mode (string): whether to open the file in read-only ("r") or
                       read-write ("w") mode.  Default is read-only for 
                       opening existing files and read-write for file creation.
    """

    def __init__(self, path, mode="r", backend="hdf5", comp="none" ):
        self.path = path
        self.cp = ct.POINTER(cVolume)
        if os.path.isdir(self.path):
            if (mode == "w"):
                self.mode = "write"
            else:
                self.mode = "read"
            self.cp = lib.ctidas_volume_open(path, access_mode[self.mode])
        else:
            self.mode = "write"
            self.cp = lib.ctidas_volume_create(path, backend_type[backend], compression_type[comp])

    def close(self):
        if self.cp is not None:
            lib.ctidas_volume_close(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        return False

    def root(self):
        croot = lib.ctidas_volume_root(self.cp)
        #sys.stderr.write("return root instance at {}\n".format(croot))
        return Block(croot)
