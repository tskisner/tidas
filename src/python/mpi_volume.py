##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function

from mpi4py import MPI

import os
import sys

import ctypes as ct

from . import ctidas as ctd
from . import ctidas_mpi as ctdmpi

from .block import *


class MPIVolume(object):
    """
    Class which represents a TIDAS MPI volume.

    Args:
        comm (mpi4py.MPI.Comm): the communicator.
        path (string): filesystem location of the volume to open or create.
        mode (string): whether to open the file in read-only ("r") or
                       read-write ("w") mode.  Default is read-only for 
                       opening existing files and read-write for file creation.
    """

    def __init__(self, comm, path, mode="r", backend="hdf5", comp="none" ):
        self._comm = comm
        self.path = path
        self.backend = backend
        self.comp = comp
        self.cp = None

        comm_ptr = MPI._addressof(comm)
        c_comm = ctdmpi.MPI_Comm.from_address(comm_ptr)

        cpath = path.encode("utf-8")
        pcpath = ct.c_char_p(cpath)
        if os.path.isdir(self.path):
            if (mode == "w"):
                self.mode = "write"
            else:
                self.mode = "read"
            self.cp = ctdmpi.libmpi.ctidas_mpi_volume_open(c_comm, pcpath, access_mode[self.mode])
        else:
            self.mode = "write"
            self.cp = ctdmpi.libmpi.ctidas_mpi_volume_create(c_comm, pcpath, backend_type[backend], compression_type[comp])

    def close(self):
        self._comm.barrier()
        if self.cp is not None:
            ctdmpi.libmpi.ctidas_mpi_volume_close(self.cp)
        self.cp = None

    @property
    def comm(self):
        return self._comm

    @property
    def comm_rank(self):
        return self._comm.rank

    @property
    def comm_size(self):
        return self._comm.size

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        return False

    def info(self, recurse=True):
        prf = "TIDAS:  "
        indent = 0
        for i in range(indent):
            prf = "{} ".format(prf)
        print("{}Volume \"{}\" (mode={}, backend={}, comp={})".format(prf, self.path, self.mode, self.backend, self.comp))
        b = self.root()
        if recurse:
            b.info("/", recurse=True)
        else:
            b.info("/", recurse=False)
        return

    def root(self):
        croot = ctdmpi.libmpi.ctidas_mpi_volume_root(self.cp)
        #sys.stderr.write("return root instance at {}\n".format(croot))
        return Block(croot)
