##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function, unicode_literals

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
        backend (str): the backend format to use.
        comp (str): the compression type to use.
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
        """
        Explicitly close the volume.
        """
        self._comm.barrier()
        if self.cp is not None:
            ctdmpi.libmpi.ctidas_mpi_volume_close(self.cp)
        self.cp = None

    @property
    def comm(self):
        """
        The mpi4py communicator.
        """
        return self._comm

    @property
    def comm_rank(self):
        """
        For convenience, the rank within the communicator.
        """
        return self._comm.rank

    @property
    def comm_size(self):
        """
        For convenience, the size of the communicator.
        """
        return self._comm.size

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        return False

    def duplicate(self, path, backend="hdf5", comp="none", selection=""):
        """
        Duplicate the volume to a new location.

        Args:
            path (str): filesystem location of the volume copy.
            backend (str): the backend format to use.
            comp (str): the compression type to use.
            selection (str): a selection filter to apply to the tree of
                blocks.
        """
        cpath = path.encode("utf-8")
        csel = selection.encode("utf-8")
        pcpath = ct.c_char_p(cpath)
        pcsel = ct.c_char_p(csel)
        if self.cp is None:
            raise RuntimeError("Volume is not open")
        ctdmpi.libmpi.ctidas_mpi_volume_duplicate(self.cp, pcpath, 
            backend_type[backend], compression_type[comp], pcsel)
        return

    def link(self, path, ltype="soft", selection=""):
        """
        Create a linked copy of the volume.

        This will create a new volume where the properties file and all
        selected groups and intervals are links to the original.  If the
        original files are writeable, then these links will be as well.

        The metadata index is copied (not linked), so that new objects may
        be added locally.

        Args:
            path (str): filesystem location of the volume copy.
            ltype (str): the link type to use ("soft" or "hard").
            selection (str): a selection filter to apply to the tree of
                blocks.
        """
        cpath = path.encode("utf-8")
        csel = selection.encode("utf-8")
        pcpath = ct.c_char_p(cpath)
        pcsel = ct.c_char_p(csel)
        if self.cp is None:
            raise RuntimeError("Volume is not open")
        ctdmpi.libmpi.ctidas_mpi_volume_link(self.cp, pcpath, 
            link_type[ltype], pcsel)
        return

    def meta_sync(self):
        """
        Synchronize metadata across processes.

        All metadata transactions in memory across all processes are
        gathered to the root process and replayed in rank order.
        Afterwards, the full updated metadata is broadcast to all
        processes. 
        """
        ctdmpi.libmpi.ctidas_mpi_volume_meta_sync(self.cp)
        return

    def info(self, recurse=True):
        """
        Print basic information about the root block to stdout.

        Mainly useful for debugging.

        Args:
            recurse (bool): if True, call recursively on child blocks.
        """
        prf = "TIDAS:  "
        indent = 0
        for i in range(indent):
            prf = "{} ".format(prf)
        b = self.root()
        if recurse:
            b.info("/", recurse=True)
        else:
            b.info("/", recurse=False)
        return

    def root(self):
        """
        Return a reference to the root block.
    
        A new python Block is returned containing a pointer to the root
        block of the volume.  However, this python class does not manage
        the lifetime of the C++ pointer.  As long as the volume is open,
        the pointer held by these root block instances will also be valid.
        """
        croot = ctdmpi.libmpi.ctidas_mpi_volume_root(self.cp)
        #sys.stderr.write("return root instance at {}\n".format(croot))
        return Block(croot, managed=False)
