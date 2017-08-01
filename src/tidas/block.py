##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import ctypes as ct

from .ctidas import *
from .group import *
from .intervals import *


class Block(object):
    """
    Class which represents a TIDAS block.
    """
    def __init__(self, handle=None, managed=True):
        self.cp = handle
        self.managed = managed

    def close(self):
        """
        Explicitly close the block (free the underlying C++ pointer).
        """
        if self.cp is not None:
            if self.managed:
                lib.ctidas_block_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def _handle(self):
        return self.cp

    def clear_groups(self):
        """
        Remove all groups from this block.
        """
        if self.cp is not None:
            lib.ctidas_block_clear_groups(self.cp)
        return

    def clear_intervals(self):
        """
        Remove all intervals from this block.
        """
        if self.cp is not None:
            lib.ctidas_block_clear_intervals(self.cp)
        return

    def clear_blocks(self):
        """
        Remove all sub-blocks from this block.
        """
        if self.cp is not None:
            lib.ctidas_block_clear_children(self.cp)
        return

    def clear(self):
        """
        Remove all groups, intervals and sub-blocks from this block.
        """
        if self.cp is not None:
            lib.ctidas_block_clear(self.cp)
        return

    def range(self):
        """
        Returns the extrema of timestamps of all groups in this block.
        
        This is not recursive (does not traverse sub-blocks).

        Returns (tuple):
            the minimum / maximum start / stop times.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        start = ct.c_double(0)
        stop = ct.c_double(0)
        lib.ctidas_block_range(self.cp, ct.byref(start), ct.byref(stop))
        return (start.value, stop.value)

    def aux_dir(self):
        """
        Returns the filesystem path of the auxilliary data directory.

        Each block has an associated directory for extra user-defined
        data.  This function returns the path to this directory for this
        block.

        Returns (str):
            path to the auxilliary data directory.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        tmpstr = lib.ctidas_block_aux_dir(self.cp)
        retstr = str(tmpstr.value)
        libc.free(tmpstr)
        return retstr

    def group_names(self):
        """
        The names of all groups in this block.

        Returns (list):
            a list of all the group names.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_groups(self.cp)
        return names

    def groups(self, names):
        """
        Return a dictionary of all the specified groups.

        Args:
            names (list): a list of strings with the group names.

        Returns (dict):
            a dictionary keyed on the names whose values are Group
            instances.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cp = lib.ctidas_block_group_get(self.cp, ct.c_char_p(cn))
            ret[n] = Group(handle=cp)
        return ret

    def group_get(self, name):
        """
        Return the specified group.

        Args:
            name (str): the name of the group.

        Returns (Group):
            the requested group.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cp = lib.ctidas_block_group_get(self.cp, ct.c_char_p(cname))
        #sys.stderr.write("group get c handle = {}\n".format(cp))
        ret = Group(handle=cp)
        return ret

    def group_add(self, name, grp):
        """
        Add the specified group to the block.

        Adds a copy of the group to this block.  If the python Group instance
        does not have a C++ handle associated with it, then a new C++ instance
        is created before adding it.

        Args:
            name (str): the name of the group.
            grp (Group): the group to add.
        
        Returns (Group):
            a copy of the newly added Group associated with the block.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cgrp = grp._handle()
        #sys.stderr.write("group add handle = {}\n".format(cgrp))
        if grp._handle() is None:
            cschm = schema_py2c(grp.schema)
            cdict = dict_py2c(grp.props)
            #sys.stderr.write("group add allocating\n")
            cgrp = lib.ctidas_group_alloc(cschm, cdict, ct.c_longlong(grp.size))
            #sys.stderr.write("group add temp handle = {}\n".format(cgrp))
            lib.ctidas_schema_free(cschm)
            lib.ctidas_dict_free(cdict)
            #sys.stderr.write("group add temp schema and dict freed\n")
        cname = name.encode("utf-8")
        ngrp = lib.ctidas_block_group_add(self.cp, ct.c_char_p(cname), cgrp)
        if grp._handle() is None:
            #sys.stderr.write("group add free temp handle = {}\n".format(cgrp))
            lib.ctidas_group_free(cgrp)
        return Group(handle=ngrp)

    def group_del(self, name):
        """
        Delete the specified group from the block.

        Args:
            name (str): the name of the group.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_group_del(self.cp, ct.c_char_p(cname))
        return

    def intervals_names(self):
        """
        The names of all intervals in this block.

        Returns (list):
            a list of all the intervals names.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_intervals(self.cp)
        return names

    def intervals(self, names):
        """
        Return a dictionary of all the specified intervals.

        Args:
            names (list): a list of strings with the intervals names.

        Returns (dict):
            a dictionary keyed on the names whose values are Intervals
            instances.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cp = lib.ctidas_block_intervals_get(self.cp, ct.c_char_p(cn))
            ret[n] = Intervals(handle=cp)
        return ret

    def intervals_get(self, name):
        """
        Return the specified intervals.

        Args:
            name (str): the name of the intervals.

        Returns (Intervals):
            the requested intervals.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cp = lib.ctidas_block_intervals_get(self.cp, ct.c_char_p(cname))
        return Intervals(handle=cp)

    def intervals_add(self, name, intr):
        """
        Add the specified intervals to the block.

        Adds a copy of the intervals to this block.  If the python Intervals
        instance does not have a C++ handle associated with it, then a new 
        C++ instance is created before adding it.

        Args:
            name (str): the name of the Intervals.
            intr (Intervals): the Intervals to add.
        
        Returns (Intervals):
            a copy of the newly added Intervals associated with the block.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cintr = intr._handle()
        if intr._handle() is None:
            cdict = dict_py2c(intr.props)
            cintr = lib.ctidas_intervals_alloc(cdict, ct.c_ulong(intr.size))
            lib.ctidas_dict_free(cdict)
        cname = name.encode("utf-8")
        nintr = lib.ctidas_block_intervals_add(self.cp, ct.c_char_p(cname), cintr)
        if intr._handle() is None:
            lib.ctidas_intervals_free(cintr)
        return Intervals(handle=nintr)

    def intervals_del(self, name):
        """
        Delete the specified intervals from the block.

        Args:
            name (str): the name of the intervals.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_intervals_del(self.cp, ct.c_char_p(cname))
        return

    def block_names(self, filter=''):
        """
        The names of all sub-blocks in this block.

        Returns (list):
            a list of all the block names.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_children(self.cp)
        return names

    def blocks(self, names):
        """
        Return a dictionary of all the specified blocks.

        Args:
            names (list): a list of strings with the block names.

        Returns (dict):
            a dictionary keyed on the names whose values are Block
            instances.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cb = lib.ctidas_block_child_get(self.cp, ct.c_char_p(cn))
            ret[n] = Block(cb)
        return ret

    def block_get(self, name):
        """
        Return the specified block.

        Args:
            name (str): the name of the block.

        Returns (Block):
            the requested block.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cb = lib.ctidas_block_child_get(self.cp, ct.c_char_p(cname))
        return Block(handle=cb)

    def block_add(self, name, blk=None):
        """
        Add the specified child block to this block.

        If the python Block instance does not have a C++ handle 
        associated with it, then a new C++ instance is created before 
        adding it.

        Args:
            name (str): the name of the block.
            blk (Block): the block to add.
        
        Returns (Block):
            a copy of the newly added child block.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        if blk is None:
            blk = Block()
        cblk = blk._handle()
        if blk._handle() is None:
            cblk = lib.ctidas_block_alloc()
        cname = name.encode("utf-8")
        nb = lib.ctidas_block_child_add(self.cp, ct.c_char_p(cname), cblk)
        if blk._handle() is None:
            lib.ctidas_block_free(cblk)
        return Block(handle=nb)

    def block_del(self, name):
        """
        Delete the specified sub-block from the block.

        Args:
            name (str): the name of the sub-block.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_child_del(self.cp, ct.c_char_p(cname))
        return

    def select(self, filter=''):
        """
        Recursively copy the current block and all descendents,
        applying the specified matching pattern.

        Args:
            filter (str): the selection string to apply.

        Returns (Block):
            a Block copy with a pruned tree of objects.
        """
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cfilter = filter.encode("utf-8")
        cblk = lib.ctidas_block_select(self.cp, ct.c_char_p(cfilter))
        return Block(handle=cblk)

    def info(self, name="", recurse=True, indent=2):
        """
        Print basic information about this block to stdout.

        Mainly useful for debugging.

        Args:
            name (str): the name of this group in the parent block.
        """
        prf = "TIDAS:  "
        if name == "":
            indent = 0
        for i in range(indent):
            prf = "{} ".format(prf)
        if name == "":
            print("{}Block".format(prf))
        else:
            print("{}Block \"{}\"".format(prf, name))
        names = self.group_names()
        grps = self.groups(names)
        for nm in sorted(grps):
            grps[nm].info(nm, indent=(indent+2))
        names = self.block_names()
        blks = self.blocks(names)
        if recurse:    
            for nm in sorted(blks):
                blks[nm].info(name=nm, recurse=recurse, indent=(indent+2))
        else:
            for nm in sorted(blks):
                print("{}  Block \"{}\"".format(prf, nm))
        return
