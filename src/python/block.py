##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
from __future__ import absolute_import, division, print_function

import sys

import ctypes as ct

from .ctidas import *
from .group import *
from .intervals import *


class Block(object):
    """
    Class which represents a TIDAS block.
    """
    def __init__(self, handle, managed=True):
        self.cp = handle
        self.managed = managed

    def close(self):
        if self.cp is not None:
            if self.managed:
                lib.ctidas_block_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def _handle(self):
        return self.cp

    def clear_groups(self):
        if self.cp is not None:
            lib.ctidas_block_clear_groups(self.cp)
        return

    def clear_intervals(self):
        if self.cp is not None:
            lib.ctidas_block_clear_intervals(self.cp)
        return

    def clear_blocks(self):
        if self.cp is not None:
            lib.ctidas_block_clear_children(self.cp)
        return

    def clear(self):
        #sys.stderr.write("block clear {}\n".format(self.cp))
        if self.cp is not None:
            lib.ctidas_block_clear(self.cp)
        return

    def range(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        start = ct.c_double(0)
        stop = ct.c_double(0)
        lib.ctidas_block_range(self.cp, ct.byref(start), ct.byref(stop))
        return (start.value, stop.value)

    def aux_dir(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        tmpstr = lib.ctidas_block_aux_dir(self.cp)
        retstr = str(tmpstr.value)
        libc.free(tmpstr)
        return retstr

    def group_names(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_groups(self.cp)
        return names

    def groups(self, names):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cp = lib.ctidas_block_group_get(self.cp, ct.c_char_p(cn))
            ret[n] = Group(handle=cp)
        return ret

    def group_get(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cp = lib.ctidas_block_group_get(self.cp, ct.c_char_p(cname))
        #sys.stderr.write("group get c handle = {}\n".format(cp))
        ret = Group(handle=cp)
        return ret

    def group_add(self, name, grp):
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
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_group_del(self.cp, ct.c_char_p(cname))
        return

    def intervals_names(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_intervals(self.cp)
        return names

    def intervals(self, names):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cp = lib.ctidas_block_intervals_get(self.cp, ct.c_char_p(cn))
            ret[n] = Intervals(handle=cp)
        return ret

    def intervals_get(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cp = lib.ctidas_block_intervals_get(self.cp, ct.c_char_p(cname))
        return Intervals(handle=cp)

    def intervals_add(self, name, intr):
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
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_intervals_del(self.cp, ct.c_char_p(cname))
        return

    def block_names(self, filter=''):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_children(self.cp)
        return names

    def blocks(self, names):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        ret = {}
        for n in names:
            cn = n.encode("utf-8")
            cb = lib.ctidas_block_child_get(self.cp, ct.c_char_p(cn))
            ret[n] = Block(cb)
        return ret

    def block_get(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        cb = lib.ctidas_block_child_get(self.cp, ct.c_char_p(cname))
        return Block(handle=cb)

    def block_add(self, name, blk):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cblk = blk._handle()
        if blk._handle() is None:
            cblk = lib.ctidas_block_alloc()
        cname = name.encode("utf-8")
        nb = lib.ctidas_block_child_add(self.cp, ct.c_char_p(cname), cblk)
        if blk._handle() is None:
            lib.ctidas_block_free(cblk)
        return Block(handle=nb)

    def block_del(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cname = name.encode("utf-8")
        lib.ctidas_block_child_del(self.cp, ct.c_char_p(cname))
        return

    def select(self, filter=''):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cfilter = filter.encode("utf-8")
        cblk = lib.ctidas_block_select(self.cp, ct.c_char_p(cfilter))
        return Block(handle=cblk)

    def info(self, name="", recurse=True, indent=2):
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
