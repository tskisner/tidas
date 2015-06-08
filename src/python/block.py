##
##  TImestream DAta Storage (TIDAS)
##  (c) 2014-2015, The Regents of the University of California, 
##  through Lawrence Berkeley National Laboratory.  See top
##  level LICENSE file for details.
##

import ctypes as ct

from .ctidas import *


class Block(object):
    """
    Class which represents a TIDAS block.
    """
    def __init__(self, handle):
        self.cp = handle

    def close(self):
        if self.cp is not None:
            lib.ctidas_block_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def _handle(self):
        return self.cp

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
            cp = lib.ctidas_block_group_get(self.cp, n)
            ret[n] = Group(handle=cp)
        return ret

    def group_add(self, name, grp):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cgrp = grp._handle()
        if grp._handle() is None:
            cschm = lib.schema_py2c(grp.schema)
            cdict = lib.dict_py2c(grp.props)
            cgrp = lib.ctidas_group_alloc(cschm, cdict, ct.c_longlong(grp.size))
            lib.ctidas_schema_free(cschm)
            lib.ctidas_dict_free(cdict)
        cblkgrp = lib.ctidas_block_group_add(self.cp, name, cgrp)
        if grp._handle() is None:
            lib.ctidas_group_free(cgrp)
        return Group(handle=cblkgrp)

    def group_del(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        lib.ctidas_block_group_del(self.cp, name)
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
            cp = lib.ctidas_block_intervals_get(self.cp, n)
            ret[n] = Intervals(handle=cp)
        return ret

    def intervals_add(self, name, intr):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cintr = intr._handle()
        if intr._handle() is None:
            cdict = lib.dict_py2c(intr.props)
            cintr = lib.ctidas_intervals_alloc(cdict, ct.c_longlong(intr.size))
            lib.ctidas_dict_free(cdict)
        cblkintr = lib.ctidas_block_intervals_add(self.cp, name, cintr)
        if intr._handle() is None:
            lib.ctidas_intervals_free(cintr)
        return Intervals(handle=cblkintr)

    def intervals_del(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        lib.ctidas_block_intervals_del(self.cp, name)
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
            cb = lib.ctidas_block_child_get(self.cp, n)
            ret[n] = Block(cb)
        return ret

    def block_add(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cb = lib.ctidas_block_alloc()
        cblk = lib.ctidas_block_child_add(self.cp, name, cb)
        lib.ctidas_block_free(cb)
        return Block(cblk)

    def block_del(self, name):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        lib.ctidas_block_child_del(self.cp, name)
        return

    def select(self, filter=''):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        cblk = lib.ctidas_block_select(self.cp, filter)
        return Block(cblk)
