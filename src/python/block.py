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
    def __init__(self, handle=None):
        self.cp = handle

    def close(self):
        if self.cp is not None:
            lib.ctidas_block_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def blocks(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = cblock_children(self.cp)
        ret = {}
        for n in names:
            cb = lib.ctidas_block_child_get(self.cp, n)
            ret[n] = Block()
            ret[n].open(cb)
        return ret

