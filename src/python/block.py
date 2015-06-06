
import ctypes as ct

import ctidas as tds


class Block(object):
    """
    Class which represents a TIDAS block.
    """
    def __init__(self, handle=None):
        self.cp = handle

    def close(self):
        if self.cp is not None:
            tds.lib.ctidas_block_free(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def blocks(self):
        if self.cp is None:
            raise RuntimeError("block is not associated with a volume")
        names = tds.cblock_children(self.cp)
        ret = {}
        for n in names:
            cb = tds.lib.ctidas_block_child_get(self.cp, n)
            ret[n] = Block()
            ret[n].open(cb)
        return ret

