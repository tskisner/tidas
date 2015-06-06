
import os

import ctypes as ct
import ctidas as tds


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
        self.cp = ct.POINTER(tds.cVolume)
        self.root = Block()
        if os.path.isdir(self.path):
            if (mode == "w"):
                self.mode = "write"
            else:
                self.mode = "read"
            self.cp = tds.lib.ctidas_volume_open(path, tds.access_mode[self.mode.value])
        else:
            self.mode = "write"
            self.cp = tds.lib.ctidas_volume_create(path, tds.backend_type[backend.value], tds.compression_type[comp.value])
        croot = tds.lib.ctidas_volume_root(self.cp)
        self.root.open(croot)

    def close(self):
        if self.cp is not None:
            tds.lib.ctidas_volume_close(self.cp)
        self.cp = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        return False

    def blocks(self):
        return self.root.blocks()
