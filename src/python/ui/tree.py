##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
"""TIDAS User interface tree manipulation.
"""

from .._pytidas import (DataType, BackendType, CompressionType, AccessMode,
    LinkType, ExecOrder, Dictionary, Intrvl, Intervals, Field, Schema, Group,
    Block, Volume)

from PyQt5.QtGui import (QStandardItemModel, QStandardItem)

from PyQt5.QtWidgets import (QWidget, QPlainTextEdit, QSplitter,
                             QHBoxLayout, QTableWidget, QTableWidgetItem,
                             QGridLayout, QTreeView)


class VolumeModel(object):
    """Model containing the volume data.
    """
    def __init__(self, vol, filter):
        self.load(vol, filter)


    def load(self, vol, filter):
        self.vol = vol
        self.filter = filter

        # The data model
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(["Name", "Type"])
        self.model.setRowCount(0)
        print("created model", flush=True)

        # Properties of the currently selected object
        self.sel_block = None
        self.sel_blockpath = None
        self.sel_type = None
        self.sel_name = None

        def _procblock(pth, nm, blk, node):
            """Recursive helper function to build the tree.
            """
            # Metadata path to this block
            pthnm = "{}/{}".format(pth, nm)
            print(pthnm, flush=True)
            # List of child blocks
            childnames = blk.block_names()
            if len(childnames) == 0:
                # This block has no children.  Set leaf state to true
                parent = node.parent()
                parent.setChild(parent.rowCount() - 1, 2,
                                QStandardItem("TRUE"))
            for ch in childnames:
                # Get the child block
                childblk = blk.block_get(ch)
                # Add child block to model
                node.appendRow([
                    QStandardItem(ch),
                    QStandardItem("Block"),
                    QStandardItem("FALSE")
                ])
                # Get handle to this newly added child
                childnode = node.child(node.rowCount() - 1)
                # Recursively process this child
                childpthnm = pthnm
                if pthnm == "/":
                    childpthnm = ""
                _procblock(childpthnm, ch, childblk, childnode)

            # Process all groups, intervals, and other data for this node.
            grpnames = blk.group_names()
            for g in grpnames:
                grp = blk.group_get(g)
                # Add group to model
                node.appendRow([
                    QStandardItem(g),
                    QStandardItem("Group")
                ])
                del grp
            intrvlnames = blk.intervals_names()
            for it in intrvlnames:
                intrvl = blk.intervals_get(it)
                # Add intervals to model
                node.appendRow([
                    QStandardItem(it),
                    QStandardItem("Intervals")
                ])
                del intrvl

        blkroot = self.vol.root()
        root = self.model.invisibleRootItem()
        _procblock("", "", blkroot, root)
        return


    def __del__(self):
        try:
            del self.model
        except:
            pass
        try:
            del self.vol
        except:
            pass
        try:
            del self.sel_block
        except:
            pass
        return


class VolumeView(QTreeView):
    """Tree view of volume.
    """
    def __init__(self, volmodel):
        super().__init__()
        self.volmodel = volmodel
        self.initUI()


    def initUI(self):
        #self.tree = QTreeView(self)
        self.header().setDefaultSectionSize(180)
        self.setModel(self.volmodel.model)
        self.hideColumn(2)
        return

    def selectionChanged(self, selected, deselected):
        if len(selected) > 0:
            print("New selection is:")
            sel = selected.indexes()[0]
            print("  {}, {}".format(s.row(), s.column()), flush=True)
        pass

    #
    # def get_selected(self, selected, deselected):
    #     # Get the metadata path by traversing up the tree.
    #     # Find the top-level item in the tree.
    #     tree_indx = selected.indexes()[0]
    #     nodename = self.model.index(0, 0, tree_index).data().toString()
    #     metapath = nodename
    #     parent = tree_index.parent()
    #     while parent.isValid():
    #         tree_index = parent
    #         nodename = self.model.index(0, 0, tree_index).data().toString()
    #         metapath = "{}/{}".format(nodename, metapath)
    #         parent = parent.parent()
    #     metapath = "/{}".format(metapath)
    #     # Split this into the path to the parent block and the object name
    #     # and type.

    #
    #     if (self.sel_block is None) or (self.sel_blockpath != blockpath):
    #         # We have a different block open.  Close it and open the new
    #         # one.
    #         pass
    #
