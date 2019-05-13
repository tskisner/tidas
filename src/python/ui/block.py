##
##  TImestream DAta Storage (TIDAS)
##  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
##  is governed by a BSD-style license that can be found in the top-level
##  LICENSE file.
##
"""TIDAS User interface block model and view.
"""


from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (QWidget, QPlainTextEdit, QSplitter,
                             QHBoxLayout, QTableWidget, QTableWidgetItem,
                             QGridLayout)

from .utils import to_dict


class BlockModel(object):

    def __init__(self, handle):
        self.nchild = 5
        self.ngroup = 2
        self.nintervals = 3
        #self.props = to_dict(handle.dictionary())
        self.props = {
            "foo" : "{}".format(1234),
            "bar" : "{}".format(1.234),
            "bat" : "pooh"
        }


class BlockView(QWidget):
    """View of a Block.

    This shows 2 panes, the first with overview stats of the block and
    the second with dictionary items.

    """
    def __init__(self, model):
        super().__init__()
        self.model = model
        self.initUI()

    def initUI(self):
        tab = "    "
        # view of basic info
        info = QPlainTextEdit()
        info.setReadOnly(True)
        lines = ["Block:"]
        lines.append("{}{} Child Blocks".format(tab, self.model.nchild))
        lines.append("{}{} Data Groups".format(tab, self.model.ngroup))
        lines.append("{}{} Lists of Intervals".format(tab,
                                                      self.model.nintervals))
        info.setPlainText("\n".join(lines))

        props = QTableWidget(len(self.model.props.keys()), 2)
        row = 0
        for k in sorted(self.model.props.keys()):
            props.setItem(row, 0, QTableWidgetItem(k))
            props.setItem(row, 1, QTableWidgetItem(self.model.props[k]))
            row += 1

        # Try to fix the size of the table in the horizontal direction.
        #props.setHorizontalPolicy(QSizePolicy.Minimum)
        props.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        props.resizeColumnsToContents()
        #swidth = props.verticalScrollBar().width()
        # FIXME: how to find "future" width of a scrollbar if it appears?
        swidth = 20
        props.setFixedWidth(props.horizontalHeader().length() \
                           + props.verticalHeader().width() \
                           + swidth)

        # view = QSplitter(Qt.Horizontal)
        # view.addWidget(info)
        # view.addWidget(props)
        # view.setStretchFactor(1, 0)

        layout = QHBoxLayout(self)
        layout.addWidget(info)
        layout.addWidget(props, alignment=Qt.AlignRight)
        return
