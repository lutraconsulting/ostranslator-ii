# -*- coding: utf-8 -*-
# OsTranslatorII QGIS Plugin
#
# Copyright (C) 2017 Lutra Consulting
# info@lutraconsulting.co.uk
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
import os
from .utils import OSII_icon_path

from qgis.PyQt import QtGui, QtCore, uic
from qgis.PyQt import QtWidgets

FORM_CLASS, _ = uic.loadUiType(os.path.join(os.path.dirname(__file__), 'ui', 'result_dialog_base.ui'))


class ResultDialog(QtWidgets.QDialog, FORM_CLASS):
    
    def __init__(self, parent=None):
        self.uiInitialised = False
        super(ResultDialog, self).__init__(parent)
        self.setupUi(self)
        self.setWindowIcon(QtGui.QIcon(QtGui.QPixmap(OSII_icon_path())))

        self.parent = parent
        
    def setText(self, text):
        self.textEdit.setText(text)
