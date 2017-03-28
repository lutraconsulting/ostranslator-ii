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

import os
from utils import OSII_icon_path

from PyQt4 import QtGui, QtCore, uic

FORM_CLASS, _ = uic.loadUiType(os.path.join(os.path.dirname(__file__), 'ui', 'result_dialog_base.ui'))


class ResultDialog(QtGui.QDialog, FORM_CLASS):
    
    def __init__(self, parent=None):
        self.uiInitialised = False
        super(ResultDialog, self).__init__(parent)
        self.setupUi(self)
        self.setWindowIcon(QtGui.QIcon(QtGui.QPixmap(OSII_icon_path())))

        self.parent = parent
        
    def setText(self, text):
        self.textEdit.setText(text)
