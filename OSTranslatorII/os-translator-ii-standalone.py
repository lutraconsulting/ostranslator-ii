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

# Fix ValueError: API 'QDate' has already been set to version 1
try:
    import qgis.PyQt
except ImportError:
    pass

import sys
from PyQt4 import QtGui
from os_translator_ii_dialog import OsTranslatorIIDialog

def main():
    app = QtGui.QApplication(sys.argv)
    d = OsTranslatorIIDialog(None, None)
    d.show()
    d.exec_()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
