# -*- coding: utf-8 -*-
"""
/***************************************************************************
 OsTranslatorIIDialog
                                 A QGIS plugin
 A plugin for loading Ordnance Survey MasterMap and other GML-based datasets.
                             -------------------
        begin                : 2014-10-03
        git sha              : $Format:%H$
        copyright            : (C) 2014 by Peter Wells for Lutra Consulting
        email                : info@lutraconsulting.co.uk
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

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
