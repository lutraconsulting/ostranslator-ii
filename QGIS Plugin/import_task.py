# -*- coding: utf-8 -*-
"""
/***************************************************************************
 OsTranslatorII
                                 A QGIS plugin
 A plugin for loading Ordnance Survey MasterMap and other GML-based datasets.
                             -------------------
        begin                : 2014-10-03
        copyright            : (C) 2014 by Peter Wells for Lutra Consulting
        email                : info@lutraconsulting.co.uk
        git sha              : $Format:%H$
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

from PyQt4.QtCore import *
from PyQt4.QtGui import *

import gzip

class ImportTask(QObject):
    
    finished = pyqtSignal(str, int, int)
    
    def __init__(self, cmd, args):
        QObject.__init__(self)
        self.cmd = cmd
        self.args = args
        self.process = None

    def __eq__(self, other):
        return id(self) == id(other)

    def start(self):
        self.process = QProcess()
        self.process.finished.connect(self.onProcessFinished)
        self.process.start(self.cmd, self.args)
        if not self.process.waitForStarted():
            raise Exception('Failed to start process.')

    def onProcessFinished(self, exitCode, exitStatus):
        #print 'Stdout was %s' % self.process.readAllStandardOutput()
        #print 'Stderr was %s' % self.process.readAllStandardError()
        self.finished.emit(str(id(self)), exitCode, exitStatus,)
