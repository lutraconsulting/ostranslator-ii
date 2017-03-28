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

from PyQt4.QtCore import *


class ImportTask(QObject):
    
    finished = pyqtSignal(str, int, int)
    
    def __init__(self, args):
        QObject.__init__(self)
        self.args = args
        self.process = None

    def __eq__(self, other):
        return id(self) == id(other)

    def start(self):
        self.process = QProcess()
        self.process.finished.connect(self.onProcessFinished)
        cmd = ''
        for arg in self.args:
            cmd += arg + ' '
        # print 'Starting %s' % cmd
        self.process.start('ogr2ogr', self.args)
        if not self.process.waitForStarted():
            raise Exception('Failed to start process. Please ensure you have gdal/ogr2ogr installed')

    def onProcessFinished(self, exitCode, exitStatus):
        #print 'Stdout was %s' % self.process.readAllStandardOutput()
        #print 'Stderr was %s' % self.process.readAllStandardError()
        self.finished.emit(str(id(self)), exitCode, exitStatus,)
