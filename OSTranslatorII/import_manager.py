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
from builtins import str
from import_task import *
import time

class ImportManager(QObject):
    """ Manages import processes.
    """
    
    progressChanged = pyqtSignal(int)
    finished = pyqtSignal()
        
    def __init__(self):
        
        QObject.__init__(self)

        # self.jobs is a data structure describing the encoding work to be done
        # It is a list of dictionaries.  Each dict has the following keys:
        #   phase, pid, titleId, aid, name, series, episode, cropParams
        # phase is initially 1 (cropdetect) and progresses through 2 (pass 1) and 3 (pass 2)
        # pid is initially None and may reflect the pid assigned to the process running the current phase
        # cropParams is initially None
        self.reset()

    def reset(self):
        """ Make everything the way it was when we initialised ourselves """
        self.pendingJobs = []
        self.runningJobs = []
        self.totalJobs = 0
        self.successfulJobs = []
        self.crashedJobs = []
        self.failedJobs = []
        self.startTime = None

    def start(self, processCount=2):
        """ Start working on the queue - just 1 at first. """
        self.startTime = time.time()
        self.processCount = processCount
        job = self.pendingJobs.pop()
        self.runningJobs.append(job)
        job.start()

    def add(self, args):
        """ Append an import job to the queue """
        job = ImportTask(args)
        job.finished.connect(self.onJobFinished)
        self.pendingJobs.insert(0, job)
        self.totalJobs += 1

    def getFinishedJob(self, finishedJobId):
        for job in self.runningJobs:
            if str(id(job)) == str(finishedJobId):
                return job
        raise Exception('Failed to locate job with id %d' % finishedJobId)

    def onJobFinished(self, finishedJobId, exitCode, exitStatus):
        finishedJob = self.getFinishedJob(finishedJobId)
        if exitStatus != 0:
            # Crashed
            self.crashedJobs.append(finishedJob)
        elif exitCode != 0:
            # Failed
            self.failedJobs.append(finishedJob)
        else:
            self.successfulJobs.append(finishedJob)
        self.runningJobs.remove(finishedJob)
        finishedJobCount = len(self.successfulJobs) + len(self.crashedJobs) + len(self.failedJobs)
        self.progress = int(finishedJobCount / (float(self.totalJobs) * 1) * 100.0 )
        self.progressChanged.emit(self.progress)

        # Increase the number of running jobs to match self.processCount
        while len(self.runningJobs) < self.processCount and len(self.pendingJobs) > 0:
            job = self.pendingJobs.pop()
            self.runningJobs.append(job)
            job.start()

        # Determine if we have finished
        if len(self.pendingJobs) == 0 and len(self.runningJobs) == 0:
            self.finished.emit()

    def getImportReport(self):

        report = ''

        if len(self.crashedJobs) > 0:
            report += 'Warning: %d import jobs crashed:\n' % len(self.crashedJobs)
            for crashedJob in self.crashedJobs:
                report += '\n  Args: %s' % crashedJob.args
                report += '\n  Stdout: %s' % crashedJob.process.readAllStandardOutput()
                report += '\n  Stderr: %s' % crashedJob.process.readAllStandardError()
            report += '\n\n'

        if len(self.failedJobs) > 0:
            report += 'Warning: %d import jobs failed:\n' % len(self.failedJobs)
            for failedJob in self.failedJobs:
                report += '\n  Args: %s' % failedJob.args
                report += '\n  Stdout: %s' % failedJob.process.readAllStandardOutput()
                report += '\n  Stderr: %s' % failedJob.process.readAllStandardError()
            report += '\n\n'

        if len(self.crashedJobs) > 0 or len(self.failedJobs) > 0:
            report += 'Warning - some import jobs did not complete successfully.\n'
        else:
            report += 'All jobs completed successfully.\n\n'

        return report
