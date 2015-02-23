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

import os, sys, time, psycopg2, string, multiprocessing
import xml.etree.ElementTree as ET

from PyQt4 import QtGui, QtCore, uic
from import_manager import *
from result_dialog import *
from indexer_thread import *
# from about_dialog import *
import resources_rc

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'os_translator_ii_dialog_base.ui'))


class OsTranslatorIIDialog(QtGui.QDialog, FORM_CLASS):
    
    def __init__(self, iface, parent=None):
        """Constructor."""
        self.uiInitialised = False
        super(OsTranslatorIIDialog, self).__init__(parent)
        # Set up the user interface from Designer.
        # After setupUI you can access any designer object by doing
        # self.<objectname>, and you can use autoconnect slots - see
        # http://qt-project.org/doc/qt-4.8/designer-using-a-ui-file.html
        # #widgets-and-dialogs-with-auto-connect
        self.setupUi(self)
        self.setWindowIcon(QtGui.QIcon(QtGui.QPixmap(':/plugins/OsTranslatorII/icon.png')))
        
        self.iface = iface
        self.parent = parent
        
        self.gfsFolder = os.path.join(os.path.dirname(__file__), 'gfs')
        self.emptyGmlFile = os.path.join(self.gfsFolder, 'empty.gml')
        
        self.supDatasets = {}
        self.populateDatasets()
        
        self.configs = []
        self.initialise_ui()
        self.fieldsTreeWidget.itemChanged.connect(self.treeItemChanged)
        self.labelLineEdit.textChanged.connect(self.updateImportTaskName)

        self.im = ImportManager()
        self.im.finished.connect(self.createSpatialIndex)
        self.im.progressChanged.connect(self.onProgressChanged)

        self.log = ''
        self.statusLabel.setText('')
        
        
    def __del__(self):
        self.labelLineEdit.textChanged.disconnect(self.updateImportTaskName)
        self.fieldsTreeWidget.itemChanged.disconnect(self.treeItemChanged)
    
    def storeSettings(self):
        if not self.uiInitialised:
            return
        
        s = QtCore.QSettings()
        
        simultaneousJobs = self.simultaneousJobsSpinBox.value()
        s.setValue("OsTranslatorII/simultaneousJobs", simultaneousJobs)
        
        dataset = self.datasetComboBox.currentText()
        s.setValue("OsTranslatorII/dataset", dataset)
        
        connection = self.postgisConnectionComboBox.currentText()
        s.setValue("OsTranslatorII/connection", connection)
        
        mode = self.modeComboBox.currentText()
        s.setValue("OsTranslatorII/mode", mode)
        
        destSchema = self.destSchema.text()
        s.setValue("OsTranslatorII/destSchema", destSchema)
        
    
    def populateDatasets(self):
        """ Read the content of the gfs folder """
        thisDir = os.path.dirname(__file__)
        gfsPath = os.path.join(thisDir, 'gfs')
        for entry in os.listdir(gfsPath):
            entryPath = os.path.join(gfsPath, entry)
            head, tail = os.path.splitext(entry)
            if tail == ('.gfs') and os.path.isfile(entryPath):
                self.supDatasets[head] = entryPath
    
    def updateImportTaskName(self, newName):
        self.tasksListWidget.currentItem().setText(newName)
        i = self.tasksListWidget.currentRow()
        self.configs[i]['name'] = newName
        
    def treeItemChanged(self, item, col):
        if item.childCount() > 0:
            # parent
            childState = item.checkState(0)
            if childState != QtCore.Qt.PartiallyChecked:
                for i in range(item.childCount()):
                    item.child(i).setCheckState(0, childState)
        else:
            # Child
            self.updateParentCheckState(item.parent())
    
    def updateParentCheckState(self, item):
        # Check state of children has been updated
        allOn = True
        allOff = True
        if item == None:
            return
        for i in range(item.childCount()):
            if item.child(i).checkState(0) == QtCore.Qt.Checked:
                allOff = False
            else:
                allOn = False
        if allOn:
            item.setCheckState(0, QtCore.Qt.Checked)
        elif allOff:
            item.setCheckState(0, QtCore.Qt.Unchecked)
        else:
            item.setCheckState(0, QtCore.Qt.PartiallyChecked)
            
    def initialise_ui(self):
        
        defaultName = 'Task 1'
        self.tasksListWidget.addItem(defaultName)
        self.tasksListWidget.setCurrentRow(0)
        self.labelLineEdit.setText(defaultName)
        cfg = { 'name' : defaultName }
        self.configs.append(cfg)
        
        for name in self.supDatasets.keys():
            self.datasetComboBox.addItem(name)
        
        s = QtCore.QSettings()
        
        # Populate settings

        # self.postgisConnectionComboBox.addItem('DEBUG')

        s.beginGroup('PostgreSQL/connections')
        for connectionName in s.childGroups():
            self.postgisConnectionComboBox.addItem(connectionName)
        s.endGroup()
        
        dataset = str(s.value("OsTranslatorII/dataset", ''))
        self.datasetComboBox.setCurrentIndex(
            self.datasetComboBox.findText(dataset)
        )
        
        connection = str(s.value("OsTranslatorII/connection", ''))
        self.postgisConnectionComboBox.setCurrentIndex(
            self.postgisConnectionComboBox.findText(connection)
        )
        
        mode = str(s.value("OsTranslatorII/mode", ''))
        #self.modeComboBox.setCurrentIndex(
        #    self.modeComboBox.findText(mode)
        #)
        
        self.destSchema.setText( str(s.value("OsTranslatorII/destSchema", '')) )
        
        try:
            val, status = s.value("OsTranslatorII/simultaneousJobs", -1).toInt()
        except:
            val = int(s.value("OsTranslatorII/simultaneousJobs", -1))
        if val == -1:
            val = multiprocessing.cpu_count() / 2 # div by 2 in case of HT
        self.simultaneousJobsSpinBox.setValue( val )
        
        self.updateFieldsList()
        self.uiInitialised = True

    def getDatasetStructure(self, dsName):
        try:
            gfsFileName = self.supDatasets[ str(dsName) ]
        except KeyError:
            return {}
        
        root = ET.parse(gfsFileName)
        featureClasses = root.findall('./GMLFeatureClass[Name]')
        
        structure = {}
        for featureClass in featureClasses:
            structure[featureClass.findall('Name')[0].text] = []
            for fieldElem in featureClass.findall('./PropertyDefn/Name'):
                fieldName = fieldElem.text
                structure[featureClass.findall('Name')[0].text].append(fieldName)
        
        return structure
            
    def updateFieldsList(self):
        
        # Clear the list
        self.fieldsTreeWidget.clear()
        
        self.fieldsTreeWidget.setColumnCount(1)
        
        # Get the fields
        structure = self.getDatasetStructure( self.datasetComboBox.currentText() )
        
        for subset in structure.keys():
            tli = QtGui.QTreeWidgetItem(self.fieldsTreeWidget)
            tli.setText(0, subset)
            tli.setCheckState(0, QtCore.Qt.Checked)
            self.fieldsTreeWidget.addTopLevelItem(tli)
            for fieldName in structure[subset]:
                f = QtGui.QTreeWidgetItem(tli)
                f.setText(0, fieldName)
                f.setCheckState(0, QtCore.Qt.Checked)

    def getTmpFileName(self):
        if sys.platform == 'win32':
            # windows
            tmpFolder = os.environ['TEMP']
        elif sys.platform == 'darwin':
            # mac
            tmpFolder = os.environ['TMPDIR']
        else:
            # Linux and a wild guess at others
            tmpFolder = '/tmp'
        gfsFilePath = os.path.join( tmpFolder, str(time.time()) + '.gfs' )
        while os.path.isfile(gfsFilePath):
            time.sleep(1)
            gfsFilePath = os.path.join( tmpFolder, str(time.time()) + '.gfs' )
        return gfsFilePath
    
    def attributeSelected(self, topLevelIdx, attName):
        """ Determine whether the attribute called attName is checked 
        by the user in the top-level-element with index topLevelIdx """
        tli = self.fieldsTreeWidget.topLevelItem(topLevelIdx)
        for i in range( tli.childCount() ):
            if  tli.child(i).text(0) == attName and \
                tli.child(i).checkState(0) == QtCore.Qt.Checked:
                return True
        return False
    
    def buildGfs(self):
        """ Based on the user's field selection, go through the 'full' 
        gfs file and make a customised GFS for the import. """
        
        dsName = str(self.datasetComboBox.currentText())
        gfsFileName = self.supDatasets[dsName]
        root = ET.parse(gfsFileName)
        newRoot = ET.Element('GMLFeatureClassList')
        
        self.destTables = []
        for i in range(self.fieldsTreeWidget.topLevelItemCount()):
            tli = self.fieldsTreeWidget.topLevelItem(i)
            if tli.checkState(0) != QtCore.Qt.Unchecked:
                self.destTables.append( str(tli.text(0)).lower() )
                gMLFeatureClassElement = ET.Element('GMLFeatureClass')
                # The user has at least one field checked under this tli
                # Output the mandatory elements required
                childElems = root.findall('./GMLFeatureClass/[Name="%s"]/*' % tli.text(0))
                for childElem in childElems:
                    if childElem.tag != 'PropertyDefn':
                        gMLFeatureClassElement.append(childElem)
                    else:
                        # This is an attribute element
                        attName = childElem.findall('./Name')[0].text
                        if self.attributeSelected(i, attName):
                            gMLFeatureClassElement.append(childElem)
                newRoot.append(gMLFeatureClassElement)
        
        # write the gfs file out to a temp location
        fName = self.getTmpFileName()
        with open(fName, 'w') as f:
            f.write( ET.tostring(newRoot) )
        return fName
        
    def extractPgConnectionDetails(self):
        
        selectedConnection = self.postgisConnectionComboBox.currentText()

        if selectedConnection == 'DEBUG':
            self.host = 'localhost'
            self.database = 'ostranslator'
            self.user = 'postgres'
            self.password = 'postgres'
            self.port = 5432
            return
        
        s = QtCore.QSettings()
        self.host = str(s.value("PostgreSQL/connections/%s/host" % selectedConnection, ''))
        if len(self.host) == 0:
            # Looks like the preferred connection could not be found
            raise Exception('Details of the selected PostGIS connection could not be found, please check your settings')
        self.database = str(s.value("PostgreSQL/connections/%s/database" % selectedConnection, ''))
        self.user = str(s.value("PostgreSQL/connections/%s/username" % selectedConnection, ''))
        self.password = str(s.value("PostgreSQL/connections/%s/password" % selectedConnection, ''))
        try:
            self.port, dummy = s.value("PostgreSQL/connections/%s/port" % selectedConnection, 5432).toInt()
        except:
            self.port = int(s.value("PostgreSQL/connections/%s/port" % selectedConnection, 5432))
        
    def getDbCur(self):
        dbConn = psycopg2.connect( database = self.database,
                                   user = self.user,
                                   password = self.password,
                                   host = self.host,
                                   port = self.port)
        dbConn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return dbConn.cursor()
        
    def accept(self):
        
        # Check the user entered a folder path
        inputFolder = self.inputLineEdit.text()
        if len(inputFolder) == 0:
            QtGui.QMessageBox.critical(None, 'No Input Folder Selected', 'Please select an input folder.')
            return
        if not os.path.isdir(inputFolder):
            QtGui.QMessageBox.critical(None, 'Invalid Input Folder', '%s is not a valid folder path.' % inputFolder)
            return
        
        
        # Check a connection is selected
        if self.postgisConnectionComboBox.count() == 0:
            QtGui.QMessageBox.critical(None, 'No PostGIS Connection Selected', 'No PostGIS connection was selected. Please configure a connection through Layer > Add PostGIS Layers...')
            return
        self.extractPgConnectionDetails()
        
        # Ensure destination schema exists - promt to create it
        self.schema_name = str(self.destSchema.text())
        for ch in self.schema_name:
            if not ch in string.ascii_lowercase and not ch == '_':
                QtGui.QMessageBox.critical(None, 'Unsupported Schema Name', 'Schema names must currently consist of lower case characters and underscores.')
        if len(self.schema_name) == 0:
            errMsg = 'No destination schema was specified. Do you wish to import into the public schema?'
            reply = QtGui.QMessageBox.question(self.parent, 'No Schema Specified', errMsg, QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
            if reply == QtGui.QMessageBox.No:
                return
            self.schema_name = 'public'
            
        try:
            cur = self.getDbCur()
        except:
            QtGui.QMessageBox.critical(None, 'Failed to Connect to Database', 'Failed to make a connection to the database - please check your connection settings.')
            return

        try:
            qDic = {'schema_name' : self.schema_name}
            cur.execute("""SELECT schema_name FROM information_schema.schemata WHERE schema_name = %(schema_name)s;""", qDic)
        except:
            QtGui.QMessageBox.critical(None, 'Failed to Query Schemas', 'Failed to determine whether destination already exists - please check your connection settings.')
            return
        if cur.rowcount < 1:
            # The schema does not already exist - create it
            try:
                cur.execute("""CREATE SCHEMA """ + self.schema_name, qDic)
                if cur.statusmessage != 'CREATE SCHEMA':
                    raise Exception()
            except:
                QtGui.QMessageBox.critical(None, 'Failed to Create Schema', 'Failed to create schema - please check your connection settings.')
                return
        
        gfsFilePath = self.buildGfs()

        # If mode is create or replace, issue a warning
        if self.modeComboBox.currentText() == 'Create or Replace':
            # See if the table exists
            existingTables = []
            for dTable in self.destTables:
                try:
                    qDic['table_name'] = dTable
                    cur.execute(""" SELECT table_name FROM information_schema.tables
                                    WHERE table_schema = %(schema_name)s AND 
                                    table_name = %(table_name)s;""", qDic)
                except:
                    QtGui.QMessageBox.critical(None, 'Failed to Query Tables', 'Failed to determine whether destination table already exists - please check your connection settings.')
                    return
                if cur.rowcount > 0:
                    existingTables.append(dTable)
            if len(existingTables) > 0:
                errMsg = "The following tables will be permanently overwritten:\n\n"
                for exTab in existingTables:
                    errMsg += '%s.%s' % (self.schema_name,exTab) + '\n'
                errMsg += "\nDo you wish to proceed?"
                reply = QtGui.QMessageBox.question(self.parent, 'Overwriting Tables', errMsg, QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
                if reply == QtGui.QMessageBox.No:
                    return
        
        inputFiles = self.getInputFiles()
        # Insert a 'pioneer' file which contains a feature of each table type
        inputFiles.insert(0, self.getPioneerFile())
        
        # Ensure the user has selected some files
        if len(inputFiles) == 0:
            QtGui.QMessageBox.critical(None, 'No Input Files Selected', 'Failed to find any GML files under the selected folder.')
            return
        
        """ Add the jobs to the import manager """
        
        pgSource = 'PG:dbname=\'%s\' host=\'%s\' port=\'%d\' active_schema=%s user=\'%s\' password=\'%s\'' % \
            (self.database, self.host, self.port, self.schema_name, self.user, self.password)

        self.im.reset()

        i = 0
        for inputFile in inputFiles:
            if inputFile.lower().endswith('.gz'):
                inputFile = '/vsigzip/' + inputFile
            args = ['-f', 'PostgreSQL',
            '--config', 'PG_USE_COPY', 'YES',
            '--config', 'GML_GFS_TEMPLATE', gfsFilePath,
            pgSource, inputFile]

            if i == 0:
                args.insert(0, '-overwrite')
                args.extend(['-lco', 'OVERWRITE=YES',
                             '-lco', 'SPATIAL_INDEX=OFF',
                             '-lco', 'PRECISION=NO'])
            else:
                args.insert(0, '-append')

            i += 1

            self.im.add(args)

        try:
            self.im.start(self.simultaneousJobsSpinBox.value())
        except:
            QtGui.QMessageBox.critical(None, 'Failed to Start Process', 'Failed to start the import process - please ensure you have ogr2ogr installed.')
            return
        self.freezeUi()
        self.progressBar.setEnabled(True)
        self.progressBar.setValue(0)
        self.statusLabel.setText('Loading - grab a snack..')

    def onProgressChanged(self, prog):
        self.progressBar.setValue(prog)

    def createSpatialIndex(self):
        # Todo parallelise this function
        self.statusLabel.setText('Indexing - grab a sleeping bag..')
        self.indexingErrors = []
        cur = self.getDbCur()
        self.indexerThread = IndexerThread(cur, self.schema_name, self.destTables)
        self.indexerThread.finished.connect(self.importFinished)
        self.indexerThread.error.connect(self.onIndexerError)
        self.indexerThread.progressChanged.connect(self.onProgressChanged)
        self.progressBar.setValue(0)
        self.indexerThread.start()
    
    def onIndexerError(self, error):
        self.indexingErrors.append(error)
        
    def importFinished(self):
        
        # Populate the dialog with the log
        self.log = ''
        if len(self.im.crashedJobs) > 0:
            self.log += 'Warning: %d import jobs crashed:\n' % len(self.im.crashedJobs)
            for crashedJob in self.im.crashedJobs:
                self.log += '\n  Args: %s' % crashedJob.args
                self.log += '\n  Stdout: %s' % crashedJob.process.readAllStandardOutput()
                self.log += '\n  Stderr: %s' % crashedJob.process.readAllStandardError()
            self.log += '\n\n'

        if len(self.im.failedJobs) > 0:
            self.log += 'Warning: %d import jobs failed:\n' % len(self.im.failedJobs)
            for failedJob in self.im.failedJobs:
                self.log += '\n  Args: %s' % failedJob.args
                self.log += '\n  Stdout: %s' % failedJob.process.readAllStandardOutput()
                self.log += '\n  Stderr: %s' % failedJob.process.readAllStandardError()
            self.log += '\n\n'

        if len(self.im.crashedJobs) > 0 or len(self.im.failedJobs) > 0:
            self.log += 'Warning - some import jobs did not complete successfully.\n'
        else:
            self.log += 'All jobs completed successfully.\n'
        if len(self.indexingErrors) > 0:
            self.log += '\nFailed to create one or more spatial indices:\n\n'
            for indexFail in self.indexingErrors:
                self.log += '%s\n' % indexFail
        loadTimeSecs = (time.time() - self.im.startTime)
        self.log += '\nLoaded in %.1f hours (%d seconds).\n' % ((loadTimeSecs / 3600.0), loadTimeSecs)

        self.progressBar.setValue(100)
        resD = ResultDialog(self)
        resD.setText(self.log)
        resD.show()
        resD.exec_()
        
        self.statusLabel.setText('')
        self.progressBar.setValue(0)
        self.progressBar.setEnabled(False)
        self.thawUi()

    def freezeUi(self):
        uiElements = [self.tasksListWidget,
                      self.deleteTaskPushButton,
                      self.newTaskPushButton,
                      self.simultaneousJobsSpinBox,
                      self.helpPushButton,
                      self.aboutPushButton,
                      self.labelLineEdit,
                      self.datasetComboBox,
                      self.postgisConnectionComboBox,
                      self.modeComboBox,
                      self.destSchema,
                      self.batchModeCheckBox,
                      self.inputLineEdit,
                      self.browsePushButton,
                      self.fieldsTreeWidget,
                      self.buttonBox]

        for ie in uiElements:
            ie.setEnabled(False)

    def thawUi(self):
        uiElements = [self.tasksListWidget,
                      #self.deleteTaskPushButton,
                      #self.newTaskPushButton,
                      self.simultaneousJobsSpinBox,
                      self.helpPushButton,
                      #self.aboutPushButton,
                      self.labelLineEdit,
                      self.datasetComboBox,
                      self.postgisConnectionComboBox,
                      #self.modeComboBox,
                      self.destSchema,
                      #self.batchModeCheckBox,
                      self.inputLineEdit,
                      self.browsePushButton,
                      self.fieldsTreeWidget,
                      self.buttonBox]

        for ie in uiElements:
            ie.setEnabled(True)

    def getInputFiles(self):
        inputFiles = []
        inputDir = str(self.inputLineEdit.text())
        if not os.path.isdir(inputDir):
            raise Exception('%s is not a valid folder.' % inputDir)
        for path, dirs, files in os.walk(inputDir):
            for f in files:
                if f.lower().endswith('.gml') or f.lower().endswith('.gz'):
                    inputFiles.append(os.path.join(path, f))
        return inputFiles
    
    def getPioneerFile(self):
        dsName = str(self.datasetComboBox.currentText())
        gfsFileName = self.supDatasets[dsName]
        head, tail = os.path.splitext(gfsFileName)
        pioneerFilePath = head + ' Pioneer.gz'
        return pioneerFilePath

    def browseForInput(self):
        """ Open a browse for files dialog - for the moment set to 
        browse directory mode """
        settings = QtCore.QSettings()
        startingDir = str(settings.value("OsTranslatorII/lastInputFolder", os.path.expanduser("~")))
        d = str( QtGui.QFileDialog.getExistingDirectory(None, 'Browse For Input', startingDir) )
        if d <> os.sep and d.lower() <> 'c:\\' and d <> '':
            settings.setValue("OsTranslatorII/lastInputFolder", d)
            self.inputLineEdit.setText(d)

    def helpPressed(self):
        QtGui.QDesktopServices.openUrl(QUrl('http://www.lutraconsulting.co.uk/products/ostranslator-ii/'))

    def aboutPressed(self):
        aboutDlg = AboutDialog(self)
        aboutDlg.show()
        aboutDlg.exec_()
