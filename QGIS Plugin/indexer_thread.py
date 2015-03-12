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

import psycopg2
from PyQt4.QtCore import *

class IndexerThread(QThread):
    
    error = pyqtSignal(str)
    progressChanged = pyqtSignal(int)
    
    def __init__(self, cur, schema, tables, createSpatialIndex=True, dedup=True):
        QThread.__init__(self)
        self.cur = cur
        self.schema = schema
        self.tables = tables
        self.createSpatialIndex = createSpatialIndex
        self.dedup = dedup

    def run(self):
        
        # Todo parallelise this function
        qDic = {}
        i = 0
        for table in self.tables:
            if self.createSpatialIndex:
                try:
                    self.cur.execute("""CREATE INDEX """ + table + """_wkb_geometry_gist ON """ + self.schema + """.""" + table + """ USING gist (wkb_geometry)""", qDic)
                except psycopg2.ProgrammingError, e:
                    if e.message.strip().startswith('relation') and e.message.strip().endswith('does not exist') or \
                       e.message.strip() == 'column "wkb_geometry" does not exist':
                        # There were no matching features imported so the table was not created, do not index
                        pass
                else:
                    if self.cur.statusmessage != 'CREATE INDEX':
                        error.emit('Failed to create index on geometry for %s.%s' % (self.schema, table))
            i += 1
            progress = int( float(i) / (len(self.tables)*3) * 100.0 )
            self.progressChanged.emit(progress)
            
            try:
                # Drop any 'pioneer' rows
                self.cur.execute("""DELETE FROM """ + self.schema + """.""" + table + """ WHERE fid = 'osgb----------------' AND ogc_fid < 100""", qDic)
                # This limits the sequential search for fids to only the first 100 rows (where the pioneers will be).
                # It means we do no longer need to build an index for the fid column
            except psycopg2.ProgrammingError, e:
                if e.message.strip().startswith('relation') and e.message.strip().endswith('does not exist'):
                    # There were no matching features imported so the table was not created, do not index
                    pass
            else:
                if not self.cur.statusmessage.startswith('DELETE'):
                    error.emit('Failed to delete pioneer rows for %s.%s' % (self.schema, table))
            i += 1
            progress = int( float(i) / (len(self.tables)*3) * 100.0 )
            self.progressChanged.emit(progress)
            
            if self.dedup:
                try:
                    # Create a quick hash index
                    self.cur.execute("""CREATE INDEX """ + table + """_fid_hash ON """ + self.schema + """.""" + table + """ USING hash (fid)""", qDic)
                    # De-dup the table
                    self.cur.execute("""DELETE FROM 
                                            """ + self.schema + """.""" + table + """ 
                                        USING 
                                            """ + self.schema + """.""" + table + """ AS dup
                                        WHERE 
                                            """ + self.schema + """.""" + table + """.fid = dup.fid AND 
                                            """ + self.schema + """.""" + table + """.ogc_fid < dup.ogc_fid;""", qDic)
                    # Drop the index
                    self.cur.execute("""DROP INDEX """ + table + """_fid_hash""", qDic)
                except psycopg2.ProgrammingError, e:
                    if e.message.strip().startswith('relation') and e.message.strip().endswith('does not exist'):
                        # There were no matching features imported so the table was not created, do not index
                        pass
            try:
                self.cur.execute("""ANALYZE """ + self.schema + """.""" + table, qDic)
            except psycopg2.ProgrammingError, e:
                if e.message.strip().startswith('relation') and e.message.strip().endswith('does not exist'):
                    # There were no matching features imported so the table was not created, do not analyse
                    pass
            else:
                if not self.cur.statusmessage.startswith('ANALYZE'):
                    error.emit('Failed to analyze %s.%s' % (self.schema, table))
            i += 1
            progress = int( float(i) / (len(self.tables)*3) * 100.0 )
            self.progressChanged.emit(progress)
                    
