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
import traceback
import sys
from qgis.PyQt.QtCore import *
from styler import *

class PostProcessorThread(QThread):
    
    error = pyqtSignal(str)
    progressChanged = pyqtSignal(int)
    
    """
        Data is initially loaded into temporary tables with the format
        dest_schema.dest_table_tmp (_tmp suffix)
        After post-processing, any old dversions of tables are dropped 
        and the new tables are renamed.
        Bear in mind that self.tables contains the target tables 
        (without the _tmp suffix).
    """
    
    def __init__(self,
                 cur,
                 uri,
                 schema,
                 tables,
                 osmm_schema,
                 osmm_style_name,
                 createSpatialIndex=True,
                 dedup=True,
                 addTopoStyleColumns=True,
                 applyDefaultStyle=True):
        QThread.__init__(self)
        self.debug = False
        self.cur = cur
        self.uri = uri
        self.schema = schema
        self.tables = tables
        self.createSpatialIndex = createSpatialIndex
        self.dedup = dedup
        self.addTopoStyleColumns = addTopoStyleColumns
        self.applyDefaultStyle = applyDefaultStyle
        self.osmm_style_name = osmm_style_name

        # Number of post-processing steps. Used to calculate the 
        # progress
        self.post_processing_steps = 4
        self.styler = None
        if self.addTopoStyleColumns:
            self.styler = Styler(cur=cur,
                                 uri=self.uri,
                                 schema=schema,
                                 osmm_schema=osmm_schema,
                                 osmm_style_name=osmm_style_name)

    def run(self):
        # import pydevd; pydevd.settrace()
        """
            The (current) logic:
            
            * Optionally add style columns to MM topo:
                * select _tmp to _tmp_style
                * drop _tmp (it is assumed that space is freed immediately)
                *  rename _tmp_style to _tmp
            
            * Optionally create spatial index on table_tmp
            * Drop any 'pioneer' rows
            * Optionally deduplicate 
            * Replace tables
                * Rename current to _old
                * Rename _tmp to current
                * Drop _old
            * ANALYZE
            
        """
        # TODO parallelise this function
        try:
            qDic = {}
            i = 0
            for table in self.tables:
                # Determine whether unique identifiers are in 'fid' or 'gml_id' column


                # Optionally add additional style columns for OS MasterMap Topo Layer
                if self.addTopoStyleColumns:
                    self.styler.addFields(table)
                if self.addTopoStyleColumns and self.applyDefaultStyle:
                    # We don't want this to kill-off this thread:
                    try:
                        self.styler.applyDefaultStyle(table)
                    except:
                        self.error.emit( 'Post-processor error: ' + str(sys.exc_info()[1]) )
                
                if self.createSpatialIndex:
                    try:
                        self.cur.execute("""CREATE INDEX """ + table + """_wkb_geometry_gist ON """ + self.schema + """_tmp.""" + table + """ USING gist (wkb_geometry)""", qDic)
                    except psycopg2.ProgrammingError as e:
                        err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                        if err.startswith('relation') and err.endswith('does not exist') or \
                           err == 'column "wkb_geometry" does not exist':
                            # There were no matching features imported so the table was not created, do not index
                            pass
                    else:
                        if self.cur.statusmessage != 'CREATE INDEX':
                            self.error.emit('Failed to create index on geometry for %s.%s' % (self.schema+'_tmp', table))
                i += 1
                progress = int( float(i) / (len(self.tables)*self.post_processing_steps) * 100.0 )
                self.progressChanged.emit(progress)

                try:
                    # Drop any 'pioneer' rows
                    self.cur.execute("""DELETE FROM """ + self.schema + """_tmp.""" + table + """ WHERE fid = 'osgb----------------' AND ogc_fid < 100""", qDic)
                    # This limits the sequential search for fids to only the first 100 rows (where the pioneers will be).
                    # It means we do no longer need to build an index for the fid column
                except psycopg2.ProgrammingError as e:
                    err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                    if err.startswith('relation') and err.endswith('does not exist'):
                        # There were no matching features imported so the table was not created, do not index
                        pass
                    elif err.startswith('column') and err.endswith('does not exist'):
                        try:
                            # Looks like we're using gml_id, not fid
                            self.cur.execute("""DELETE FROM """ + self.schema + """_tmp.""" + table + """ WHERE gml_id = 'osgb----------------' AND ogc_fid < 100""", qDic)
                            if not self.cur.statusmessage.startswith('DELETE'):
                                self.error.emit('Failed to delete pioneer rows for %s.%s' % (self.schema+'_tmp', table))
                        except psycopg2.ProgrammingError:
                            # neither fid nor gml_id delete queries were successful, related to ignore the FID option
                            # see also https://github.com/lutraconsulting/ostranslator-ii/issues/48
                            pass
                    else:
                        raise Exception(str(sys.exc_info()))
                else:
                    if not self.cur.statusmessage.startswith('DELETE'):
                        self.error.emit('Failed to delete pioneer rows for %s.%s' % (self.schema+'_tmp', table))
                i += 1
                progress = int( float(i) / (len(self.tables)*self.post_processing_steps) * 100.0 )
                self.progressChanged.emit(progress)
                
                if self.dedup:
                    try:
                        # Create a quick hash index
                        self.cur.execute("""CREATE INDEX """ + table + """_fid_hash ON """ + self.schema + """_tmp.""" + table + """ USING hash (fid)""", qDic)
                        # De-dup the table
                        self.cur.execute("""DELETE FROM 
                                                """ + self.schema + """_tmp.""" + table + """ 
                                            USING 
                                                """ + self.schema + """_tmp.""" + table + """ AS dup
                                            WHERE 
                                                """ + self.schema + """_tmp.""" + table + """.fid = dup.fid AND 
                                                """ + self.schema + """_tmp.""" + table + """.ogc_fid < dup.ogc_fid;""", qDic)
                        # Drop the index
                        self.cur.execute("""DROP INDEX """ + table + """_fid_hash""", qDic)
                    except psycopg2.ProgrammingError as e:
                        err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                        if err.startswith('relation') and err.endswith('does not exist'):
                            # There were no matching features imported so the table was not created, do not index
                            pass
                
                i += 1
                progress = int( float(i) / (len(self.tables)*self.post_processing_steps) * 100.0 )
                self.progressChanged.emit(progress)
                
                # In a transaction:
                # -     Drop the target table
                # -     Move the new table from the _tmp to target schema
                # -     Commit
                try:
                    self.cur.execute("""START TRANSACTION""", qDic)
                    self.cur.execute("""DROP TABLE IF EXISTS """ + self.schema + """.""" + table, qDic)
                    self.cur.execute("""ALTER TABLE """ + self.schema + """_tmp.""" + table + """ SET SCHEMA """ + self.schema, qDic)
                    self.cur.execute("""COMMIT""", qDic)
                except psycopg2.ProgrammingError as e:
                    err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                    if err.startswith('relation') and err.endswith('does not exist'):
                        # There were no matching features imported so the table was not created, do not analyse
                        self.cur.execute("""ROLLBACK""", qDic)
                
                try:
                    self.cur.execute("""ANALYZE """ + self.schema + """.""" + table, qDic)
                except psycopg2.ProgrammingError as e:
                    err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                    if err.startswith('relation') and err.endswith('does not exist'):
                        # There were no matching features imported so the table was not created, do not analyse
                        pass
                else:
                    if not self.cur.statusmessage.startswith('ANALYZE'):
                        self.error.emit('Failed to analyze %s.%s' % (self.schema, table))
                
                """
                    We loaded the data using ogr2ogr into a table with a _tmp suffix.
                    ogr2ogr will have registered the table in public.geometry_columns
                    We will therefore need to update this row to reflect the fact that 
                    the table no longer has the _tmp suffix at this point.
                """
                self.cur.execute("""SELECT postgis_version()""", qDic)
                pgVersionString, = self.cur.fetchone()
                if pgVersionString.startswith('1.'):
                    try:
                        qDic['schema'] = self.schema
                        qDic['schema_tmp'] = self.schema + '_tmp'
                        qDic['table'] = table
                        
                        self.cur.execute("""SELECT f_table_catalog, f_geometry_column, coord_dimension, srid, type FROM public.geometry_columns WHERE f_table_schema = %(schema)s AND f_table_name = %(table)s""", qDic)
                        assert self.cur.rowcount <= 1
                        if self.cur.rowcount == 1:
                            # If we have previously imported a layer with this name there will already be a reference to it in 
                            # the public.geometry_columns table.  In order to not violate the integrity of this table we must delete 
                            # any old reference an re-insert.
                            # 
                            # TODO: What's the implication of not also matching the f_table_catelog column?
                            qDic['f_table_catalog'], qDic['f_geometry_column'], qDic['coord_dimension'], qDic['srid'], qDic['type'] = self.cur.fetchone()
                            self.cur.execute("""DELETE FROM UPDATE public.geometry_columns WHERE f_table_schema = %(schema)s AND f_table_name = %(table)s""", qDic)
                            self.cur.execute("""INSERT INTO public.geometry_columns 
                                                    (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, type) 
                                                VALUES 
                                                    (%(f_table_catalog)s, %(schema)s, %(table)s, %(f_geometry_column)s, %(coord_dimension)s, %(srid)s, %(type)s)""", qDic)
                        else:
                            self.cur.execute("""UPDATE public.geometry_columns SET f_table_schema = %(schema)s WHERE f_table_schema = %(schema_tmp)s AND f_table_name = %(table)s""", qDic)
                    except psycopg2.ProgrammingError as e:
                        err = e.pgerror.splitlines()[0].replace('ERROR:', '').strip()
                        if err.startswith('relation') and err.endswith('does not exist'):
                            # There were no matching features imported so the table was not created, do not analyse
                            pass
                    else:
                        if not self.cur.statusmessage.startswith('UPDATE'):
                            self.error.emit('Failed to update geometry_columns table for %s.%s' % (self.schema, table))
                            # TODO: Check this.
                
                i += 1
                progress = int( float(i) / (len(self.tables)*self.post_processing_steps) * 100.0 )
                self.progressChanged.emit(progress)
        except:
            if self.debug:
                self.error.emit( 'Post-processor error: ' + str(traceback.format_exc()) )
            else:
                self.error.emit( 'Post-processor error: ' + str(sys.exc_info()[1]) )
