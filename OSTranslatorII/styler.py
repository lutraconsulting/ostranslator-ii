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

import psycopg2
try:
    from qgis.core import QgsVectorLayer
except ImportError:
    pass  # We may be calling the script from the console in which case this import is not used

from PyQt4.Qt import QDomDocument
import utils


class Styler():
    def __init__(self, cur, uri, schema, osmm_schema):
        self.cur = cur
        self.uri = uri
        self.schema = schema
        self.osmm_schema = osmm_schema #7-9
        self.tmpSchema = schema + '_tmp' # We work on the temporary version of the table
        self.styleSupportedTopoTables = ['topographicarea',
                                         'cartographicsymbol',
                                         'cartographictext',
                                         'boundaryline',
                                         'topographicline',
                                         'topographicpoint']

        self.qmlLocations = {}
        self.sqlLocations = {}
        self.populate_locations()

    def populate_locations(self):
        base_url = 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets'

        if self.osmm_schema == 7:
            qml_base = base_url + '/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20'

            self.qmlLocations = { 'topographicarea' : qml_base + 'Topographic%20Area.qml',
                                  'cartographicsymbol' : qml_base + 'Cartographic%20Symbol.qml',
                                  'cartographictext' : qml_base + 'Cartographic%20Text.qml',
                                  'boundaryline' : qml_base + 'Boundary%20Line.qml',
                                  'topographicline' : qml_base + 'Topographic%20Line.qml',
                                  'topographicpoint' : qml_base + 'Topographic%20Point.qml' }

            sql_base = base_url + '/v1.0.0/Schema%20version%207/SQL/PostGIS/Array/'
            sql_mode = '_createtable_array.sql'
            for t in self.styleSupportedTopoTables:
                self.sqlLocations[t] = sql_base + t + sql_mode


        elif self.osmm_schema == 9:
            qml_base = base_url + '/master/Schema%20version%209/Stylesheets/QGIS%20stylesheets%20(QML)/'
            qml_mode = '-standard.qml' #TODO allow user to choose between standard, light and outdoor

            sql_base = base_url + '/master/Schema%20version%209/SQL/PostGIS/Array/'
            sql_mode = '_createtable_array.sql'

            for t in self.styleSupportedTopoTables:
                self.qmlLocations[t] = qml_base + t + qml_mode
                self.sqlLocations[t] = sql_base + t + sql_mode

        else:
            raise Exception("Unsupported OSMM schema {}".format(self.osmm_schema))


    def addFields(self, table):
        
        # Only style if it looks like a topo layer
        if not table in self.sqlLocations.keys():
            return
        
        self.prepare(table)

        # Download SQL file
        sqlPath = utils.download(self.sqlLocations[table], table + '.sql')
        sqlQuery = ""
        # also patch, we need different schema name
        with open(sqlPath, "rt") as fin:
            for line in fin:
                sqlQuery += line.replace(' osmm_topo.', " " + self.tmpSchema + ".") + "\n"


        self.cur.execute(sqlQuery, {})

        self.cleanUp(table)
        
    def applyDefaultStyle(self, table):
        """
            Returns True if a style was found, false otherwise
            
            * Grab the associated .qml file and make a QDomDocument out of it
            * Temporarily load the layer
            * Load style from file into a QDomDocument
            * Apply the style with QgsMapLayer::importNamedStyle
            * save it to the DB using QGIS (as the default style)
                QgsVectorLayer::saveStyleToDatabase
            
            Note that this function is called AFTER the table has been moved to the permanent schema to ensure the saved 
            style points to the right schema.
            
        """
        
        if not table in self.qmlLocations.keys():
            return False
        
        defaultStyleName = 'Default OS Style'
        
        # Read the QML
        qmlPath = utils.download(self.qmlLocations[table], table + '.qml')
        domDoc = QDomDocument('default')
        # In odd circumstances (sh*tty wifi connections that require you to register or login) we may have ended up downloading 
        # some odd HTML document.
        with open(qmlPath, 'r') as inf:
            domDoc.setContent(inf.read())
        
        pgLayer = QgsVectorLayer(self.getLayerUri(table, schemaType='tmp'), 'tmp_layer', 'postgres')
        if not pgLayer.isValid():
            raise Exception('Failed to load layer %s for applying default style.' % table)
        
        success, message = pgLayer.importNamedStyle(domDoc)
        if not success:
            raise Exception('Failed to load layer style: %s\n\nThis can happen when using free wifi connections requiring registration.' % message)
        try:
            # Technically we should only pass .. bool, string but there are some 
            # issues with SIP that will be resolved shortly
            # 2/7/15 Updated call below based on Martin's feedback of the 27/5/15
            pgLayer.saveStyleToDatabase(defaultStyleName, '', True, '', None)
        except TypeError:
            # For the case when the SIP files are fixed
            # TODO: Clean this up (eventually)
            pgLayer.saveStyleToDatabase(defaultStyleName, '', True, '')

        del pgLayer # Unload
        
        # Update layer_styles to ensure the relavant row references the destination schema
        qDic = {}
        qDic['dest_schema'] = self.schema
        qDic['tmp_schema'] = self.schema + '_tmp'
        qDic['table'] = table
        qDic['style_name'] = defaultStyleName
        failedDbStyleSaveError = 'Failed to save style to database (postgres). Please first ensure you can successfully save ' \
                                 'layer styles to the database normally in QGIS: Right click a layer > Properties > Style > ' \
                                 'Save Style > Save in database (postgres). This error usually indicates an underlying ' \
                                 'database permissions issue.'
        try:
            self.cur.execute("""UPDATE layer_styles SET
                                    f_table_schema = %(dest_schema)s
                                WHERE
                                    f_table_schema = %(tmp_schema)s AND
                                    f_table_name = %(table)s AND
                                    f_geometry_column = 'wkb_geometry' AND
                                    stylename = %(style_name)s""", qDic)
            if self.cur.rowcount != 1:
                # Either no rows have been updated or oddly more than one has
                raise Exception('Error: %s' % failedDbStyleSaveError)
        except psycopg2.ProgrammingError:
            raise Exception('Error: %s' % failedDbStyleSaveError)
        
        return True

    def getLayerUri(self, table, schemaType='destination'):
        # set database schema, table name, geometry column and optionally
        # subset (WHERE clause)
        # Note that this function returns the destination schema, not temporary
        if schemaType == 'destination':
            schema = self.schema
        elif schemaType == 'tmp':
            schema = self.schema + '_tmp'
        else:
            raise Exception('getLayerUri: Unexpected schemaType argument')
        self.uri.setDataSource(schema, table, 'wkb_geometry')
        return self.uri.uri()

    def prepare(self, table):
        self.cur.execute("""DROP TABLE IF EXISTS """ + self.tmpSchema + """.""" + table + """_style""", {})

    def cleanUp(self, table):
        self.cur.execute("""DROP TABLE IF EXISTS """ + self.tmpSchema + """.""" + table, {})
        self.cur.execute("""ALTER TABLE """ + self.tmpSchema + """.""" + table + """_style RENAME TO """ + table, {})
        # Add back primary key constraint
        self.cur.execute("""ALTER TABLE """ + self.tmpSchema + """.""" + table + """ ADD PRIMARY KEY (ogc_fid)""", {})
