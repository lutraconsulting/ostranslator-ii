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
from builtins import object
import psycopg2
try:
    from qgis.core import QgsVectorLayer
except ImportError:
    pass  # We may be calling the script from the console in which case this import is not used

from qgis.PyQt.Qt import QDomDocument
from . import utils


class Styler(object):
    def __init__(self, cur, uri, schema, osmm_schema, osmm_style_name):
        self.cur = cur
        self.uri = uri
        self.schema = schema
        self.osmm_schema = osmm_schema  # A string
        self.osmm_style_name = osmm_style_name
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
        base_url = 'https://raw.githubusercontent.com/OrdnanceSurvey/OS-Master-Map-Topography'

        if self.osmm_schema == '7':
            qml_base = base_url + '/Styling/Stylesheets/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20(QML)/'

            self.qmlLocations = { 'topographicarea' : qml_base + 'OSMM%20Topo%20-%20Topographic%20Area.qml',
                                  'cartographicsymbol' : qml_base + 'OSMM%20Topo%20-%20Cartographic%20Symbol.qml',
                                  'cartographictext' : qml_base + 'OSMM%20Topo%20-%20Cartographic%20Text.qml',
                                  'boundaryline' : qml_base + 'OSMM%20Topo%20-%20Boundary%20Line.qml',
                                  'topographicline' : qml_base + 'OSMM%20Topo%20-%20Topographic%20Line.qml',
                                  'topographicpoint' : qml_base + 'OSMM%20Topo%20-%20Topographic%20Point.qml' }

            sql_base = base_url + '/Styling/Stylesheets/Schema%20version%209/SQL/PostGIS/Array/'
            sql_mode = '_createtable_array.sql'
            for t in self.styleSupportedTopoTables:
                self.sqlLocations[t] = sql_base + t + sql_mode


        elif self.osmm_schema == '9':
            allowed_style_names = ["backdrop", "light", "standard", "outdoor"]
            if (not self.osmm_style_name in allowed_style_names):
                self.osmm_style_name = "standard"

            qml_base = base_url + '/master/Styling/Stylesheets/Schema%20version%209/Stylesheets/QGIS%20Stylesheets%20(QML)/'

            sql_base = base_url + '/master/Styling/Stylesheets/Schema%20version%209/SQL/PostGIS/Array/'
            sql_mode = '_createtable_array.sql'

            for t in self.styleSupportedTopoTables:
                if t == "boundaryline" and self.osmm_style_name == "outdoor":
                    # boundaryLine does not have outdoor style at all
                    qml_mode = "-standard.qml"
                else:
                    qml_mode = "-" + self.osmm_style_name + '.qml'

                self.qmlLocations[t] = qml_base + t + qml_mode
                self.sqlLocations[t] = sql_base + t + sql_mode

        else:
            raise Exception("Unsupported OSMM schema {}".format(self.osmm_schema))


    def addFields(self, table):
        
        # Only style if it looks like a topo layer
        if not table in list(self.sqlLocations.keys()):
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
        utils.delete(sqlPath)
        
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
        
        if table not in list(self.qmlLocations.keys()):
            return False
        
        defaultStyleName = f'{table} OS style'
        
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
        utils.delete(qmlPath)

        if not success:
            raise Exception('Failed to load layer style: %s\n\nThis can happen when using free wifi connections requiring registration.' % message)

        # Update layer_styles to ensure the relavant row references the destination schema
        qDic = {}
        qDic['dest_schema'] = self.schema
        qDic['tmp_schema'] = self.schema + '_tmp'
        qDic['table'] = table
        qDic['style_name'] = defaultStyleName
        try:
            qDic['geom_type'] = pgLayer.geometryType().name
        except AttributeError:
            gtype = {
                0: "Point",
                1: "Line",
                2: "Polygon",
                3: "UnknownGeometry",
                4: "NullGeometry",
            }
            qDic['geom_type'] = gtype.get(pgLayer.geometryType(), "UnknownGeometry")

        failedDbStyleSaveError = 'Failed to save style to database (postgres). Please first ensure you can successfully save ' \
                                 'layer styles to the database normally in QGIS: Right click a layer > Properties > Style > ' \
                                 'Save Style > Save in database (postgres). This error usually indicates an underlying ' \
                                 'database permissions issue.'

        try:
            self.cur.execute("""DELETE FROM layer_styles 
                            WHERE
                                f_table_schema = %(dest_schema)s AND
                                f_table_name = %(table)s AND
                                f_geometry_column = 'wkb_geometry' AND
                                stylename = %(style_name)s""", qDic)
        except:
            pass
            # it is possible the styles table doesn't exist yet

        pgLayer.saveStyleToDatabase(defaultStyleName, '', True, '')

        del pgLayer

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

        try:
            self.cur.execute("""UPDATE layer_styles SET
                                    type = %(geom_type)s
                                WHERE
                                    f_table_schema = %(dest_schema)s AND
                                    f_table_name = %(table)s AND
                                    f_geometry_column = 'wkb_geometry' AND
                                    stylename = %(style_name)s""", qDic)
            if self.cur.rowcount != 1:
                # Either no rows have been updated or oddly more than one has
                raise Exception('Error: %s' % failedDbStyleSaveError)
        except psycopg2.ProgrammingError:
            raise Exception(f'Error: Could not set geometry type for {table}')

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
