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

import os, urllib2, traceback, psycopg2
try:
    from qgis.core import QgsVectorLayer
except ImportError:
    pass  # We may be calling the script from the console in which case this import is not used
from PyQt4.QtCore import QSettings
from PyQt4.Qt import QDomDocument

class Styler():

    def __init__(self, cur, uri, schema):
        self.cur = cur
        self.uri = uri
        self.schema = schema
        self.tmpSchema = schema + '_tmp' # We work on the temporary version of the table
        self.styleSupportedTopoTables = [   'topographicarea', 
                                            'cartographicsymbol', 
                                            'cartographictext', 
                                            'boundaryline', 
                                            'topographicline', 
                                            'topographicpoint']
        
        self.qmlLocations = { 'topographicarea' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Topographic%20Area.qml',
                              'cartographicsymbol' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Cartographic%20Symbol.qml',
                              'cartographictext' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Cartographic%20Text.qml',
                              'boundaryline' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Boundary%20Line.qml',
                              'topographicline' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Topographic%20Line.qml',
                              'topographicpoint' : 'https://raw.githubusercontent.com/OrdnanceSurvey/OSMM-Topography-Layer-stylesheets/v1.0.0/Schema%20version%207/Stylesheets/QGIS%20stylesheets%20%28QML%29/OSMM%20Topo%20-%20Topographic%20Point.qml' }
    
    def addFields(self, table):
        
        # Only style if it looks like a topo layer
        if not table in self.styleSupportedTopoTables:
            return
        
        self.prepare(table)
        if table == 'topographicarea':
            self.addTopographicAreaStyleFields(table)
        elif table == 'cartographicsymbol':
            self.addCartographicSymbolStyleFields(table)
        elif table == 'cartographictext':
            self.addCartographicTextStyleFields(table)
        elif table == 'boundaryline':
            self.addBoundaryLineStyleFields(table)
        elif table == 'topographicline':
            self.addTopographicLineStyleFields(table)
        elif table == 'topographicpoint':
            self.addTopographicPointStyleFields(table)
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
        qmlPath = self.downloadStyle(table)
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
    
    def downloadStyle(self, table):
        # Much of this is stolen from Crayfish - consider turning into 
        # something more reuseable.
        destFolder = os.path.dirname(__file__)
        s = QSettings()
        try:
            useProxy = s.value("proxy/proxyEnabled", False).toBool()
        except:
            useProxy = s.value("proxy/proxyEnabled", False, type=bool)
        if useProxy:
            proxyHost = s.value("proxy/proxyHost", unicode())
            proxyPassword = s.value("proxy/proxyPassword", unicode())
            proxyPort = s.value("proxy/proxyPort", unicode())
            proxyType = s.value("proxy/proxyType", unicode())
            proxyTypes = { 'DefaultProxy' : 'http', 'HttpProxy' : 'http', 'Socks5Proxy' : 'socks', 'HttpCachingProxy' : 'http', 'FtpCachingProxy' : 'ftp' }
            if proxyType in proxyTypes: proxyType = proxyTypes[proxyType]
            proxyUser = s.value("proxy/proxyUser", unicode())
            proxyString = 'http://' + proxyUser + ':' + proxyPassword + '@' + proxyHost + ':' + proxyPort
            proxy = urllib2.ProxyHandler({proxyType : proxyString})
            auth = urllib2.HTTPBasicAuthHandler()
            opener = urllib2.build_opener(proxy, auth, urllib2.HTTPHandler)
            urllib2.install_opener(opener)
        
        url = self.qmlLocations[table]
        generalDownloadFailureMessage = '\n\nPlease check your network settings. Styles may need to be added manually. This does not affect the loaded data.'
        try:
            conn = urllib2.urlopen(url, timeout=30) # Allow 30 seconds for completion
            if conn.getcode() != 200:
                # It looks like the request was unsuccessful
                raise Exception('Failed to download the stylesheet at %s\n\nThe HTTP status code was %d.' % (url,conn.getcode()) + generalDownloadFailureMessage)
        except urllib2.URLError as e:
            raise Exception('Failed to download the stylesheet at %s\n\nThe reason was %s.' % (url, e.reason) + generalDownloadFailureMessage)
        
        destinationFileName = os.path.join(destFolder, table + '.qml')
        if os.path.isfile(destinationFileName):
            os.unlink(destinationFileName)
        destinationFile = open(destinationFileName, 'wb')
        destinationFile.write( conn.read() )
        destinationFile.close()
        return destinationFileName
    
    def prepare(self, table):
        self.cur.execute("""DROP TABLE IF EXISTS """ + self.tmpSchema + """.""" + table + """_style""", {})
    
    def addTopographicAreaStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm IS NULL THEN 'Building Fill'
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm = '{"Multi Surface"}' THEN 'Multi Surface Fill'
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND make = 'Natural' THEN 'Natural Fill'
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm IS NULL AND make = 'Manmade' THEN 'Road Or Track Fill'
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND (make = 'Manmade' OR make = 'Unknown') THEN 'Manmade Fill'
                                    WHEN descriptivegroup @> '{Roadside}' AND make = 'Natural' THEN 'Roadside Natural Fill'
                                    WHEN descriptivegroup @> '{Roadside}' AND (make = 'Manmade' OR make = 'Unknown') THEN 'Roadside Manmade Fill'
                                    WHEN descriptivegroup @> '{"Inland Water"}' AND descriptiveterm IS NULL THEN 'Inland Water Fill'
                                    WHEN descriptivegroup @> '{Path}' AND descriptiveterm IS NULL THEN 'Path Fill'
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm = '{Track}' THEN 'Track Fill'
                                    WHEN descriptiveterm = '{Slope}' THEN 'Slope Fill'
                                    WHEN descriptivegroup @> '{Structure}' AND (descriptiveterm IS NULL OR descriptiveterm = '{"Upper Level Of Communication"}' OR descriptiveterm = '{"Overhead Construction"}') THEN 'Structure Fill'
                                    WHEN descriptiveterm = '{Cliff}' THEN 'Cliff Fill'
                                    WHEN descriptiveterm = '{Step}' THEN 'Step Fill'
                                    WHEN descriptiveterm = '{Foreshore}' THEN 'Foreshore Fill'
                                    WHEN descriptiveterm = '{"Traffic Calming"}' THEN 'Traffic Calming Fill'
                                    WHEN descriptivegroup = '{Glasshouse}' THEN 'Glasshouse Fill'
                                    WHEN descriptivegroup @> '{Rail}' AND descriptiveterm IS NULL AND make = 'Natural' THEN 'Rail Natural Fill'
                                    WHEN descriptiveterm = '{Pylon}' THEN 'Pylon Fill'
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Archway}'THEN 'Archway Fill'
                                    WHEN descriptivegroup @> '{Landform}' AND make = 'Natural' THEN 'Landform Natural Fill'
                                    WHEN descriptivegroup @> '{"Tidal Water"}' AND descriptiveterm IS NULL THEN 'Tidal Water Fill'
                                    WHEN descriptivegroup @> '{Landform}' AND make = 'Manmade' THEN 'Landform Manmade Fill'
                                    WHEN descriptivegroup = '{Rail}' AND make = 'Manmade' OR make = 'Unknown' THEN 'Rail Manmade Fill'
                                    WHEN (descriptiveterm @> '{"Nonconiferous Trees"}' OR descriptiveterm @> '{"Nonconiferous Trees (Scattered)"}') AND (descriptiveterm @> '{"Coniferous Trees"}' OR descriptiveterm @> '{"Coniferous Trees (Scattered)"}') THEN 'Mixed Woodland Fill'
                                    WHEN descriptiveterm @> '{"Nonconiferous Trees"}' OR descriptiveterm @> '{"Nonconiferous Trees (Scattered)"}' THEN 'Nonconiferous Tree Fill'
                                    WHEN descriptiveterm @> '{"Coniferous Trees"}' OR descriptiveterm @> '{"Coniferous Trees (Scattered)"}' THEN 'Coniferous Tree Fill'
                                    WHEN descriptiveterm @> '{Orchard}' THEN 'Orchard Fill'
                                    WHEN descriptiveterm @> '{"Coppice Or Osiers"}' THEN 'Coppice Or Osiers Fill'
                                    WHEN descriptiveterm @> '{Scrub}' THEN 'Scrub Fill'
                                    WHEN descriptiveterm @> '{Boulders}' OR descriptiveterm @> '{"Boulders (Scattered)"}' THEN 'Boulders Fill'
                                    WHEN descriptiveterm @> '{Rock}' OR descriptiveterm @> '{"Rock (Scattered)"}' THEN 'Rock Fill'
                                    WHEN descriptiveterm @> '{Scree}' THEN 'Scree Fill'
                                    WHEN descriptiveterm @> '{"Rough Grassland"}' THEN 'Rough Grassland Fill'
                                    WHEN descriptiveterm @> '{Heath}' THEN 'Heath Fill'
                                    WHEN descriptiveterm @> '{"Marsh Reeds Or Saltmarsh"}' THEN 'Marsh Fill'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm IS NULL THEN 1
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm = '{"Multi Surface"}' THEN 2
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND make = 'Natural' THEN 3
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm IS NULL AND make = 'Manmade' THEN 4
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND (make = 'Manmade' OR make = 'Unknown') THEN 5
                                    WHEN descriptivegroup @> '{Roadside}' AND make = 'Natural' THEN 6
                                    WHEN descriptivegroup @> '{Roadside}' AND (make = 'Manmade' OR make = 'Unknown') THEN 7
                                    WHEN descriptivegroup @> '{"Inland Water"}' AND descriptiveterm IS NULL THEN 8
                                    WHEN descriptivegroup @> '{Path}' AND descriptiveterm IS NULL THEN 9
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm = '{Track}' THEN 10
                                    WHEN descriptiveterm = '{Slope}' THEN 11
                                    WHEN descriptivegroup @> '{Structure}' AND (descriptiveterm IS NULL OR descriptiveterm = '{"Upper Level Of Communication"}' OR descriptiveterm = '{"Overhead Construction"}') THEN 12
                                    WHEN descriptiveterm = '{Cliff}' THEN 13
                                    WHEN descriptiveterm = '{Step}' THEN 14
                                    WHEN descriptiveterm = '{Foreshore}' THEN 15
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm = '{"Traffic Calming"}' THEN 16
                                    WHEN descriptivegroup = '{Glasshouse}' THEN 17
                                    WHEN descriptivegroup @> '{Rail}' AND descriptiveterm IS NULL AND make = 'Natural' THEN 18
                                    WHEN descriptiveterm = '{Pylon}' THEN 19
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Archway}'THEN 20
                                    WHEN descriptivegroup @> '{Landform}' AND make = 'Natural' THEN 21
                                    WHEN descriptivegroup @> '{"Tidal Water"}' AND descriptiveterm IS NULL THEN 22
                                    WHEN descriptivegroup @> '{Landform}' AND make = 'Manmade' THEN 23
                                    WHEN descriptivegroup = '{Rail}' AND make = 'Manmade' OR make = 'Unknown' THEN 24
                                    WHEN (descriptiveterm @> '{"Nonconiferous Trees"}' OR descriptiveterm @> '{"Nonconiferous Trees (Scattered)"}') AND (descriptiveterm @> '{"Coniferous Trees"}' OR descriptiveterm @> '{"Coniferous Trees (Scattered)"}') THEN 25
                                    WHEN descriptiveterm @> '{"Nonconiferous Trees"}' OR descriptiveterm @> '{"Nonconiferous Trees (Scattered)"}' THEN 26
                                    WHEN descriptiveterm @> '{"Coniferous Trees"}' OR descriptiveterm @> '{"Coniferous Trees (Scattered)"}' THEN 27
                                    WHEN descriptiveterm @> '{Orchard}' THEN 28
                                    WHEN descriptiveterm @> '{"Coppice Or Osiers"}' THEN 29
                                    WHEN descriptiveterm @> '{Scrub}' THEN 30
                                    WHEN descriptiveterm @> '{Boulders}' OR descriptiveterm @> '{"Boulders (Scattered)"}' THEN 31
                                    WHEN descriptiveterm @> '{Rock}' OR descriptiveterm @> '{"Rock (Scattered)"}' THEN 32
                                    WHEN descriptiveterm @> '{Scree}' THEN 33
                                    WHEN descriptiveterm @> '{"Rough Grassland"}' THEN 34
                                    WHEN descriptiveterm @> '{Heath}' THEN 35
                                    WHEN descriptiveterm @> '{"Marsh Reeds Or Saltmarsh"}' THEN 36
                                    ELSE 99
                                END AS style_code
                            FROM 
                                """ + self.tmpSchema + """.""" + table + """ as a;""", {})
        
    def addCartographicSymbolStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN featurecode = 10091 THEN 'Culvert Symbol'
                                    WHEN featurecode = 10082 THEN 'Direction Of Flow Symbol'
                                    WHEN featurecode = 10130 THEN 'Boundary Half Mereing Symbol'
                                    WHEN featurecode = 10066 OR featurecode = 10170 THEN 'Bench Mark Symbol'
                                    WHEN featurecode = 10165 THEN 'Railway Switch Symbol'
                                    WHEN featurecode = 10177 THEN 'Road Related Flow Symbol'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN featurecode = 10091 THEN 1
                                    WHEN featurecode = 10082 THEN 2
                                    WHEN featurecode = 10130 THEN 3
                                    WHEN featurecode = 10066 OR featurecode = 10170 THEN 4
                                    WHEN featurecode = 10165 THEN 5
                                    WHEN featurecode = 10177 THEN 6
                                    ELSE 99
                                END::integer AS style_code
                            FROM 
                                """ + self.tmpSchema + """.""" + table + """ as a""", {})
    
    def addCartographicTextStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN descriptivegroup @> '{"Buildings Or Structure"}' THEN 'Building Text'
                                    WHEN descriptivegroup @> '{"Inland Water"}' THEN 'Water Text'
                                    WHEN descriptivegroup @> '{"Road Or Track"}' THEN 'Road Text'
                                    WHEN descriptivegroup = '{Terrain And Height}' THEN 'Height Text'
                                    WHEN descriptivegroup @> '{Roadside}' THEN 'Roadside Text'
                                    WHEN descriptivegroup @> '{Structure}' THEN 'Structure Text'
                                    WHEN descriptivegroup = '{"Political Or Administrative"}' THEN 'Administrative Text'
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Natural' THEN 'General Surface Natural Text'
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Manmade' OR descriptivegroup = '{"General Surface"}' AND make IS NULL THEN 'General Surface Manmade Text'
                                    WHEN descriptivegroup = '{Landform}' and make = 'Natural' THEN 'Landform Natural Text'
                                    WHEN descriptiveterm = '{Foreshore}' THEN 'Foreshore Text'
                                    WHEN descriptivegroup @> '{"Tidal Water"}' THEN 'Tidal Water Text'
                                    WHEN descriptivegroup = '{"Built Environment"}' THEN 'Built Environment Text'
                                    WHEN descriptivegroup @> '{"Historic Interest"}' THEN 'Historic Text'
                                    WHEN descriptivegroup = '{Rail}' THEN 'Rail Text'
                                    WHEN descriptivegroup @> '{"General Feature"}' THEN 'General Feature Text'
                                    WHEN descriptivegroup = '{Landform}' and make = 'Manmade' THEN 'Landform Manmade Text'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN descriptivegroup @> '{"Buildings Or Structure"}' THEN 1
                                    WHEN descriptivegroup @> '{"Inland Water"}' THEN 2
                                    WHEN descriptivegroup @> '{"Road Or Track"}' THEN 3
                                    WHEN descriptivegroup = '{Terrain And Height}' THEN 4
                                    WHEN descriptivegroup @> '{Roadside}' THEN 5
                                    WHEN descriptivegroup @> '{Structure}' THEN 6
                                    WHEN descriptivegroup = '{"Political Or Administrative"}' THEN 7
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Natural' THEN 8
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Manmade' OR descriptivegroup = '{"General Surface"}' AND make IS NULL THEN 9
                                    WHEN descriptivegroup = '{Landform}' and make = 'Natural' THEN 10
                                    WHEN descriptiveterm = '{Foreshore}' THEN 11
                                    WHEN descriptivegroup @> '{"Tidal Water"}' THEN 12
                                    WHEN descriptivegroup = '{"Built Environment"}' THEN 13
                                    WHEN descriptivegroup @> '{"Historic Interest"}' THEN 14
                                    WHEN descriptivegroup = '{Rail}' THEN 15
                                    WHEN descriptivegroup @> '{"General Feature"}' THEN 16
                                    WHEN descriptivegroup = '{Landform}' and make = 'Manmade' THEN 17
                                    ELSE '99'
                                END::integer AS style_code,
                                CASE
                                    WHEN descriptivegroup @> '{"Buildings Or Structure"}' THEN 1
                                    WHEN descriptivegroup @> '{"Inland Water"}' THEN 2
                                    WHEN descriptivegroup @> '{"Road Or Track"}' THEN 1
                                    WHEN descriptivegroup = '{Terrain And Height}' THEN 3
                                    WHEN descriptivegroup @> '{Roadside}' THEN 1
                                    WHEN descriptivegroup @> '{Structure}' THEN 1
                                    WHEN descriptivegroup = '{"Political Or Administrative"}' THEN 5
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Natural' THEN 1
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Manmade' OR descriptivegroup = '{"General Surface"}' AND make IS NULL THEN 1
                                    WHEN descriptivegroup = '{Landform}' and make = 'Natural' THEN 4
                                    WHEN descriptiveterm = '{Foreshore}' THEN 4
                                    WHEN descriptivegroup @> '{"Tidal Water"}' THEN 2
                                    WHEN descriptivegroup = '{"Built Environment"}' THEN 1
                                    WHEN descriptivegroup @> '{"Historic Interest"}' THEN 1
                                    WHEN descriptivegroup = '{Rail}' THEN 1
                                    WHEN descriptivegroup @> '{"General Feature"}' THEN 1
                                    WHEN descriptivegroup = '{Landform}' and make = 'Manmade' THEN 4
                                    ELSE '1' 
                                END::integer AS colour_code,
                                CASE
                                    WHEN descriptivegroup @> '{"Buildings Or Structure"}' THEN 1
                                    WHEN descriptivegroup @> '{"Inland Water"}' THEN 2
                                    WHEN descriptivegroup @> '{"Road Or Track"}' THEN 1
                                    WHEN descriptivegroup = '{Terrain And Height}' THEN 1
                                    WHEN descriptivegroup @> '{Roadside}' THEN 1
                                    WHEN descriptivegroup @> '{Structure}' THEN 1
                                    WHEN descriptivegroup = '{"Political Or Administrative"}' THEN 1
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Natural' THEN 1
                                    WHEN descriptivegroup = '{"General Surface"}' AND make = 'Manmade' OR descriptivegroup = '{"General Surface"}' AND make IS NULL THEN 1
                                    WHEN descriptivegroup = '{Landform}' and make = 'Natural' THEN 1
                                    WHEN descriptiveterm = '{Foreshore}' THEN 1
                                    WHEN descriptivegroup @> '{"Tidal Water"}' THEN 2
                                    WHEN descriptivegroup = '{"Built Environment"}' THEN 1
                                    WHEN descriptivegroup @> '{"Historic Interest"}' THEN 3
                                    WHEN descriptivegroup = '{Rail}' THEN 1
                                    WHEN descriptivegroup @> '{"General Feature"}' THEN 1
                                    WHEN descriptivegroup = '{Landform}' and make = 'Manmade' THEN 1
                                    ELSE '1' 
                                END::integer AS font_code,
                                (orientation/10) as rotation,
                                CASE
                                    WHEN anchorposition = 0 THEN 0
                                    WHEN anchorposition = 1 THEN 0
                                    WHEN anchorposition = 2 THEN 0
                                    WHEN anchorposition = 3 THEN 0.5
                                    WHEN anchorposition = 4 THEN 0.5
                                    WHEN anchorposition = 5 THEN 0.5
                                    WHEN anchorposition = 6 THEN 1
                                    WHEN anchorposition = 7 THEN 1
                                    WHEN anchorposition = 8 THEN 1
                                    END AS geo_x,
                                CASE
                                    WHEN anchorposition = 0 THEN 0
                                    WHEN anchorposition = 1 THEN 0.5
                                    WHEN anchorposition = 2 THEN 1
                                    WHEN anchorposition = 3 THEN 0
                                    WHEN anchorposition = 4 THEN 0.5
                                    WHEN anchorposition = 5 THEN 1
                                    WHEN anchorposition = 6 THEN 0
                                    WHEN anchorposition = 7 THEN 0.5
                                    WHEN anchorposition = 8 THEN 1
                                    END AS geo_y,
                                CASE
                                    WHEN anchorposition = 0 THEN 'SW'
                                    WHEN anchorposition = 1 THEN 'W'
                                    WHEN anchorposition = 2 THEN 'NW'
                                    WHEN anchorposition = 3 THEN 'S'
                                    WHEN anchorposition = 4 THEN ''
                                    WHEN anchorposition = 5 THEN 'N'
                                    WHEN anchorposition = 6 THEN 'SE'
                                    WHEN anchorposition = 7 THEN 'E'
                                    WHEN anchorposition = 8 THEN 'NE'
                                    END as anchor
                            FROM
                                """ + self.tmpSchema + """.""" + table + """ as a""", {})
    
    def addBoundaryLineStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN featurecode = 10136 THEN 'Parish Boundary'
                                    WHEN featurecode = 10131 THEN 'District Boundary'
                                    WHEN featurecode = 10128 THEN 'Electoral Boundary'
                                    WHEN featurecode = 10127 THEN 'County Boundary'
                                    WHEN featurecode = 10135 THEN 'Parliamentary Boundary'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN featurecode = 10136 THEN 1
                                    WHEN featurecode = 10131 THEN 2
                                    WHEN featurecode = 10128 THEN 3
                                    WHEN featurecode = 10127 THEN 4
                                    WHEN featurecode = 10135 THEN 5
                                    ELSE 99
                                END::integer AS style_code
                            FROM
                                """ + self.tmpSchema + """.""" + table + """ as a""", {})
    
    def addTopographicLineStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm IS NULL AND physicalpresence = 'Obstructing' THEN 'Default Line'
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Outline}' AND make = 'Manmade' AND physicalpresence = 'Obstructing' THEN 'Building Outline Line'
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm IS NULL AND physicalpresence = 'Edge / Limit' THEN 'Edge Line'
                                    WHEN descriptivegroup @> '{"Road Or Track"}' AND descriptiveterm = '{Public}' AND make = 'Manmade' AND physicalpresence = 'Edge / Limit' THEN 'Road Or Track Line'
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Division}' AND make = 'Manmade' AND physicalpresence = 'Obstructing' THEN 'Building Division Line'
                                    WHEN descriptiveterm = '{"Polygon Closing Link"}' THEN 'Polygon Closing Line'
                                    WHEN descriptivegroup @> '{"Inland Water"}' AND descriptiveterm IS NULL AND physicalpresence = 'Edge / Limit' THEN 'Inland Water Line'
                                    WHEN descriptiveterm = '{"Inferred Property Closing Link"}' THEN 'Property Closing Line'
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND make = 'Natural' AND physicalpresence = 'Edge / Limit' THEN 'General Surface Natural Line'
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Outline}' AND make = 'Manmade' AND physicalpresence = 'Overhead' THEN 'Building Overhead Line'
                                    WHEN descriptiveterm = '{"Bottom Of Slope"}' THEN 'Bottom Of Slope Line'
                                    WHEN descriptiveterm = '{"Top Of Slope"}' THEN 'Top Of Slope Line'
                                    WHEN descriptiveterm = '{Step}' THEN 'Step Line'
                                    WHEN descriptivegroup @> '{Path}' AND descriptiveterm = '{"Unmade Path Alignment"}' AND physicalpresence = 'Edge / Limit' THEN 'Path Line'
                                    WHEN descriptiveterm = '{"Mean High Water (Springs)"}' THEN 'Mean High Water Line'
                                    WHEN descriptiveterm = '{"Traffic Calming"}' THEN 'Traffic Calming Line'
                                    WHEN descriptiveterm = '{"Standard Gauge Track"}' THEN 'Standard Gauge Track Line'
                                    WHEN descriptiveterm = '{"Bottom Of Cliff"}' THEN 'Bottom Of Cliff Line'
                                    WHEN descriptiveterm = '{"Top Of Cliff"}' THEN 'Top Of Cliff Line'
                                    WHEN descriptiveterm = '{"Mean Low Water (Springs)"}' THEN 'Mean Low Water Line'
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm = '{"Overhead Construction"}' THEN 'Overhead Construction Line'
                                    WHEN descriptiveterm = '{Culvert}' THEN 'Culvert Line'
                                    WHEN descriptiveterm = '{Pylon}' THEN 'Pylon Line'
                                    WHEN descriptivegroup = '{Landform}' AND make = 'Natural' AND physicalpresence = 'Edge / Limit' THEN 'Landform Natural Line'
                                    WHEN descriptiveterm = '{Ridge Or Rock Line}' THEN 'Ridge Or Rock Line'
                                    WHEN descriptivegroup = '{"Historic Interest"}' THEN 'Historic Interest Line'
                                    WHEN descriptiveterm = '{"Narrow Gauge"}' THEN 'Narrow Gauge Line'
                                    WHEN descriptiveterm = '{Buffer}' THEN 'Railway Buffer Line'
                                    WHEN descriptiveterm = '{"Tunnel Edge"}' THEN 'Tunnel Edge Line'
                                    WHEN descriptivegroup = '{Landform}' AND make = 'Manmade' AND physicalpresence = 'Edge / Limit' THEN 'Landform Manmade Line'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm IS NULL  AND physicalpresence = 'Obstructing' THEN 1
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Outline}' AND make = 'Manmade' AND physicalpresence = 'Obstructing' THEN 2
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm IS NULL AND physicalpresence = 'Edge / Limit' THEN 3
                                    WHEN descriptivegroup = '{"Road Or Track"}' AND descriptiveterm = '{Public}' AND make = 'Manmade' AND physicalpresence = 'Edge / Limit' THEN 4
                                    WHEN descriptivegroup = '{Building}' AND descriptiveterm = '{Division}' AND make = 'Manmade' AND physicalpresence = 'Obstructing' THEN 5
                                    WHEN descriptiveterm = '{"Polygon Closing Link"}' THEN 6
                                    WHEN descriptivegroup @> '{"Inland Water"}' AND descriptiveterm IS NULL AND physicalpresence = 'Edge / Limit' THEN 7
                                    WHEN descriptiveterm = '{"Inferred Property Closing Link"}' THEN 8
                                    WHEN descriptivegroup @> '{"General Surface"}' AND descriptiveterm IS NULL AND make = 'Natural' AND physicalpresence = 'Edge / Limit' THEN 9
                                    WHEN descriptivegroup @> '{Building}' AND descriptiveterm = '{Outline}' AND make = 'Manmade' AND physicalpresence = 'Overhead' THEN 10
                                    WHEN descriptiveterm = '{"Bottom Of Slope"}' THEN 11
                                    WHEN descriptiveterm = '{"Top Of Slope"}' THEN 12
                                    WHEN descriptivegroup = '{"General Surface"}' AND descriptiveterm = '{Step}' AND make = 'Manmade' THEN 13
                                    WHEN descriptivegroup @> '{Path}' AND descriptiveterm = '{"Unmade Path Alignment"}' AND physicalpresence = 'Edge / Limit' THEN 14
                                    WHEN descriptiveterm = '{"Mean High Water (Springs)"}' THEN 15
                                    WHEN descriptiveterm = '{"Traffic Calming"}' THEN 16
                                    WHEN descriptiveterm = '{"Standard Gauge Track"}' THEN 17
                                    WHEN descriptiveterm = '{"Bottom Of Cliff"}' THEN 18
                                    WHEN descriptiveterm = '{"Top Of Cliff"}' THEN 19
                                    WHEN descriptiveterm = '{"Mean Low Water (Springs)"}' THEN 20
                                    WHEN descriptivegroup @> '{"General Feature"}' AND descriptiveterm = '{"Overhead Construction"}' THEN 21
                                    WHEN descriptiveterm = '{Culvert}' THEN 22
                                    WHEN descriptiveterm = '{Pylon}' THEN 23
                                    WHEN descriptivegroup = '{Landform}' AND make = 'Natural' AND physicalpresence = 'Edge / Limit' THEN 24
                                    WHEN descriptiveterm = '{Ridge Or Rock Line}' THEN 25
                                    WHEN descriptivegroup = '{"Historic Interest"}' THEN 26
                                    WHEN descriptiveterm = '{"Narrow Gauge"}' THEN 27
                                    WHEN descriptiveterm = '{Buffer}' THEN 28
                                    WHEN descriptiveterm = '{"Tunnel Edge"}' THEN 29
                                    WHEN descriptivegroup = '{Landform}' AND make = 'Manmade' AND physicalpresence = 'Edge / Limit' THEN 30
                                    ELSE 99
                                END::integer AS style_code
                            FROM
                                """ + self.tmpSchema + """.""" + table + """ as a""", {})
    
    def addTopographicPointStyleFields(self, table):
        self.cur.execute("""CREATE TABLE """ + self.tmpSchema + """.""" + table + """_style AS 
                            SELECT
                                a.*,
                                CASE
                                    WHEN featurecode = 10197 THEN 'Spot Height Point'
                                    WHEN featurecode = 10085 THEN 'Culvert Point'
                                    WHEN featurecode = 10048 THEN 'Positioned Nonconiferous Tree Point'
                                    WHEN featurecode = 10088 THEN 'Inland Water Point'
                                    WHEN featurecode = 10186 AND descriptiveterm IS NULL THEN 'Structure Point'
                                    WHEN featurecode = 10179 THEN 'Roadside Point'
                                    WHEN featurecode = 10186 AND descriptiveterm = '{"Overhead Construction"}' THEN 'Overhead Construction Point'
                                    WHEN featurecode = 10158 THEN 'Rail Point'
                                    WHEN featurecode = 10050 THEN 'Positioned Coniferous Tree Point'
                                    WHEN featurecode = 10094 THEN 'Landform Point'
                                    WHEN featurecode = 10080 THEN 'Historic Point'
                                    WHEN featurecode = 10129 THEN 'Boundary Post Point'
                                    WHEN featurecode = 10186 AND  descriptiveterm = '{"Triangulation Point Or Pillar"}' THEN 'Triangulation Point Or Pillar Point'
                                    WHEN featurecode = 10191 THEN 'Structure Point'
                                    WHEN featurecode = 10072 THEN 'Site of Heritage'
                                    WHEN featurecode = 10051 THEN 'Positioned Boulder Point'
                                    WHEN featurecode = 10209 THEN 'Tidal Water Point'
                                    WHEN featurecode = 10100 THEN 'Diused Feature Point'
                                    WHEN featurecode = 10159 AND descriptiveterm = '{Switch}' THEN 'Rail Switch Point'
                                    WHEN featurecode = 10132 THEN 'Positioned Nonconiferous Tree Point'
                                    WHEN featurecode = 10080 THEN 'Positioned Nonconiferous Tree Point'
                                    WHEN featurecode = 10120 THEN 'Inland Water Point'
                                    WHEN featurecode = 10176 THEN 'Inland Water Point'
                                    WHEN featurecode = 10159 THEN 'Inland Water Point'
                                    ELSE 'Unclassified'
                                END::text AS style_description,
                                CASE
                                    WHEN featurecode = 10197 THEN 1
                                    WHEN featurecode = 10085 THEN 2
                                    WHEN featurecode = 10048 THEN 3
                                    WHEN featurecode = 10088 THEN 4
                                    WHEN featurecode = 10186 AND descriptiveterm IS NULL THEN 5
                                    WHEN featurecode = 10179 THEN 6
                                    WHEN featurecode = 10186 AND descriptiveterm = '{"Overhead Construction"}' THEN 7
                                    WHEN featurecode = 10158 THEN 8
                                    WHEN featurecode = 10050 THEN 9
                                    WHEN featurecode = 10094 THEN 10
                                    WHEN featurecode = 10080 THEN 11
                                    WHEN featurecode = 10129 THEN 12
                                    WHEN featurecode = 10186 AND  descriptiveterm = '{"Triangulation Point Or Pillar"}' THEN 13
                                    WHEN featurecode = 10191 THEN 5
                                    WHEN featurecode = 10072 THEN 14
                                    WHEN featurecode = 10051 THEN 15
                                    WHEN featurecode = 10209 THEN 16
                                    WHEN featurecode = 10100 THEN 17
                                    WHEN featurecode = 10159 AND descriptiveterm = '{Switch}' THEN 18
                                    WHEN featurecode = 10132 THEN 3
                                    WHEN featurecode = 10080 THEN 3
                                    WHEN featurecode = 10120 THEN 4
                                    WHEN featurecode = 10176 THEN 4
                                    WHEN featurecode = 10159 THEN 4
                                    ELSE 99
                                END::integer AS style_code
                            FROM
                                """ + self.tmpSchema + """.""" + table + """ as a""", {})
    
    def cleanUp(self, table):
        self.cur.execute("""DROP TABLE IF EXISTS """ + self.tmpSchema + """.""" + table, {})
        self.cur.execute("""ALTER TABLE """ + self.tmpSchema + """.""" + table + """_style RENAME TO """ + table, {})
        # Add back primary key constraint
        self.cur.execute("""ALTER TABLE """ + self.tmpSchema + """.""" + table + """ ADD PRIMARY KEY (ogc_fid)""", {})
