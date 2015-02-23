rem /***************************************************************************
rem  OsTranslatorII
rem                                  A QGIS plugin
rem  A plugin for loading Ordnance Survey MasterMap and other GML-based datasets.
rem                              -------------------
rem         begin                : 2014-10-03
rem         copyright            : (C) 2014 by Peter Wells for Lutra Consulting
rem         email                : info@lutraconsulting.co.uk
rem         git sha              : $Format:%H$
rem  ***************************************************************************/
rem 
rem /***************************************************************************
rem  *                                                                         *
rem  *   This program is free software; you can redistribute it and/or modify  *
rem  *   it under the terms of the GNU General Public License as published by  *
rem  *   the Free Software Foundation; either version 2 of the License, or     *
rem  *   (at your option) any later version.                                   *
rem  *                                                                         *
rem  ***************************************************************************/

SET DEST=%HOMEPATH%\.qgis2\python\plugins\OSTranslatorII
mkdir %DEST%
mkdir %DEST%\gfs
xcopy /e /y *.py %DEST%
xcopy /e /y osTrans_128px.png %DEST%
xcopy /e /y metadata.txt %DEST%
xcopy /e /y *.ui %DEST%
xcopy /e /y gfs\*.* %DEST%\gfs
